# Clickstream A/B Testing Pipeline

A synthetic e-commerce clickstream pipeline that simulates a checkout-redesign A/B test end to end: generating realistic multi-session user behavior, landing it in a cloud warehouse, transforming it with dbt, and evaluating the experiment with proper statistical rigor.

The core question the project is built to answer: **did a new checkout UI improve purchase conversion, and can we trust that result?**

## What this project does

1. **Simulates traffic.** A Markov-chain funnel generator (`view_homepage → search_product → view_product_details → add_to_cart → checkout_initiated → purchase_completed`) produces synthetic clickstream events for a `control` vs. `new_checkout_ui` experiment, with users persisted across multiple sessions rather than generated as one-off, independent events.
2. **Streams to the cloud.** Events are written either as local JSON files or streamed directly to S3, partitioned by `year/month/day`, ready to be picked up by Snowpipe into a Snowflake raw landing table.
3. **Transforms with dbt.** A layered dbt project (staging → intermediate → marts) turns raw JSON payloads into session-level facts, conversion funnels, drop-off analysis, and A/B test summary tables, backed by dbt data tests.
4. **Evaluates the experiment.** Python scripts run sample-ratio-mismatch checks, covariate balance checks, heterogeneous treatment effect analysis, conversion-velocity (survival) analysis, and behavioral user clustering on top of the dbt marts.
5. **Orchestrates the whole thing.** An Airflow DAG chains generation → dbt build → evaluation into a single scheduled pipeline.

## File structure

```
config/
  config.yaml                 Funnel steps, transition probabilities, experiment config
src/
  generator/event_generator.py  Markov-chain event simulator + statistically-sized cohort generation
  pipeline/s3_streamer.py       Streams generated sessions directly to S3, partitioned by date
  utils/sample_size.py          Power-analysis sample size calculator (two-proportion z-test)
dbt_clickstream/
  packages.yml                  dbt package dependencies (dbt_utils)
  models/staging/                 stg_events — parses raw VARIANT JSON from Snowflake
  models/intermediate/          int_sessions — rolls events up to session-level metrics
  models/marts/                 fct_conversion_funnel, fct_session_dropoff,
                                 fct_ab_test_summary, fct_user_retention
tests/                          dbt data-quality SQL tests + pytest unit/integration/e2e tests
scripts/
  evaluate_experiment.py        SRM check, covariate balance, HTE (logistic regression)
  analyze_velocity.py           Kaplan-Meier survival analysis of time-to-purchase
  cluster_users.py              KMeans behavioral segmentation of users
orchestration/dags/clickstream_dag.py   Airflow DAG: generate → dbt build → evaluate
clear_s3.py / clear_snowflake.py        Utility scripts to reset the raw S3 prefix / Snowflake table
.github/workflows/ci.yml        CI: pytest suite + dbt parse on every push/PR
```

## How to run

**1. Install dependencies**
```bash
python -m venv .venv
source .venv/bin/activate        # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

**2. Configure credentials**
Fill in `.env` (see `.env.example`) with your AWS and Snowflake credentials:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET_NAME`,
`SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`.

**3. Tune the experiment**
Edit `config/config.yaml` to set funnel transition probabilities and the control/variant checkout conversion rates you want to simulate.

**4. Generate event data**
```bash
python -m src.generator.event_generator   # writes JSON files locally to local_clickstream_raw/
python -m src.pipeline.s3_streamer        # or stream sessions straight to S3
```
The user count isn't arbitrary — it's derived from a power analysis (`src/utils/sample_size.py`) against the control/variant rates in `config.yaml`, so the generated sample is large enough to detect the configured lift.

**5. Load into Snowflake**
Point Snowpipe (or a manual `COPY INTO`) at the S3 `raw_events/` prefix to land data into `CLICKSTREAM_DB.RAW.clickstream_events`, which `dbt_clickstream/models/staging/sources.yml` reads from.

**6. Run the dbt transformations**
```bash
cd dbt_clickstream
dbt deps    # installs dbt_utils (declared in packages.yml)
dbt build   # requires a `clickstream_dbt` profile in ~/.dbt/profiles.yml
```

**7. Evaluate the experiment**
```bash
python scripts/evaluate_experiment.py   # SRM, covariate balance, HTE
python scripts/analyze_velocity.py      # Kaplan-Meier time-to-purchase
python scripts/cluster_users.py         # behavioral user segmentation
```

**8. Run tests**
```bash
pytest tests/
```

**9. (Optional) Orchestrate end to end**
Deploy `orchestration/dags/clickstream_dag.py` to an Airflow instance to run generation → dbt build → evaluation on a schedule.

## Unique features

- **Statistically-sized simulation, not an arbitrary N.** The generator computes the required sample size from a two-proportion power analysis against the configured control/variant rates, so the synthetic experiment is properly powered by design.
- **Persistent, multi-session user behavior.** Users are simulated with stable attributes (device, acquisition channel, variant assignment) across 1–5 sessions spread over a signup window, rather than as independent single-session events — closer to how real experiment data looks.
- **Rigorous experiment evaluation, not just a conversion-rate diff.** `evaluate_experiment.py` checks for sample ratio mismatch (SRM), verifies covariate balance across device and acquisition channel, and fits a logistic regression to detect heterogeneous treatment effects (HTE) by subgroup. Because users are persistent across multiple sessions, SRM/balance checks run at the user level and the regression uses standard errors clustered by `user_id` — testing at the session level would pseudo-replicate each user and produce spuriously significant results.
- **Beyond binary conversion.** Kaplan-Meier survival analysis captures *how fast* users convert, not just whether they do, and KMeans clustering surfaces behavioral user segments independent of the experiment.
- **Data quality enforced in the warehouse layer.** Custom dbt tests assert funnel monotonicity, that drop-off percentages sum correctly, and that retention rates stay within logical bounds — on top of standard not-null/accepted-values/uniqueness schema tests.
- **Layered, source-traced dbt architecture.** staging → intermediate → marts, tracing all the way back to raw semi-structured JSON landed via Snowpipe.
- **Fully orchestrated and CI-checked.** An Airflow DAG runs the pipeline end to end; GitHub Actions validates dbt syntax and runs the pytest suite on every push and PR.

## Results

Run end to end against live infrastructure: 62,468 persistent users, 128,938 sessions (1–5 sessions each), generated and streamed to S3, landed in Snowflake, transformed with dbt, and evaluated with the scripts below.

**Randomization health** (checked at the user level, per the pseudo-replication note above):
- **SRM**: Control 31,320 users vs. Variant 31,148 — Chi² = 0.47, p = 0.49. Balanced, no sample ratio mismatch.
- **Covariate balance**: device Chi² = 1.42, p = 0.49 (balanced); acquisition channel Chi² = 7.51, p = 0.057 (balanced, though closer to the line than device).

**Did the new checkout UI win?** No detectable effect. The logistic regression (`has_purchased ~ variant * device + variant * acquisition_channel`, clustered SEs by `user_id`) puts the `new_checkout_ui` main effect at coef = 0.146, p = 0.44 — not statistically significant. None of the variant × device or variant × acquisition_channel interaction terms were significant either (all p > 0.15), so there's no evidence of a subgroup where the redesign helped or hurt. Given the configured lift in `config.yaml` (control 5.0% → variant 5.5%, a 10% relative lift) and the sample size the power analysis called for, this null result is consistent with a genuinely small, hard-to-detect effect rather than an obviously broken experiment.

**Time-to-purchase** (`analyze_velocity.py`): Kaplan-Meier survival curves for both variants are saved to `scripts/conversion_velocity.png` — visually near-identical between control and variant, consistent with the flat regression result above.

**Behavioral segments** (`cluster_users.py`, KMeans on funnel depth, bounce rate, session count, duration, purchases, and cart adds): K=5 was the best fit (silhouette = 0.425), recovering distinct, interpretable segments:
| Segment | Bounce rate | Funnel depth | Avg. session | Repeat visits | Converts? |
|---|---|---|---|---|---|
| Bouncers | 86% | 1.3 | 10s | 1.5 | No |
| Light browsers | 4% | 2.6 | 96s | 1.4 | No |
| Cart abandoners | 5% | 4.5 | 195s | 1.8 | No (adds to cart, never buys) |
| Loyal non-converters | 21% | 3.8 | 101s | 3.9 (most repeat visits, oldest signups) | No |
| Converters | 9% | 6.0 (full funnel) | 222s | 2.7 | Yes |

The clustering finds real behavioral structure (a loyal-but-never-converts segment worth investigating separately, a clear cart-abandonment segment) independent of the experiment itself.

## Future scope / learnings

- **Geography-based experiment assignment.** Right now variant assignment is a simple random split. A more realistic setup would randomize by geography (e.g., stagger the checkout redesign rollout by region/market) to mimic how experiments are actually rolled out in production and to test for geo-level confounders.
- Replace simulated events with real front-end/clickstream capture (e.g. Segment, Snowplow) to validate the pipeline against real-world data quality issues.
- Add variance-reduction techniques (e.g. CUPED) and sequential/always-valid testing so the evaluation doesn't rely purely on a fixed-horizon z-test.
- Automate the S3 → Snowflake ingestion step (Snowpipe setup, or an Airflow sensor/operator) instead of relying on manual loading.
- Extend CI to run dbt against a live Snowflake dev target so data tests execute automatically, not just `dbt parse`.
