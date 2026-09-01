import numpy as np
import pandas as pd
from scipy.stats import chisquare, chi2_contingency
import statsmodels.formula.api as smf
import os
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")


def run_experiment_diagnostics(df: pd.DataFrame):
    # variant/device/acquisition_channel are persistent per-user attributes, and users
    # can contribute multiple (correlated) sessions. Randomization health checks -- SRM
    # and covariate balance -- must be evaluated at the user level (one row per user),
    # since testing them on repeated session rows pseudo-replicates each user and
    # inflates the effective sample size, producing spurious "imbalanced" verdicts.
    user_df = df.drop_duplicates('user_id')

    print("---SRM TEST---")
    variant_counts = user_df['variant'].value_counts()
    n_control = variant_counts.get('control', 0)
    n_variant = variant_counts.get('new_checkout_ui', 0)
    total = n_control + n_variant

    srm_stat, srm_pvalue = chisquare([n_control, n_variant], f_exp = [total/2, total/2])
    print(f"Counts -> Control: {n_control:,} | Variant: {n_variant:,} (users)")
    print(f"SRM Chi2 Stat: {srm_stat:.4f} | p-value: {srm_pvalue:.4f}")
    if srm_pvalue < 0.01:
        print("WARNING: Severe Sample Ratio Mismatch detected! Check traffic router.\n")
    else:
        print("Traffic split is balanced (No SRM).\n")

    print("--- COVARIATE BALANCE CHECKS ---")
    for covariate in ['device', 'acquisition_channel']:
        contingency_tab = pd.crosstab(user_df['variant'], user_df[covariate])
        chi2, p_val, _, _ = chi2_contingency(contingency_tab)
        status = "Balanced" if p_val > 0.05 else "Imbalanced"
        print(f"Covariate '{covariate}': Chi2 = {chi2:.2f}, p-value = {p_val:.4f} ({status})")
    print("\n")

    print("--- HTE & SUBGROUP ANALYSIS ---")
    # has_purchased is a session-level outcome, so this stays at session granularity for
    # statistical power, but standard errors are clustered by user_id since a user's
    # sessions aren't independent observations.
    formula = "has_purchased ~ C(variant, Treatment('control')) * C(device) + C(variant, Treatment('control')) * C(acquisition_channel)"
    model = smf.logit(formula, data=df).fit(cov_type='cluster', cov_kwds={'groups': df['user_id']})

    print(model.summary().tables[1])


if __name__ == "__main__":

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "INTERMEDIATE")
    )

    # Query session data transformed by dbt
    print("Fetching transformed session data from Snowflake...")
    query = """
        select 
            session_id,
            user_id,
            variant,
            device,
            acquisition_channel,
            user_type,
            has_purchased
        from int_sessions
    """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    df['has_purchased'] = df['has_purchased'].astype(int)
    conn.close()

    # Execute experiment diagnostics and HTE analysis
    print(f"Loaded {len(df):,} session records. Running evaluation...\n")
    run_experiment_diagnostics(df)