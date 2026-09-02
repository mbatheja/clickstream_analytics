import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter
import matplotlib.pyplot as plt
import os
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

# (from_ts_col, to_ts_col, precomputed duration_seconds col, plot label)
TRANSITIONS = [
    ("homepage_ts", "search_ts", "homepage_to_search_seconds", "Homepage -> Search"),
    ("search_ts", "product_view_ts", "search_to_product_view_seconds", "Search -> Product View"),
    ("product_view_ts", "add_to_cart_ts", "product_view_to_add_to_cart_seconds", "Product View -> Add to Cart"),
    ("add_to_cart_ts", "checkout_initiated_ts", "add_to_cart_to_checkout_seconds", "Add to Cart -> Checkout"),
    ("checkout_initiated_ts", "purchase_completed_ts", "checkout_to_purchase_seconds", "Checkout -> Purchase"),
]


def run_velocity_analysis(df: pd.DataFrame):
    kmf = KaplanMeierFitter()
    plt.figure(figsize=(10,6))

    for variant_name, group in df.groupby('variant'):
        kmf.fit(
            durations=group['session_duration'],
            event_observed = group['has_purchased'],
            label=f"Variant: {variant_name}"
        )
        kmf.plot_survival_function()

    plt.title('Conversion Survival Function (session duration to purchase)')
    plt.xlabel('Time in seconds')
    plt.ylabel('Unconverted Proportion')
    plt.grid(True)
    plt.savefig('scripts/conversion_velocity.png')
    print("Velocity plot saved to scripts/conversion_velocity.png")


def run_step_transition_analysis(df: pd.DataFrame):
    """
    Fits a separate Kaplan-Meier survival curve per funnel step transition, so
    hazard/time-to-advance can be compared stage by stage instead of lumped into
    one whole-session duration. A session that reaches the "from" step of a
    transition but never reaches the "to" step is right-censored at session_end
    (we know they didn't make it by then, not how much longer it would have taken).
    """
    kmf = KaplanMeierFitter()
    plt.figure(figsize=(10, 6))

    for from_col, to_col, duration_col, label in TRANSITIONS:
        reached = df[df[from_col].notna()].copy()
        if reached.empty:
            continue

        completed = reached[to_col].notna()
        censored_duration = (reached['session_end'] - reached[from_col]).dt.total_seconds()

        reached['duration'] = np.where(completed, reached[duration_col], censored_duration)
        reached['event_observed'] = completed.astype(int)

        kmf.fit(
            durations=reached['duration'],
            event_observed=reached['event_observed'],
            label=label
        )
        kmf.plot_survival_function()

    plt.title('Time to Advance, by Funnel Step Transition')
    plt.xlabel('Time in seconds')
    plt.ylabel('Proportion Not Yet Advanced')
    plt.grid(True)
    plt.savefig('scripts/step_transition_velocity.png')
    print("Step transition plot saved to scripts/step_transition_velocity.png")


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

    print("Fetching transformed session data from Snowflake...")
    session_query = """
        select
            session_id,
            user_id,
            variant,
            device,
            acquisition_channel,
            user_type,
            session_duration,
            has_purchased
        from int_sessions
    """
    df = pd.read_sql(session_query, conn)
    df.columns = df.columns.str.lower()

    print("Fetching step transition data from Snowflake...")
    transition_query = """
        select
            session_id,
            variant,
            session_end,
            homepage_ts,
            search_ts,
            product_view_ts,
            add_to_cart_ts,
            checkout_initiated_ts,
            purchase_completed_ts,
            homepage_to_search_seconds,
            search_to_product_view_seconds,
            product_view_to_add_to_cart_seconds,
            add_to_cart_to_checkout_seconds,
            checkout_to_purchase_seconds
        from int_step_transition
    """
    transitions_df = pd.read_sql(transition_query, conn)
    transitions_df.columns = transitions_df.columns.str.lower()
    conn.close()

    print(f"Loaded {len(df):,} session records. Running velocity calauclations...\n")
    run_velocity_analysis(df)

    print(f"Loaded {len(transitions_df):,} transition records. Running step transition analysis...\n")
    run_step_transition_analysis(transitions_df)
