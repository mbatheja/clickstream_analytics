import pandas as pd
from lifelines import KaplanMeierFitter
import matplotlib.pyplot as plt
import os
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")


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
    query = """
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
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    conn.close()

    print(f"Loaded {len(df):,} session records. Running velocity calauclations...\n")
    run_velocity_analysis(df)
