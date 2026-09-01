import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
import os
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

def run_user_clustering(df: pd.DataFrame):
    df = df.copy()
    df['days_since_signup'] = (df['session_start'] - df['user_signup_date']).dt.days
    df['funnel_depth'] = (
        df['homepage_views'].gt(0).astype(int)
        + df['has_searched_product'].gt(0).astype(int)
        + df['has_viewed_product'].gt(0).astype(int)
        + df['added_to_cart'].gt(0).astype(int)
        + df['has_checkout'].astype(int)
        + df['has_purchased'].astype(int)
    )
    df['is_bounce'] = df['total_events'] <= 1

    user_features = df.groupby('user_id').agg(
        days_since_signup=('days_since_signup', 'max'),
        max_funnel_depth=('funnel_depth', 'max'),
        bounce_rate=('is_bounce', 'mean'),
        total_sessions=('session_id', 'nunique'),
        avg_duration=('session_duration', 'mean'),
        total_purchase=('has_purchased', 'sum'),
        cart_additions=('added_to_cart', 'sum')
    ).reset_index()

    scaler = StandardScaler()
    scaled_matrix = scaler.fit_transform(user_features.drop(columns=['user_id']))

    scores = []
    for k in range(2, 6):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(scaled_matrix)
        score = silhouette_score(scaled_matrix, labels)
        scores.append((k, score))
        print(f"K={k} | Silhouette Score: {score:.3f}")

    best_k, best_score = max(scores, key=lambda x: x[1])
    print(f"\nBest K={best_k} | Silhouette Score: {best_score:.3f}")

    final_kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    user_features['cluster'] = final_kmeans.fit_predict(scaled_matrix)

    print("CLUSTER SUMMARY")
    print(user_features.groupby('cluster').mean(numeric_only=True))

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
                    user_signup_date,
                    session_start,
                    session_duration,
                    total_events,
                    homepage_views,
                    has_searched_product,
                    has_viewed_product,
                    added_to_cart,
                    has_checkout,
                    has_purchased
                from int_sessions
            """
    df = pd.read_sql(query, conn)
    df.columns = df.columns.str.lower()
    df['user_signup_date'] = pd.to_datetime(df['user_signup_date'])
    conn.close()

    print(f"Loaded {len(df):,} session records. Running clustering algorithm...\n")
    run_user_clustering(df)