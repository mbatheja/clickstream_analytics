import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
)

try:
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE IF EXISTS CLICKSTREAM_DB.RAW.clickstream_events")
    print("Truncated CLICKSTREAM_DB.RAW.clickstream_events")
finally:
    conn.close()
