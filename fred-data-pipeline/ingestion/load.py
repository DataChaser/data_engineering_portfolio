import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import os
from extract import fetch_all_indicators

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="ECON_WH",
        database="ECONOMIC_INDICATORS",
        schema="RAW",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def create_raw_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.FRED_OBSERVATIONS (
            DATE DATE NOT NULL,
            VALUE FLOAT,
            INDICATOR_NAME VARCHAR(100) NOT NULL,
            SERIES_ID VARCHAR(50) NOT NULL,
            LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("Raw table ready.")

# Table is first truncated (deleting all records) and then reloaded with all data
# Reason for this is that economic data gets revised frequently
def truncate_and_load(cursor, df: pd.DataFrame):
    cursor.execute("TRUNCATE TABLE RAW.FRED_OBSERVATIONS")
    print("Table truncated.")

    rows = [
        (
            row["date"].date(),
            row["value"],
            row["indicator_name"],
            row["series_id"],
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany(
        """
        INSERT INTO RAW.FRED_OBSERVATIONS (DATE, VALUE, INDICATOR_NAME, SERIES_ID)
        VALUES (%s, %s, %s, %s)
        """,
        rows
    )
    print(f"Loaded {len(rows)} rows into RAW.FRED_OBSERVATIONS")


def run():
    print("Starting extraction...")
    df = fetch_all_indicators()

    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        create_raw_table(cursor)
        truncate_and_load(cursor, df)
        conn.commit()
        print("Done. Data loaded successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()