import requests
import os
from datetime import datetime
from dotenv import load_dotenv
from utils import get_snowflake_connection, execute_query

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "GNPCA": "real_gnp",
    "UNRATE": "unemployment_rate",
    "CPIAUCSL": "cpi"
}

# Check the latest date already loaded for a given series.
def get_latest_date(conn, series_id):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(DATE) 
        FROM RAW.FRED_OBSERVATIONS 
        WHERE SERIES_ID = %s
    """, (series_id,))
    result = cursor.fetchone()[0]
    cursor.close()
    return str(result) if result else "2000-01-01"  #this returns the date that was selected as the starting data when the data was first loaded

# Fetch observations from FRED starting after the latest loaded date.
def fetch_fred_series(series_id, start_date):
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()["observations"]

def create_table(conn):
    query = """
        CREATE TABLE IF NOT EXISTS RAW.FRED_OBSERVATIONS (
            SERIES_ID  VARCHAR,
            SERIES_NAME VARCHAR,
            DATE DATE,
            VALUE FLOAT,
            LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    execute_query(conn, query)

def insert_records(conn, series_id, series_name, observations):
    query = """
        INSERT INTO RAW.FRED_OBSERVATIONS
            (SERIES_ID, SERIES_NAME, DATE, VALUE)
        VALUES (%s, %s, %s, %s)
    """
    rows = [
        (series_id, series_name, obs["date"],
         float(obs["value"]) if obs["value"] != "." else None)
        for obs in observations
    ]
    execute_query(conn, query, rows)
    print(f" Inserted {len(rows)} rows for {series_id}")

def run():
    conn = get_snowflake_connection()
    create_table(conn)

    for series_id, series_name in SERIES.items():
        print(f"Fetching {series_id}...")
        latest_date = get_latest_date(conn, series_id)
        print(f" Latest date in Snowflake: {latest_date}")
        observations = fetch_fred_series(series_id, latest_date)
        insert_records(conn, series_id, series_name, observations)

    conn.close()
    print("FRED ingestion complete.")

if __name__ == "__main__":
    run()