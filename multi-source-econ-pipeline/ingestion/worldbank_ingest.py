import requests
from utils import get_snowflake_connection, execute_query

BASE_URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"

INDICATORS = {
    "NY.GDP.MKTP.CD": "gdp_current_usd",
    "FP.CPI.TOTL.ZG": "inflation_rate",
    "SL.UEM.TOTL.ZS": "unemployment_rate"
}

COUNTRIES = ["USA", "GBR", "DEU", "JPN", "IND", "BRA", "CHN"]

# Check the latest date already loaded for a given series.
def get_latest_year(conn, indicator_id, country_code):
    """Check the latest year already loaded for a given indicator and country."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(YEAR)
        FROM RAW.WORLDBANK_INDICATORS
        WHERE INDICATOR_ID = %s
        AND COUNTRY_CODE = %s
    """, (indicator_id, country_code))
    result = cursor.fetchone()[0]
    cursor.close()
    return int(result) if result else 2000  # if table is empty, default to 2000 which was the start year of the data already loaded.

# Fetch observations from World Bank starting after the latest loaded date.
def fetch_indicator(country, indicator_id):
    url = BASE_URL.format(country=country, indicator=indicator_id)
    params = {
        "format": "json",
        "per_page": 100,
        "mrv": 25
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    if len(data) < 2 or data[1] is None:
        return []
    return data[1]

def create_table(conn):
    query = """
        CREATE TABLE IF NOT EXISTS RAW.WORLDBANK_INDICATORS (
            COUNTRY_CODE VARCHAR,
            COUNTRY_NAME VARCHAR,
            INDICATOR_ID VARCHAR,
            INDICATOR_NAME VARCHAR,
            YEAR INTEGER,
            VALUE FLOAT,
            LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    execute_query(conn, query)

def insert_records(conn, indicator_id, indicator_name, records, latest_year):
    query = """
        INSERT INTO RAW.WORLDBANK_INDICATORS
            (COUNTRY_CODE, COUNTRY_NAME, INDICATOR_ID, INDICATOR_NAME, YEAR, VALUE)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            r["countryiso3code"],
            r["country"]["value"],
            indicator_id,
            indicator_name,
            int(r["date"]),
            float(r["value"]) if r["value"] is not None else None
        )
        for r in records
        if r["value"] is not None and int(r["date"]) > latest_year
    ]
    if not rows:
        print(f" No new rows for {indicator_id}")
        return
    execute_query(conn, query, rows)
    print(f" Inserted {len(rows)} new rows for {indicator_id}")

def run():
    conn = get_snowflake_connection()
    create_table(conn)

    for indicator_id, indicator_name in INDICATORS.items():
        for country in COUNTRIES:
            print(f"Fetching {indicator_id} for {country}...")
            latest_year = get_latest_year(conn, indicator_id, country)
            print(f" Latest year in Snowflake: {latest_year}")
            records = fetch_indicator(country, indicator_id)
            if records:
                insert_records(conn, indicator_id, indicator_name, records, latest_year)

    conn.close()
    print("World Bank ingestion complete.")

if __name__ == "__main__":
    run()