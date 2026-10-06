import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils import setup_logging, get_gcs_client
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

base_url = "https://www.imf.org/external/datamapper/api/v1"
rate_limit_seconds = 0.5

# Getting indicators and countries
def load_indicators(source):
    filepath = os.path.join("mapping", "indicators.csv")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Indicators file not found at {filepath}")
    df = pd.read_csv(filepath)
    filtered = df[df["source"] == source].reset_index(drop=True)
    logger.info(f"Loaded {len(filtered)} indicators for source '{source}'")
    return filtered

def load_countries():
    filepath = os.path.join("mapping", "countries.csv")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Countries file not found at {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} countries from countries.csv")
    return df, df["country_code"].tolist()

# Function to extract the data
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)
def fetch_indicator(indicator_code, country_list):
    url = f"{base_url}/{indicator_code}"
    response = requests.get(url, timeout=(10, 30))
    response.raise_for_status()

    data = response.json()

    if "values" not in data or indicator_code not in data["values"]:
        logger.warning(f"Unexpected response structure for {indicator_code}")
        return None

    all_countries = data["values"][indicator_code]

    # Filter to only our 30 countries
    filtered = {k: v for k, v in all_countries.items() if k in country_list}

    record_count = sum(len(years) for years in filtered.values())
    logger.info(f"  Fetched {len(filtered)} countries, {record_count} observations")

    return {"values": {indicator_code: filtered}}

# Write data to GCS
def write_to_gcs(payload, indicator_code, extracted_at):
    client = get_gcs_client()
    bucket = client.bucket(os.getenv("GCS_BUCKET"))
    blob_path = f"imf/{indicator_code}/{extracted_at}.json"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    logger.info(f"Written to gs://{os.getenv('GCS_BUCKET')}/{blob_path}")


def run():
    logger.info("Starting IMF extraction")

    indicators_df = load_indicators("imf")
    countries_df, countries = load_countries()
    extracted_at = datetime.now().strftime("%Y%m%d_%H%M%S")

    total_files = 0
    failed = []

    for _, row in indicators_df.iterrows():
        indicator_code = row["series_id"]
        indicator_name = row["indicator_name"]

        logger.info(f"Fetching {indicator_code} ({indicator_name})")

        data = fetch_indicator(indicator_code, set(countries))

        if data is None:
            logger.warning(f"No data for {indicator_code} — skipping")
            failed.append(indicator_code)
            continue

        payload = {
            "source": "imf",
            "indicator_code": indicator_code,
            "indicator_name": indicator_name,
            "extracted_at": extracted_at,
            "data": data
        }

        try:
            write_to_gcs(payload, indicator_code, extracted_at)
            total_files += 1
        except Exception as e:
            logger.error(f"GCS write failed for {indicator_code}: {e}")
            failed.append(indicator_code)

        time.sleep(rate_limit_seconds)

    logger.info("IMF extraction complete")
    logger.info(f"Files written: {total_files}")
    if failed:
        logger.warning(f"Failed indicators ({len(failed)}): {failed}")
    else:
        logger.info("No failures")

if __name__ == "__main__":
    run()