import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils import setup_logging, api_retry_wrapper, get_gcs_client

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

base_url = "https://api.worldbank.org/v2/country/{countries}/indicator/{indicator}"

# Polite delay between API requests
rate_limit_seconds = 0.5

#Loading countries and indicators from the mapping files
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
        raise FileNotFoundError(f"Country config not found at {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} countries from countries.csv")
    country_string = ";".join(df["country_code"].tolist())
    return df, country_string

# Fetching the indicators
def fetch_indicator(country_string, indicator_code):
    url    = base_url.format(countries=country_string, indicator=indicator_code)
    params = {"format": "json", "per_page": 1000, "page": 1}

    # Fetch page 1 first to get total_pages from metadata
    response = api_retry_wrapper(requests.get, url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Sanity check so that there are no weird responses
    if not data or len(data) < 2 or data[1] is None:
        logger.warning(f"Empty or weird response for {indicator_code}")
        return []

    metadata = data[0]
    all_records = data[1]
    total_pages = int(metadata.get("pages", 1))
    logger.info(f"Page 1/{total_pages}. {len(all_records)} records")

    # Fetch remaining pages if any exist
    for page in range(2, total_pages + 1):
        params["page"] = page
        response = api_retry_wrapper(requests.get, url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data[1] or []
        all_records.extend(records)
        logger.info(f"Page {page}/{total_pages}. {len(records)} records")
        time.sleep(rate_limit_seconds)

    return all_records

# Write to GCS
def write_to_gcs(payload, indicator_code, extracted_at):
    client    = get_gcs_client()
    bucket    = client.bucket(os.getenv("GCS_BUCKET"))
    blob_path = f"worldbank/{indicator_code}/{extracted_at}.json"
    blob      = bucket.blob(blob_path)

    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    
    logger.info(
        f"Written to gs://{os.getenv('GCS_BUCKET')}/{blob_path}. ({payload['record_count']} records)"
    )

def run():
    logger.info("Starting World Bank extraction")

    indicators_df = load_indicators("worldbank")
    countries_df, country_string = load_countries()

    extracted_at = datetime.now().strftime("%Y%m%d_%H%M%S")

    total_files = 0
    total_records = 0
    failed = []

    for _, row in indicators_df.iterrows():
        indicator_code = row["series_id"]
        indicator_name = row["indicator_name"]

        logger.info(f"Fetching {indicator_code} ({indicator_name})")

        try:
            records = fetch_indicator(country_string, indicator_code)
        except Exception as e:
            logger.error(f"Failed to fetch {indicator_code}: {e}")
            failed.append(indicator_code)
            continue

        if not records:
            logger.warning(f"No records for {indicator_code}. Skipping")
            continue

        payload = {
            "source": "worldbank",
            "indicator_code": indicator_code,
            "indicator_name": indicator_name,
            "extracted_at": extracted_at,
            "record_count": len(records),
            "records": records
        }

        try:
            write_to_gcs(payload, indicator_code, extracted_at)
            total_files += 1
            total_records += len(records)
        except Exception as e:
            logger.error(f"GCS write failed for {indicator_code}: {e}")
            failed.append(indicator_code)

        time.sleep(rate_limit_seconds)

    logger.info("World Bank extraction complete")
    logger.info(f"Files written: {total_files}")
    logger.info(f"Total records: {total_records:,}")
    if failed:
        logger.warning(f"Failed indicators ({len(failed)}): {failed}")
    else:
        logger.info("No failures")

if __name__ == "__main__":
    run()
