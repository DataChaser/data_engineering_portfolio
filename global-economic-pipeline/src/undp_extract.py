import os
import json
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils import setup_logging, api_retry_wrapper, get_gcs_client

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

base_url = "https://hdrdata.org/api/CompositeIndices/query"

# Loading indicators and countries
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
    country_string = ",".join(df["country_code"].tolist())
    return df, country_string

# Fetching indicator-level data
def fetch_all(indicator_string, country_string):
    params = {
        "apikey": os.getenv("UNDP_API_KEY"),
        "countryOrAggregation": country_string,
        "indicator": indicator_string
    }

    response = api_retry_wrapper(requests.get, base_url, params=params, timeout=60)
    response.raise_for_status()

    records = response.json()

    if not records:
        logger.warning("Empty response from UNDP API")
        return []

    logger.info(f"  Fetched {len(records)} records")
    return records

# Writing data to GCS
def write_to_gcs(payload, extracted_at):
    client = get_gcs_client()
    bucket = client.bucket(os.getenv("GCS_BUCKET"))
    blob_path = f"undp/{extracted_at}.json"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    logger.info(f"Written to gs://{os.getenv('GCS_BUCKET')}/{blob_path}")

def run():
    logger.info("Starting UNDP extraction")

    indicators_df = load_indicators("undp")
    countries_df, country_string = load_countries()

    indicator_string = ",".join(indicators_df["series_id"].tolist())
    extracted_at = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info(f"Fetching {len(indicators_df)} indicators for {len(countries_df)} countries")

    try:
        records = fetch_all(indicator_string, country_string)
    except Exception as e:
        logger.error(f"UNDP extraction failed: {e}")
        return

    if not records:
        logger.warning("No records returned — nothing to write")
        return

    # Wrap records in metadata envelope before writing to GCS
    payload = {
        "source": "undp",
        "indicators": indicators_df["series_id"].tolist(),
        "extracted_at": extracted_at,
        "record_count": len(records),
        "records": records
    }

    try:
        write_to_gcs(payload, extracted_at)
    except Exception as e:
        logger.error(f"GCS write failed: {e}")
        return
    
    logger.info("UNDP extraction complete")
    logger.info(f"Records written: {len(records):,}")

if __name__ == "__main__":
    run()