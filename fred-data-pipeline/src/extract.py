import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from utils import setup_logging, api_retry_wrapper

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

#FRED Data Configs
fred_base_url = "https://api.stlouisfed.org/fred/series/observations"
fred_api_key = os.getenv("FRED_API_KEY")
default_start_date = "1975-01-01"

rate_limit = 0.5

#Get list of indicators from indicator mapping file
def get_indicator_list():
    filepath  = "mapping"
    filename = "indicators.csv"
    indicator_file = os.path.join(filepath, filename)
    if not os.path.exists(indicator_file):
        raise FileNotFoundError(f"{filename} not found in {filepath}")
    else:
        logger.info(f"Loading file from {indicator_file}")

    indicators = pd.read_csv(indicator_file)
    logger.info(f"loaded {len(indicators)} series from indicators.csv")

    return indicators

#Function to fetch data for an indicator
def fetch_series(series_id, indicator_name, frequency):
    params = {
        "series_id": series_id,
        "api_key": fred_api_key,
        "file_type":"json",
        "observation_start": default_start_date,
        "sort_order": "asc",
    }

    response = api_retry_wrapper(requests.get, fred_base_url, params=params)
    response.raise_for_status()

    observations = response.json().get("observations", [])
    if not observations:
        logger.warning(f"{series_id}: FRED returned zero observations")
        return pd.DataFrame()

    df = pd.DataFrame(observations)
    df = df[["date", "value"]]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["value"] = df["value"].where(df["value"].notna(), other=None)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["series_id"] = series_id
    df["indicator_name"] = indicator_name
    df["frequency"] = frequency

    return df

#Extract data for all indicators
def extract_all():
    indicators = get_indicator_list()
    total_series = len(indicators)
    all_dfs = []
    failed_series = []

    for i, row in indicators.iterrows():
        series_id = row["series_id"]
        indicator_name = row["indicator_name"]
        frequency = row["frequency"]

        logger.info(
            f"[{i+1}/{total_series}] fetching {series_id} "
            f"({indicator_name}, {frequency})"
        )

        try:
            df = fetch_series(series_id, indicator_name, frequency)
            if not df.empty:
                all_dfs.append(df)
                logger.info(f"{series_id}: {len(df)} rows fetched")
            else:
                logger.warning(f"{series_id}: no data returned, skipping")
        except Exception as e:
            logger.error(f"{series_id}: failed after all retries -- {e}")
            failed_series.append(series_id)
        time.sleep(rate_limit)

    if failed_series:
        logger.error(
            f"Extraction completed with {len(failed_series)} failed series: "
            f"{failed_series}"
        )
    else:
        logger.info("All series fetched successfully")

    if not all_dfs:
        logger.warning("No data fetched across any series")
        return pd.DataFrame()
    combined = pd.concat(all_dfs, ignore_index=True)

    logger.info(
        f"Extraction complete. Total rows fetched: {len(combined)} across {combined['series_id'].nunique()} series"
    )
    logger.info(
        f"frequency breakdown: {combined['frequency'].value_counts().to_dict()}"
    )

    return combined


if __name__ == "__main__":
    df = extract_all()
    if not df.empty:
        print(df.head(10))


