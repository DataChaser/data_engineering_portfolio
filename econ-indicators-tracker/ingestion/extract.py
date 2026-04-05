import requests
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

INDICATORS = {
    "GDP Growth": "GDP",
    "Inflation": "CPIAUCSL",
    "Unemployment": "UNRATE",
    "Interest Rate": "FEDFUNDS",
    "Housing Starts": "HOUST",
    "Retail Sales": "RSXFS",
}


def fetch_indicator(series_id: str, indicator_name: str):
    params = {
        "series_id":         series_id,
        "api_key":           FRED_API_KEY,
        "file_type":         "json",
        "observation_start": "2000-01-01",
        "sort_order":        "asc",
    }

    response = requests.get(FRED_BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data["observations"])
    df = df[["date", "value"]]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["value"] = df["value"].replace({np.nan: None})
    df["date"] = pd.to_datetime(df["date"])
    df["indicator_name"] = indicator_name
    df["series_id"] = series_id

    return df


def fetch_all_indicators():
    all_dfs = []

    for indicator_name, series_id in INDICATORS.items():
        print(f"Fetching {indicator_name} ({series_id})...")
        df = fetch_indicator(series_id, indicator_name)
        all_dfs.append(df)
        print(f"  Got {len(df)} rows")

    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Total rows fetched: {len(combined)}")
    return combined


if __name__ == "__main__":
    df = fetch_all_indicators()
    print(df.head(10))
    print(df.dtypes)