import os
import logging
import requests
from dotenv import load_dotenv
from utils import setup_logging, get_gcs_client, api_retry_wrapper

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

# URLs for parquet files and zone lookup
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
zone_lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

months = [
    "2025-09", "2025-10", "2025-11", "2025-12",
    "2026-01", "2026-02", "2026-03", "2026-04",
    "2026-05", "2026-06", "2026-07", "2026-08"
]

chunk_size = 16 * 1024 * 1024  # will read 16mb at a time when streaming from source to GCS

# Check if file exists. If exists, then skip it
def file_exists_in_gcs(bucket, blob_path):
    blob = bucket.blob(blob_path)
    return blob.exists()

# Function to stream upload file to GCS without any local downloads
def stream_to_gcs(url, bucket, blob_path, content_type):
    blob = bucket.blob(blob_path)
    with api_retry_wrapper(requests.get, url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with blob.open("wb", content_type=content_type) as gcs_file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    gcs_file.write(chunk)

# Function to upload one month's data
def upload_trip_file(bucket, year_month):
    year, month = year_month.split("-")
    blob_path = f"raw/yellow_taxi/year={year}/month={month}/yellow_tripdata_{year_month}.parquet"

    if file_exists_in_gcs(bucket, blob_path):
        logger.info(f"{year_month} already exists in GCS. Skipping")
        return

    url = base_url.format(year_month=year_month)
    logger.info(f"Streaming {year_month} to GCS: {blob_path}")

    stream_to_gcs(url, bucket, blob_path, content_type="application/octet-stream")
    logger.info(f"{year_month} uploaded successfully")

# Upload zone lookup to GCS
def upload_zone_lookup(bucket):
    blob_path = "raw/zone_lookup/taxi_zone_lookup.csv"

    logger.info("Uploading zone lookup CSV to GCS")
    stream_to_gcs(zone_lookup_url, bucket, blob_path, content_type="text/csv")
    logger.info("Zone lookup uploaded successfully")


def run():
    logger.info("Starting extraction to GCS")

    client = get_gcs_client()
    bucket = client.bucket(os.getenv("GCS_BUCKET_NAME"))

    uploaded = 0
    skipped = 0
    failed = []

    for year_month in months:
        try:
            upload_trip_file(bucket, year_month)
            uploaded += 1
        except Exception as e:
            logger.error(f"Failed to upload {year_month}: {e}")
            failed.append(year_month)

    try:
        upload_zone_lookup(bucket)
    except Exception as e:
        logger.error(f"Failed to upload zone lookup: {e}")

    logger.info("Extraction complete")
    logger.info(f"months attempted: {len(months)}")
    if failed:
        logger.warning(f"Failed months: {failed}")
    else:
        logger.info("No failures")


if __name__ == "__main__":
    run()