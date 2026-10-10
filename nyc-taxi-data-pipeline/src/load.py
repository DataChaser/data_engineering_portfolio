import os
import logging
import gcsfs
import pyarrow.parquet as pq
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from utils import setup_logging, get_bq_client

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

months = [
    "2025-09", "2025-10", "2025-11", "2025-12",
    "2026-01", "2026-02", "2026-03", "2026-04",
    "2026-05", "2026-06", "2026-07", "2026-08"
]

def get_table_refs():
    project = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET_RAW")
    return {
        "trips": f"{project}.{dataset}.raw_trips",
        "zone_lookup": f"{project}.{dataset}.raw_zone_lookup"
    }


def month_already_loaded(client, table_ref, year_month):
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{table_ref}`
        WHERE source_month = '{year_month}'
    """
    try:
        result = client.query(query).result()
        row_count = list(result)[0].row_count
        return row_count > 0
    except Exception:
        return False

# Function to load one month's data
def load_trips_month(client, year_month, table_ref, bucket_name):
    year, month = year_month.split("-")
    gcs_path = f"gs://{bucket_name}/raw/yellow_taxi/year={year}/month={month}/yellow_tripdata_{year_month}.parquet"

    project = os.getenv("GCP_PROJECT_ID")
    credentials_path = os.path.join(
        os.getenv("PROJECT_ROOT", os.getcwd()),
        os.getenv("GCP_CREDENTIALS_PATH")
    )

    fs = gcsfs.GCSFileSystem(project=project, token=credentials_path)

    logger.info(f"Reading {year_month} from GCS: {gcs_path}")

    parquet_file = pq.ParquetFile(gcs_path, filesystem=fs)
    total_row_groups = parquet_file.metadata.num_row_groups
    logger.info(f"{year_month} has {total_row_groups} row groups")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )

    for i in range(total_row_groups):
        chunk = parquet_file.read_row_group(i).to_pandas()
        chunk["source_month"] = year_month

        logger.info(f"Loading row group {i+1}/{total_row_groups}. {len(chunk):,} rows")

        load_job = client.load_table_from_dataframe(chunk, table_ref, job_config=job_config)
        load_job.result()

    table = client.get_table(table_ref)
    logger.info(f"{year_month} loaded successfully. {table.num_rows:,} total rows in table")


def load_zone_lookup(client, table_ref, bucket_name):
    gcs_uri = f"gs://{bucket_name}/raw/zone_lookup/taxi_zone_lookup.csv"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    logger.info("Loading zone lookup from GCS to BigQuery")
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()

    table = client.get_table(table_ref)
    logger.info(f"Zone lookup loaded — {table.num_rows} rows")


def run():
    logger.info("Starting BigQuery load")

    client = get_bq_client()
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    table_refs = get_table_refs()

    loaded = 0
    skipped = 0
    failed = []

    for year_month in months:
        if month_already_loaded(client, table_refs["trips"], year_month):
            logger.info(f"{year_month} already loaded in BigQuery. Skipping")
            skipped += 1
            continue

        try:
            load_trips_month(client, year_month, table_refs["trips"], bucket_name)
            loaded += 1
        except Exception as e:
            logger.error(f"Failed to load {year_month}: {e}")
            failed.append(year_month)

    try:
        load_zone_lookup(client, table_refs["zone_lookup"], bucket_name)
    except Exception as e:
        logger.error(f"Failed to load zone lookup: {e}")

    logger.info("BigQuery load complete")
    logger.info(f"Months loaded: {loaded}. Skipped: {skipped}")
    if failed:
        logger.warning(f"Failed months: {failed}")
    else:
        logger.info("No failures")


if __name__ == "__main__":
    run()