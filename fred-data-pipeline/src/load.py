import sys
import os
import io
import pandas as pd
from dotenv import load_dotenv
import gcsfs
from utils import logger, get_snowflake_connection, get_gcs_client
from extract import extract_all, get_indicator_list

import logging
logger = logging.getLogger(__name__)

load_dotenv()

# GCS configuration
gcs_bucket     = os.getenv("GCP_BUCKET") #the landing zone bucket, from .env
gcs_tmp_path   = "raw/fred_observations_tmp.parquet" #temporary path to protect against corrupted writes
gcs_final_path = "raw/fred_observations.parquet" #the live path that Snowflake COPY INTO reads from

# Snowflake configuration
raw_table     = "FRED_DB.RAW.FRED_OBSERVATIONS" #raw data loaded from GCS
staging_table = "FRED_DB.RAW.FRED_OBSERVATIONS_STAGING" #temp table created after each MERGE
gcs_stage     = "FRED_DB.RAW.FRED_GCS_STAGE" #external stage pointing to thr GCS bucket

# Getting list of series loaded in snowflake to be compared against edits made to the indicator mapping file
def get_snowflake_series(cursor):
    try:
        cursor.execute(f"SELECT DISTINCT SERIES_ID FROM {raw_table}")
        rows = cursor.fetchall()
        return {row[0] for row in rows }
    except Exception:
        logger.info(f"Raw table is empty or does not exist")
        return set()

# Delete data for indicators removed from indicators mapping file to remove stale data
def delete_discontinued_series(cursor, snowflake_series: set, csv_series: set):
    discontinued_series = snowflake_series - csv_series
    if not discontinued_series:
        logger.info("No discontinued series found")
        return
    
    discontinued_series_list = ", ".join(f"'{series}'" for series in discontinued_series)
    logger.info(f"Deleting {len(discontinued_series_list)} series: {discontinued_series}")

    cursor.execute(f"DELETE FROM {raw_table} WHERE SERIES_ID IN ({discontinued_series_list})")
    logger.info("Discontinued series deleted")

#Writing to Google Cloud Storage as a Parquet file
def write_to_gcs(df: pd.DataFrame):
    credentials_path = os.getenv("GCP_CREDENTIALS_PATH")
    fs = gcsfs.GCSFileSystem(token=credentials_path)

    tmp_path   = f"{gcs_bucket}/{gcs_tmp_path}"
    final_path = f"{gcs_bucket}/{gcs_final_path}"

    logger.info(f"Writing {len(df):,} rows to GCS as Parquet")

    with fs.open(tmp_path, "wb") as f:
        df.to_parquet(f, index=False)
    logger.info(f"Written to temp path: gs://{tmp_path}")
    fs.rename(tmp_path, final_path)
    logger.info(f"Live file updated: gs://{final_path}")

def copy_into_staging(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {staging_table} (
            DATE DATE,
            VALUE FLOAT,
            INDICATOR_NAME VARCHAR(100),
            SERIES_ID VARCHAR(50),
            FREQUENCY VARCHAR(20)
        )
    """)
    cursor.execute(f"TRUNCATE TABLE {staging_table}")
    logger.info(f"loading Parquet file from GCS into {staging_table}")
    cursor.execute(f"""
        COPY INTO {staging_table}
        FROM @{gcs_stage}/fred_observations.parquet
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    """)
    logger.info("COPY INTO staging complete")

# Adding new data and revising existing data in Snowflake raw table
def merge_into_raw(cursor):
    logger.info(f"merging staging into {raw_table}")

    cursor.execute(f"""
        MERGE INTO {raw_table} AS target
        USING {staging_table} AS source
        ON  target.SERIES_ID = source.SERIES_ID
        AND target.DATE = source.DATE
        WHEN MATCHED THEN UPDATE SET
            target.VALUE = source.VALUE,
            target.INDICATOR_NAME = source.INDICATOR_NAME,
            target.FREQUENCY = source.FREQUENCY
        WHEN NOT MATCHED THEN INSERT (
            DATE, VALUE, SERIES_ID, INDICATOR_NAME, FREQUENCY
        )
        VALUES (
            source.DATE,
            source.VALUE,
            source.SERIES_ID,
            source.INDICATOR_NAME,
            source.FREQUENCY
        )
    """)
    logger.info(f"Merge complete")

def run():
    logger.info("Starting the ingestion pipeline")

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Load indicators and handle orphan deletes ─────────────────────────
        indicators = get_indicator_list()
        csv_series = set(indicators["series_id"].tolist())

        snowflake_series = get_snowflake_series(cursor)
        delete_discontinued_series(cursor, snowflake_series, csv_series)

        # Data extraction 
        logger.info("starting extraction")
        df = extract_all()
        if df.empty:
            logger.warning("Extraction returned empty DataFrame. Stopping pipeline")
            return

        # Write data to GCS, copy into staging and merge to raw table
        write_to_gcs(df)
        copy_into_staging(cursor)
        merge_into_raw(cursor)

        #Delete the temp staging table
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
        logger.info("Staging table dropped")

        #Commit the changes
        conn.commit()

        # Verify changes
        cursor.execute(f"SELECT COUNT(*) FROM {raw_table}")
        row_count = cursor.fetchone()[0]
        logger.info(f"Ingestion complete. {row_count:,} total rows in {raw_table}")

    except Exception as e:
        # roll back any partial Snowflake changes on unexpected failure to ensure that the raw table is never left in a half-loaded state
        conn.rollback()
        logger.error(f"Pipeline failed. {e}")
        raise

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run()



