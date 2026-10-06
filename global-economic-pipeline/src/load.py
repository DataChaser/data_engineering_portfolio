from dotenv import load_dotenv
from utils import setup_logging, get_snowflake_connection

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

stage = "ECON_DB.RAW.ECON_GCS_STAGE"
worldbank_table = "ECON_DB.RAW.WORLDBANK_RAW"
imf_table = "ECON_DB.RAW.IMF_RAW"
undp_table = "ECON_DB.RAW.UNDP_RAW"

# Function to load data
def load_source(cursor, table, source_folder):
    source_name = source_folder.rstrip("/")
    logger.info(f"Loading {source_name}...")

    # Truncate the RAW table to ensures no duplicate rows
    cursor.execute(f"TRUNCATE TABLE {table}")
    logger.info(f"  Truncated {table}")

    cursor.execute(f"""
        COPY INTO {table} (RAW_DATA)
        FROM @{stage}/{source_folder}
        FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
        FORCE = TRUE
        ON_ERROR = CONTINUE
    """)
    logger.info(f" COPY INTO complete for {source_name}")

    # Verifying row count to confirm data actually landed.
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    row_count = cursor.fetchone()[0]
    logger.info(f" {row_count:,} rows in {table}")

    return row_count

def run():
    logger.info("Starting Snowflake load")

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        wb_rows = load_source(cursor, worldbank_table, "worldbank/")
        imf_rows = load_source(cursor, imf_table, "imf/")
        undp_rows = load_source(cursor, undp_table, "undp/")

        conn.commit()

        logger.info("Snowflake load complete")
        logger.info(f"WORLDBANK_RAW: {wb_rows:,} rows")
        logger.info(f"IMF_RAW: {imf_rows:,} rows")
        logger.info(f"UNDP_RAW: {undp_rows:,} rows")

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed. All changes rolled back: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run()