# This script provides the logging setup, Snowflake connection, GCS client, API retry decorator.
# Every other script in src/ imports from here.

import os
import logging
from dotenv import load_dotenv
from google.cloud import storage
import snowflake.connector
from tenacity import (retry, stop_after_attempt, wait_exponential, 
                      retry_if_exception_type, before_sleep_log)

load_dotenv()

# Logging setup
def setup_logging():    
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(), logging.FileHandler("pipeline.log")],
        level=getattr(logging, log_level)
        )
logger = logging.getLogger(__name__)

# Snowflake connection setup
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

# GCS client setup
def get_gcs_client():
    project_root = os.getenv("PROJECT_ROOT", os.getcwd())
    credentials_path = os.path.join(project_root, os.getenv("GCP_CREDENTIALS_PATH"))
    return storage.Client.from_service_account_json(credentials_path)

#
# Rety decorator setup
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(_logger, logging.WARNING),
    reraise=True
)
def api_retry_wrapper(func, *args, **kwargs):
    return func(*args, **kwargs)
