# This script provides the logging setup, GCS client, BigQuery client, and retry decorator.
# Every other script in src/ imports from here.

import os
import logging
from dotenv import load_dotenv
from google.cloud import storage, bigquery
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

# GCS client setup
def get_gcs_client():
    project_root = os.getenv("PROJECT_ROOT", os.getcwd())
    credentials_path = os.path.join(project_root, os.getenv("GCP_CREDENTIALS_PATH"))
    return storage.Client.from_service_account_json(credentials_path)

# BigQuery client setup
def get_bq_client():
    project_root = os.getenv("PROJECT_ROOT", os.getcwd())
    credentials_path = os.path.join(project_root, os.getenv("GCP_CREDENTIALS_PATH"))
    return bigquery.Client.from_service_account_json(
        credentials_path,
        project=os.getenv("GCP_PROJECT_ID")
    )

# Retry decorator setup
_logger = logging.getLogger(__name__)
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(_logger, logging.WARNING),
    reraise=True
)
def api_retry_wrapper(func, *args, **kwargs):
    return func(*args, **kwargs)