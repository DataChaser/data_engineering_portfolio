import logging
import os
from google.cloud import storage
import snowflake.connector
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

#Loading credentials
load_dotenv()

#Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline.log")],
    level=getattr(logging, LOG_LEVEL)
)
logger = logging.getLogger(__name__)

#Snowflake connection
def get_snowflake_connection():
    logger.info('Fetching Snowflake credentials')
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

#GCS Client
def get_gcs_client():
    gcp_credentials_path=os.getenv('GCP_CREDENTIALS_PATH')
    logger.info(f"Initializing gcs client from {gcp_credentials_path}")
    return storage.Client.from_service_account_json(gcp_credentials_path)

#retry decorator for external api calls to handle errors, retries, backoff etc.
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)

def api_retry_wrapper(func, *args, **kwargs):
    return func(*args, **kwargs)

