# src/test_gcs.py
# Temporary — delete after Day 1 verification. Do not commit.
#
# What this does:
#   1. Reads GCS credentials from .env
#   2. Constructs the absolute credentials path correctly for both
#      local (os.getcwd()) and container (PROJECT_ROOT) environments
#   3. Writes a test file to GCS, reads it back, deletes it
#   4. Confirms the connection works end to end before any real data flows

import os
import logging
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

def test_gcs():
    # Construct absolute credentials path using PROJECT_ROOT if set,
    # otherwise fall back to current working directory.
    # This is the pattern used throughout the project to handle
    # Windows local paths vs Linux container paths.
    project_root = os.getenv("PROJECT_ROOT", os.getcwd())
    creds_path = os.path.join(project_root, os.getenv("GCP_CREDENTIALS_PATH"))

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    logger.info(f"Using credentials at: {creds_path}")

    client = storage.Client()
    bucket_name = os.getenv("GCS_BUCKET")
    bucket = client.bucket(bucket_name)

    # Write test file
    blob = bucket.blob("test/connection_test.json")
    blob.upload_from_string('{"test": "connection successful"}')
    logger.info(f"Written to gs://{bucket_name}/test/connection_test.json")

    # Read it back
    content = blob.download_as_text()
    logger.info(f"Read back: {content}")

    # Clean up
    blob.delete()
    logger.info("Test file deleted. GCS connection working.")

if __name__ == "__main__":
    test_gcs()