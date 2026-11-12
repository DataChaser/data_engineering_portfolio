# Runs the whole clickstream pipeline in order: six Spark jobs on GCP, the Silver checks,
# the BigQuery load and the Gold checks. No schedule since data is one time update

import os
import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

logger = logging.getLogger("clickstream_dag")

# The project folder inside the container
project_root = "/usr/local/airflow/project"

# Setting the key filepath
os.environ.setdefault("PROJECT_ROOT", project_root)

# Add the project root and src/ to Python's path
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

# Settings
project = os.getenv("GCP_PROJECT_ID")
region = os.getenv("GCP_REGION")
service_account = os.getenv("GCP_SERVICE_ACCOUNT")
raw_bucket = os.getenv("GCS_RAW_BUCKET")
processed_bucket = os.getenv("GCS_PROCESSED_BUCKET")
processed = f"gs://{processed_bucket}"
credentials_path = os.path.join(project_root, os.getenv("GCP_CREDENTIALS_PATH"))

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Running spark job function
def run_spark_job(job_name, script, args):
    # Imported here and not at the top, because the import is slow and the top of
    # the file runs every time Airflow re-reads it (as in the NYC taxi DAG).
    from google.cloud import dataproc_v1

    # The client that talks to Serverless Spark, signed in with the service account's
    # key file. api_endpoint points at the Delhi region (the --region flag).
    client = dataproc_v1.BatchControllerClient.from_service_account_file(
        credentials_path,
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"},
    )

    # The batch itself: the same things the gcloud command described.
    batch = dataproc_v1.Batch(
        pyspark_batch=dataproc_v1.PySparkBatch(
            # The script in the code/ folder of the processed bucket (Day 3)
            main_python_file_uri=f"{processed}/code/{script}",
            args=args,
        ),
        environment_config=dataproc_v1.EnvironmentConfig(
            # The identity the job runs as (the --service-account flag)
            execution_config=dataproc_v1.ExecutionConfig(service_account=service_account)
        ),
        # Pin the runtime so a rerun behaves the same
        runtime_config=dataproc_v1.RuntimeConfig(version="2.2"),
    )

    batch_id = f"{job_name.replace('_', '-')}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    logger.info(f"Submitting batch {batch_id} ({script})")

    operation = client.create_batch(
        parent=f"projects/{project}/locations/{region}",
        batch=batch,
        batch_id=batch_id,
    )

    result = operation.result()

    if result.state != dataproc_v1.Batch.State.SUCCEEDED:
        raise RuntimeError(f"Batch {batch_id} ended in state {result.state.name}")
    logger.info(f"Batch {batch_id} succeeded")


# Function to run silver layer data validation
def run_silver_validation():
    from validate import validate_silver
    passed = validate_silver()
    return "sessions" if passed else "silver_validation_failed"


# Function to load the Gold tables
def load_gold_tables():
    from google.cloud import bigquery

    client = bigquery.Client.from_service_account_json(credentials_path, project=project)
    with open(os.path.join(project_root, "src", "load-gold-tables.sql")) as f:
        sql = f.read()
    client.query(sql, location=region).result()
    logger.info("Gold tables loaded")


# Function to run gold layer data validation
def run_gold_validation():
    from validate import validate_gold
    passed = validate_gold()
    return "pipeline_complete" if passed else "gold_validation_failed"


# Defining the DAG
with DAG(
    dag_id="ecommerce_clickstream_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce-clickstream"],
    doc_md="""
    ## E-commerce clickstream pipeline
    Runs the PySpark jobs on serverless Spark (Bronze, Silver, sessions, funnel, customers,
    schema), checks Silver and Gold with Great Expectations, and loads the Gold tables
    into BigQuery.
    """,
) as dag:

    # The six Spark jobs: task name, script in the code/ folder, and its arguments.
    spark_jobs = [
        ("bronze", "parquet-convert.py", [f"gs://{raw_bucket}/events/*.csv.gz", f"{processed}/bronze/events"]),
        ("silver", "data-cleaning.py", [processed]),
        ("sessions", "session-metrics.py", [processed]),
        ("funnel", "funnel-metrics.py", [processed]),
        ("customers", "customer-summary.py", [processed]),
        ("schema", "schema.py", [processed]),
    ]

    # One PythonOperator (one task) per Spark job
    spark_tasks = {}
    for name, script, args in spark_jobs:
        spark_tasks[name] = PythonOperator(
            task_id=name,
            python_callable=run_spark_job,
            op_kwargs={"job_name": name, "script": script, "args": args},
        )

    # Silver checks
    check_silver = BranchPythonOperator(
        task_id="check_silver", python_callable=run_silver_validation,
    )

    # Empty operator: the pipeline stops here if the Silver checks fail
    silver_validation_failed = EmptyOperator(task_id="silver_validation_failed")

    # Load the four Gold tables into BigQuery
    load_gold = PythonOperator(
        task_id="load_gold", python_callable=load_gold_tables,
    )

    # Gold checks
    check_gold = BranchPythonOperator(
        task_id="check_gold", python_callable=run_gold_validation,
    )

    # Empty operators: the end of the pipeline, one for each outcome
    pipeline_complete = EmptyOperator(task_id="pipeline_complete")
    gold_validation_failed = EmptyOperator(task_id="gold_validation_failed")

    # Task dependencies
    spark_tasks["bronze"] >> spark_tasks["silver"] >> check_silver
    check_silver >> [spark_tasks["sessions"], silver_validation_failed]
    spark_tasks["sessions"] >> spark_tasks["funnel"] >> spark_tasks["customers"] >> spark_tasks["schema"]
    spark_tasks["schema"] >> load_gold >> check_gold
    check_gold >> [pipeline_complete, gold_validation_failed]