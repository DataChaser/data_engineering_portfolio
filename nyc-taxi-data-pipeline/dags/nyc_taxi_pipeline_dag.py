# Runs on the 10th of every month at midnight, processing the previous month's data.
# TARGET_MONTH is derived automatically from data_interval_start — no hardcoding needed.

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from dbt.cli.main import dbtRunner

# Add project root and src/ to Python path so DAG tasks can import from src/
project_root = "/usr/local/airflow/project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

# dbt paths
# /tmp paths for logs and target to avoid Docker permission issues
dbt_project_dir = os.path.join(project_root, "dbt_project", "nyc_taxi_data_pipeline")
dbt_log_path    = "/tmp/dbt_logs"
dbt_target_path = "/tmp/dbt_target"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Getting the year-month to be processed
def get_target_month(context) -> str:
    return context["data_interval_start"].strftime("%Y-%m")

# Data extraction
def run_extract(**context):
    from extract import run
    year_month = get_target_month(context)
    run(year_month=year_month)

# Data load to GCS
def run_load(**context):
    from load import run
    year_month = get_target_month(context)
    run(year_month=year_month)

# Running dbt models
def run_dbt(command: str, select: str):
    runner = dbtRunner()
    result = runner.invoke([
        command,
        "--select", select,
        "--profiles-dir", dbt_project_dir,
        "--project-dir", dbt_project_dir,
        "--target-path", dbt_target_path,
        "--log-path", dbt_log_path,
    ])
    if not result.success:
        raise Exception(f"dbt {command} --select {select} failed")

# Great Expectations validation
def run_gx_validation(**context):
    from validate import validate
    year_month = get_target_month(context)
    passed = validate(year_month)
    return "dbt_marts" if passed else "validation_failed"

# Defining the DAG
with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 9, 1),
    schedule="0 0 1 * *",
    catchup=False,
    tags=["nyc_taxi", "monthly"],
    doc_md="""
    ## NYC Yellow Taxi Monthly Pipeline
    Processes NYC TLC yellow taxi trip records on a monthly schedule.
    Lands parquet files to GCS, loads to BigQuery raw layer, transforms through dbt staging and mart models, 
    validates with Great Expectations. TARGET_MONTH derived automatically from Airflow execution date.
    """
) as dag:

    # Land parquet file for target month from TLC website to GCS
    extract = PythonOperator(
        task_id="extract", python_callable=run_extract,
    )

    # Load parquet from GCS into BigQuery raw_trips table
    load_raw = PythonOperator(
        task_id="load_raw", python_callable=run_load,
    )

    # Build dbt staging views
    dbt_staging = PythonOperator(
        task_id="dbt_staging", python_callable=run_dbt,
        op_kwargs={"command": "run", "select": "staging"},
    )

    # Run dbt tests on staging models
    dbt_staging_tests = PythonOperator(
        task_id="dbt_staging_tests", python_callable=run_dbt,
        op_kwargs={"command": "test", "select": "staging"},
    )

    # Build dbt intermediate and mart models
    dbt_marts = PythonOperator(
        task_id="dbt_marts",
        python_callable=run_dbt,
        op_kwargs={"command": "run", "select": "intermediate+ mart_daily_revenue mart_trip_duration mart_pickup_zones mart_payment_summary"},
    )

    # GX validation on staging output
    gx_validation = BranchPythonOperator(
        task_id="gx_validation", python_callable=run_gx_validation,
    )

    # Run dbt tests on mart models
    dbt_mart_tests = PythonOperator(
        task_id="dbt_mart_tests",
        python_callable=run_dbt,
        op_kwargs={"command": "test", "select": "mart_daily_revenue mart_trip_duration mart_pickup_zones mart_payment_summary"},
    )

    # Empty operator — pipeline stops here if GX validation fails
    validation_failed = EmptyOperator(
        task_id="validation_failed",
    )

    # Task dependencies
    extract >> load_raw >> dbt_staging >> dbt_staging_tests >> gx_validation
    gx_validation >> [dbt_marts, validation_failed]
    dbt_marts >> dbt_mart_tests