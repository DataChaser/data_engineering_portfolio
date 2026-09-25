# fred_data_pipeline.py
# airflow dag that orchestrates the full fred economic indicators pipeline
# runs on the 5th of every month at 7am, after most monthly FRED releases
# task order: extract + load -> dbt staging -> dbt staging tests -> gx validation -> dbt mart -> dbt mart tests
# each task only runs if the previous one succeeded
# if gx validation fails, dbt mart never runs so that bad data never reaches the mart

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from dbt.cli.main import dbtRunner

# adding the project root to Python's path so tasks can import from src/
project_root = "/usr/local/airflow/project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

# path to the dbt project, log and target to avoid permission errors
dbt_project_dir = os.path.join(project_root, "dbt_project/fred_data_pipeline")
dbt_log_path    = "/tmp/dbt_logs"
dbt_target_path = "/tmp/dbt_target"

# default arguments applied to all tasks
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# function to run the full ingestion pipeline
def run_load():
    from load import run
    run()

#function to run the dbt commands programmatically using the dbt Python API
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


# function to run great expectations against the snowflake staging table
# BranchPythonOperator returns the task_id of the next task to execute
#   if validation passes: proceed to dbt_mart
#   if validation fails: proceed to validation_failed, stopping the pipeline
def run_validation():
    from validate import validate
    passed = validate()
    if passed:
        return "dbt_mart"
    else:
        return "validation_failed"


with DAG(
    dag_id="fred_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 7 5 * *",
    catchup=False,
    tags=["fred", "economic-indicators"],
    doc_md="""
    ## FRED Economic Indicators Pipeline
    Pulls 60 economic series from the FRED API, lands them in GCS, loads into Snowflake,
    runs Great Expectations validation on the staging table, then builds the mart model
    with period-on-period change calculations. Runs on the 5th of every month at 7am.
    """
) as dag:

    # task 1: extract all series from FRED and load into snowflake raw table
    extract_and_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=run_load,
    )

    #task 2: run the staging model 
    dbt_staging = PythonOperator(
        task_id="dbt_staging",
        python_callable=run_dbt,
        op_kwargs={"command": "run", "select": "stg_fred_observations"},
    )

    # task 3: run dbt tests on the staging model before validation
    dbt_staging_tests = PythonOperator(
        task_id="dbt_staging_tests",
        python_callable=run_dbt,
        op_kwargs={"command": "test", "select": "stg_fred_observations"},
    )


    # task 4: run great expectations against the snowflake staging table
    # branches to dbt_mart if validation passes, validation_failed if it fails
    gx_validation = BranchPythonOperator(
        task_id="gx_validation",
        python_callable=run_validation,
    )

    # task 5a: validation passed. build the mart model
    dbt_mart = PythonOperator(
        task_id="dbt_mart",
        python_callable=run_dbt,
        op_kwargs={"command": "run", "select": "mart_economic_indicators"},
    )


    # task 5b: validation failed -- pipeline stops here
    validation_failed = EmptyOperator(
        task_id="validation_failed",
    )

    # task 6: run dbt tests on the mart model. only runs if validation succeeded
    dbt_mart_tests = PythonOperator(
        task_id="dbt_mart_tests",
        python_callable=run_dbt,
        op_kwargs={"command": "test", "select": "mart_economic_indicators"},
    )

    extract_and_load >> dbt_staging >> dbt_staging_tests >> gx_validation >> [dbt_mart, validation_failed]
    dbt_mart >> dbt_mart_tests