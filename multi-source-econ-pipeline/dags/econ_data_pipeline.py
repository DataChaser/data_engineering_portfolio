# Orchestrates the full multi-source economic data pipeline.
# Runs twice a year --  1st April and 1st October at 7am

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from dbt.cli.main import dbtRunner

# Add project root and src/ to Python path so tasks can import from src/
project_root = "/usr/local/airflow/project"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

# dbt project path and writable output paths
dbt_project_dir = os.path.join(project_root, "dbt_project/econ_data_pipeline")
dbt_log_path    = "/tmp/dbt_logs"
dbt_target_path = "/tmp/dbt_target"

# Default args applied to all tasks
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Defining the tasks

# Fetching all indicators from their sources and writes JSON files to GCS.
def run_worldbank_extract():
    from worldbank_extract import run
    run()

def run_imf_extract():
    from imf_extract import run
    run()

def run_undp_extract():
    from undp_extract import run
    run()

# Get the data into Snowflake raw table
def run_load():
    from load import run
    run()

# dbt run
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

# Running validation
def run_validation():
    from validate import validate
    passed = validate()
    return "dbt_mart" if passed else "validation_failed"

# Defining the DAG
with DAG(
    dag_id="econ__datapipeline",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="0 7 1 4,10 *",
    catchup=False,
    tags=["economic_data", "worldbank", "imf", "undp"],
    doc_md="""
    ## Multi-Source Economic Data Pipeline
    Fetches 18 economic and human development indicators from World Bank, IMF,
    and UNDP for 30 countries across G7, BRICS, ASEAN, Mercosur, USMCA, and CPTPP.
    Runs twice a year on 1st April and 1st October at 7am, aligned with the
    IMF World Economic Outlook release schedule.
    """
) as dag:

    # Extract World Bank data and write to GCS
    worldbank_extract = PythonOperator(
        task_id="worldbank_extract",
        python_callable=run_worldbank_extract,
    )

    # Extract IMF data and write to GCS
    imf_extract = PythonOperator(task_id="imf_extract", python_callable=run_imf_extract,)

    # Extract UNDP data and write to GCS
    undp_extract = PythonOperator(task_id="undp_extract", python_callable=run_undp_extract,)

    # Load all data to Snowflake raw table
    load = PythonOperator( task_id="load", python_callable=run_load,)

    # dbt staging model
    dbt_staging = PythonOperator(
        task_id="dbt_staging",
        python_callable=run_dbt,
        op_kwargs={"command": "run", "select": "staging"},
    )

    # Rrun dbt tests on staging models
    dbt_staging_tests = PythonOperator(
        task_id="dbt_staging_tests",
        python_callable=run_dbt,
        op_kwargs={"command": "test", "select": "staging"},
    )

    # Great Expectations validation checks
    gx_validation = BranchPythonOperator(
        task_id="gx_validation",
        python_callable=run_validation,
    )

    # dbt mart model build if GX validation passes
    dbt_mart = PythonOperator(
        task_id="dbt_mart",
        python_callable=run_dbt,
        op_kwargs={"command": "run", "select": "intermediate mart_economic_snapshot"},
    )

    # Pipeline stops if validation fails
    validation_failed = EmptyOperator(task_id="validation_failed",)

    # dbt test on mart
    dbt_mart_tests = PythonOperator(
        task_id="dbt_mart_tests",
        python_callable=run_dbt,
        op_kwargs={"command": "test", "select": "mart_economic_snapshot"},
    )

    # Task dependencies
    # Three extracts run in parallel, all must succeed before load starts
    [worldbank_extract, imf_extract, undp_extract] >> load
    load >> dbt_staging >> dbt_staging_tests >> gx_validation
    gx_validation >> [dbt_mart, validation_failed]
    dbt_mart >> dbt_mart_tests