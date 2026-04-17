import pyarrow.parquet as pq
import sys
import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# This is where docker-compose.override.yml mounts your project.
# All imports from ingestion/, expectations/ etc. work because of this.
PROJECT_ROOT = '/opt/airflow/project'
sys.path.insert(0, PROJECT_ROOT)

# Which month this pipeline run processes.
# Change this when you want to process a different month.
TARGET_MONTH = "2026-01"

# Where dbt project lives inside the container.
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, 'dbt_project', 'nyc_taxi_data_pipeline')

# dbt profiles.yml needs to know where to find credentials.
# By default dbt looks in ~/.dbt/ — we point it to our project folder instead.
DBT_PROFILES_DIR = os.path.join(PROJECT_ROOT, 'dbt_project', 'nyc_taxi_data_pipeline')


def task_download_data(**context):
    """Download raw parquet file and zone lookup for TARGET_MONTH."""
    from exploration.explore import download_trip_data, download_zone_lookup
    print(f"Downloading data for {TARGET_MONTH}")
    download_trip_data(TARGET_MONTH)
    download_zone_lookup()
    print("Download complete")


def task_load_raw(**context):
    """Load downloaded parquet into Snowflake RAW_TRIPS. Skips if already loaded."""
    from ingestion.load import load_trip_file, load_zone_lookup
    from utils.snowflake_utils import get_connection
    print(f"Loading raw data for {TARGET_MONTH}")
    conn = get_connection()
    load_trip_file(conn, TARGET_MONTH)
    load_zone_lookup(conn)
    conn.close()
    print("Raw load complete")


def task_run_gx(**context):
    """Run Great Expectations validation on raw data. Returns True/False."""
    from expectations.taxi_raw_suite import run_validation
    print(f"Running GX validation for {TARGET_MONTH}")
    passed = run_validation(TARGET_MONTH)
    print(f"GX result: {'PASS' if passed else 'FAIL'}")
    # Return value is stored in XCom automatically — branch task reads it next
    return passed


def task_branch_on_gx(**context):
    """
    Read GX result from XCom and decide which path to take.
    If GX passed -> run dbt.
    If GX failed -> log failure and skip dbt.
    """
    gx_passed = context['ti'].xcom_pull(task_ids='run_gx_validation')
    print(f"GX passed: {gx_passed}")
    if gx_passed:
        return 'run_dbt'
    else:
        return 'log_failure'


def task_run_dbt(**context):
    """Run dbt build inside the container using subprocess."""
    print("Running dbt build...")
    result = subprocess.run(
        [
            'dbt', 'build',
            '--project-dir', DBT_PROJECT_DIR,
            '--profiles-dir', DBT_PROFILES_DIR,
            '--no-write-json',          # don't write run artifacts to disk
            '--no-partial-parse',
            '--target-path', '/tmp/dbt_target',
            '--log-path', '/tmp/dbt_logs'  # write logs to /tmp which is writable
        ],
        capture_output=True,
        text=True
    )
    # Always print stdout so logs are visible in Airflow UI
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt build failed:\n{result.stderr}")
    print("dbt build complete")
    return True


def task_log_failure(**context):
    """Log GX validation failure to Snowflake PIPELINE_LOGS table."""
    from utils.snowflake_utils import get_connection
    print("GX failed — logging to Snowflake")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO TAXI_DB.RAW.PIPELINE_LOGS
            (run_id, month_processed, status, task_name, message)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        context['run_id'],
        TARGET_MONTH,
        'FAILED',
        'run_gx_validation',
        f'GX validation failed for {TARGET_MONTH}. dbt not run.'
    ))
    cursor.close()
    conn.close()
    print("Failure logged")


def task_log_result(**context):
    """Log final pipeline status to Snowflake PIPELINE_LOGS table."""
    from utils.snowflake_utils import get_connection
    # XCom pull from run_dbt — will be None if dbt was skipped due to GX failure
    dbt_result = context['ti'].xcom_pull(task_ids='run_dbt')
    status = 'SUCCESS' if dbt_result else 'FAILED'
    print(f"Logging final status: {status}")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO TAXI_DB.RAW.PIPELINE_LOGS
            (run_id, month_processed, status, task_name, message)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        context['run_id'],
        TARGET_MONTH,
        status,
        'pipeline_complete',
        f'Pipeline complete for {TARGET_MONTH}. Status: {status}'
    ))
    cursor.close()
    conn.close()
    print(f"Logged: {status}")


# DAG definition
with DAG(
    dag_id='taxi_pipeline',
    schedule='0 0 1 * *',       # runs at midnight on the 1st of every month
    start_date=datetime(2025, 11, 1),
    catchup=False,               # don't backfill missed runs
    tags=['taxi', 'monthly', 'pipeline'],
    description='Monthly NYC yellow taxi pipeline'
) as dag:

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=task_download_data,
    )

    load_raw = PythonOperator(
        task_id='load_raw',
        python_callable=task_load_raw,
    )

    run_gx_validation = PythonOperator(
        task_id='run_gx_validation',
        python_callable=task_run_gx,
    )

    branch_on_gx = BranchPythonOperator(
        task_id='branch_on_gx',
        python_callable=task_branch_on_gx,
    )

    run_dbt = PythonOperator(
        task_id='run_dbt',
        python_callable=task_run_dbt,
    )

    log_failure = PythonOperator(
        task_id='log_failure',
        python_callable=task_log_failure,
    )

    log_result = PythonOperator(
        task_id='log_result',
        python_callable=task_log_result,
        # ALL_DONE means this runs regardless of whether run_dbt or log_failure ran
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Pipeline flow:
    # download -> load -> validate -> branch
    # branch -> dbt -> log_result      (happy path)
    # branch -> log_failure -> log_result  (GX failed path)
    download_data >> load_raw >> run_gx_validation >> branch_on_gx
    branch_on_gx >> run_dbt >> log_result
    branch_on_gx >> log_failure >> log_result