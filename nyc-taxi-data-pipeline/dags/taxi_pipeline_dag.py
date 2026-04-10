import sys
import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = '/opt/airflow/project'
sys.path.insert(0, PROJECT_ROOT)

from ingestion.load import load_trip_file, load_zone_lookup, get_connection
from expectations.taxi_raw_suite import run_validation

TARGET_MONTH = "2026-01"
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, 'dbt_project', 'nyc_taxi_data_pipeline')


def task_download_data(**context):
    from exploration.explore import download_trip_data, download_zone_lookup
    print(f"Downloading data for month: {TARGET_MONTH}")
    trip_filepath = download_trip_data(TARGET_MONTH)
    zone_filepath = download_zone_lookup()
    print(f"Trip file: {trip_filepath}")
    print(f"Zone file: {zone_filepath}")
    return f"Downloaded {TARGET_MONTH}"


def task_load_raw(**context):
    print(f"Loading raw data for month: {TARGET_MONTH}")
    conn = get_connection()
    load_trip_file(conn, TARGET_MONTH)
    load_zone_lookup(conn)
    conn.close()
    print("Raw load complete")
    return f"Loaded {TARGET_MONTH}"


def task_run_gx(**context):
    print(f"Running GX validation for month: {TARGET_MONTH}")
    passed = run_validation(TARGET_MONTH)
    print(f"GX validation result: {'PASS' if passed else 'FAIL'}")
    return passed


def task_branch_on_gx(**context):
    gx_passed = context['ti'].xcom_pull(task_ids='run_gx_validation')
    print(f"GX result: {gx_passed}")
    if gx_passed:
        return 'run_dbt'
    else:
        return 'log_failure'


def task_run_dbt(**context):
    print("Running dbt build...")
    result = subprocess.run(
        ['dbt', 'build', '--project-dir', DBT_PROJECT_DIR],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt build failed with return code {result.returncode}")
    print("dbt build completed successfully")
    return True


def task_log_failure(**context):
    print("GX validation failed — logging to Snowflake")
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


def task_log_success(**context):
    print("Logging pipeline result to Snowflake")
    dbt_result = context['ti'].xcom_pull(task_ids='run_dbt')
    status = 'SUCCESS' if dbt_result else 'FAILED'
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


with DAG(
    dag_id='taxi_pipeline',
    schedule='0 0 1 * *',
    start_date=datetime(2025, 11, 1),
    catchup=False,
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
        python_callable=task_log_success,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    download_data >> load_raw >> run_gx_validation >> branch_on_gx
    branch_on_gx >> run_dbt >> log_result
    branch_on_gx >> log_failure >> log_result