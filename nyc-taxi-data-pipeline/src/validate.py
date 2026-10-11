import os
import great_expectations as gx
from dotenv import load_dotenv
from utils import setup_logging

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()


def validate(year_month: str) -> bool:
    logger.info(f"Starting GX validation for {year_month}")

    context = gx.get_context()

    project = os.getenv("GCP_PROJECT_ID")
    project_root = os.getenv("PROJECT_ROOT", os.getcwd())
    credentials_path = os.path.join(
        project_root, os.getenv("GCP_CREDENTIALS_PATH")
    )

    connection_string = f"bigquery://{project}/staging?credentials_path={credentials_path}"

    data_source = context.data_sources.add_bigquery(
    name="nyc_taxi_bigquery",
    connection_string=connection_string
    )

    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name=f"stg_trips_suite_{year_month}")
    )

    # Row count check
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=3_000_000,
            max_value=7_000_000
        )
    )

    # Columns check
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "vendor_id", "source_month", "pickup_datetime",
                "dropoff_datetime", "pickup_date", "trip_duration_minutes",
                "passenger_count", "trip_distance", "pickup_location_id",
                "dropoff_location_id", "payment_type", "fare_amount",
                "tip_amount", "total_amount", "cbd_congestion_fee"
            ],
            exact_match=False
        )
    )

    # fare_amount check 
    suite.add_expectation(
        gx.expectations.ExpectColumnMedianToBeBetween(
            column="fare_amount",
            min_value=5.0,
            max_value=50.0
        )
    )

    logger.info(f"Suite built with {len(suite.expectations)} expectations")

    # Filter batch to current month only using a SQL query
    # This ensures all checks run against only the month being processed
    query_asset = data_source.add_query_asset(
        name=f"stg_trips_{year_month}",
        query=f"""
            SELECT *
            FROM `{os.getenv('GCP_PROJECT_ID')}.staging.stg_trips`
            WHERE source_month = '{year_month}'
        """
    )
    batch_definition = query_asset.add_batch_definition_whole_table(
        f"stg_trips_{year_month}_batch"
    )
    batch = batch_definition.get_batch()

    logger.info(f"Running validation against staging.stg_trips for {year_month}")
    validation_result = batch.validate(suite)

    results = validation_result.results
    total  = len(results)
    passed = sum(1 for r in results if r.success)

    logger.info(f"Validation complete. {passed}/{total} expectations passed")

    for result in results:
        if result.success:
            logger.info(f"PASS: {result.expectation_config.type}")
        else:
            logger.error(
                f"FAIL: {result.expectation_config.type}. Details: {result.result}"
            )

    if validation_result.success:
        logger.info(f"{year_month} passed validation. Safe to proceed.")
    else:
        logger.error(f"{year_month} failed validation. dbt will not run.")

    return validation_result.success


if __name__ == "__main__":
    import sys
    # Accept year_month as command line argument for manual runs.
    # When called from Airflow, year_month is passed directly to validate().
    year_month = sys.argv[1] if len(sys.argv) > 1 else "2026-08"
    result = validate(year_month)
    print("PASSED" if result else "FAILED")