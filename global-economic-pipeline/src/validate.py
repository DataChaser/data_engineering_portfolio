import os
import great_expectations as gx
from dotenv import load_dotenv
from utils import setup_logging

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

# Dictionary of tables to run the validation against
staging_tables = [
    {
        "suite": "worldbank_staging_suite",
        "table": "stg_worldbank",
        "source": "worldbank"
    },
    {
        "suite": "imf_staging_suite",
        "table": "stg_imf",
        "source": "imf"
    },
    {
        "suite": "undp_staging_suite",
        "table": "stg_undp",
        "source": "undp"
    }
]

# Function to run validation against one table
def validate_table(context, data_source, table_config):
    suite = table_config["suite"]
    table = table_config["table"]
    source = table_config["source"]

    logger.info(f"Building suite for {table}")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=suite))

    # Zero row-count check
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    # Required columns check
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=["country_code", "observation_year", "indicator_code",
                "indicator_value"],
                exact_match=False))

    logger.info(f"Suite built with {len(suite.expectations)} expectations")

    # Connect to the specific staging table
    table_asset = data_source.add_table_asset(
        name=table,
        table_name=table
    )
    batch_definition = table_asset.add_batch_definition_whole_table(
        f"{table}_batch"
    )
    batch = batch_definition.get_batch()

    # Run validation
    logger.info(f"Running validation against STAGING.{table.upper()}")
    validation_result = batch.validate(suite)

    # Log results per expectation
    results = validation_result.results
    total   = len(results)
    passed  = sum(1 for r in results if r.success)

    logger.info(f"Validation complete. {passed}/{total} expectations passed")

    for result in results:
        if result.success:
            logger.info(f"PASS: {result.expectation_config.type}")
        else:
            column = getattr(result.expectation_config, "column", "table-level")
            unexpected_pct = result.result.get("unexpected_percent", "N/A")
            logger.error(
                f"FAIL: {result.expectation_config.type} | column: {column} | unexpected %: {unexpected_pct}"
            )

    return validation_result.success

# Use above function to run validation against all the tables
def validate() -> bool:
    logger.info("Starting GX validation")

    context = gx.get_context()

    # Get Snowflake connections
    data_source = context.data_sources.add_snowflake(
        name="econ_snowflake",
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="STAGING",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

    all_passed = True

    for table_config in staging_tables:
        passed = validate_table(context, data_source, table_config)
        if not passed:
            all_passed = False
            logger.error(
                f"Validation FAILED for {table_config['table']}"
            )
        else:
            logger.info(
                f"Validation PASSED for {table_config['table']}"
            )

    if all_passed:
        logger.info("All staging tables passed. Safe to proceed.")
    else:
        logger.error("One or more staging tables failed.")

    return all_passed

if __name__ == "__main__":
    result = validate()
    print("PASSED" if result else "FAILED")