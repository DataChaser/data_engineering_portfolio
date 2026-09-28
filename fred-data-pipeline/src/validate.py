import os
import great_expectations as gx
from dotenv import load_dotenv
from utils import setup_logging

import logging
setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

# Run validation against the staging table and return pass/fail
def validate() -> bool:
    logger.info("starting GX validation on staging table")
    
    #Creating data context, suite, add expectations/quality checks
    context = gx.get_context()
    suite = context.suites.add_or_update(gx.ExpectationSuite(name="fred_staging_suite"))

    #Row and column check
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))
    suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=["observation_date", "indicator_name", "series_id","frequency", "value"], exact_match=False))

    #Not null checks in date and series_id
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="observation_date"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="series_id"))

    # Accepted values check on frequency column
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="frequency",
        value_set=["Daily", "Weekly", "Monthly", "Quarterly", "Annual"]))

    logger.info(f"suite built with {len(suite.expectations)} expectations")

    # Connect GX to Snowflake. Adding data source via snowflake, pointing to the specific table
    data_source = context.data_sources.add_snowflake(
        name="fred_snowflake",
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="FRED_DB",
        schema="STAGING",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"))
        
    table_asset = data_source.add_table_asset(name="stg_fred_observations", table_name="stg_fred_observations")

    batch_definition = table_asset.add_batch_definition_whole_table("staging_batch")

    batch = batch_definition.get_batch()

    # Running the validation
    logger.info("Running validation against STAGING.STG_FRED_OBSERVATIONS")
    validation_result = batch.validate(suite)

    # Results
    results = validation_result.results
    total   = len(results)
    passed  = sum(1 for r in results if r.success)

    logger.info(f"Validation complete.{passed}/{total} expectations passed")

    for result in results:
        if result.success:
            logger.info(f" PASS: {result.expectation_config.type}")
        else:
            column = getattr(result.expectation_config, "column", "table-level")
            unexpected_pct = result.result.get("unexpected_percent", "N/A")
            logger.error(f" FAIL: {result.expectation_config.type} | column: {column} | unexpected %: {unexpected_pct}")

    if validation_result.success:
        logger.info("Validation passed. Safe to build mart model")
    else:
        logger.error("Validation failed. Mart model will not be built")

    return validation_result.success

if __name__ == "__main__":
    result = validate()
    print("PASSED" if result else "FAILED")