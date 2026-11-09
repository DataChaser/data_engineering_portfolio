import os
import logging
import great_expectations as gx
from great_expectations.expectations.row_conditions import Column
from dotenv import load_dotenv

# Logging setup
logging.basicConfig(
    # How each line looks: time | level | logger name | message
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    # Show INFO and above (INFO, WARNING, ERROR)
    level=logging.INFO,
)
logger = logging.getLogger("validate")

load_dotenv()
project = os.getenv("GCP_PROJECT_ID")
dataset = os.getenv("BQ_DATASET")
project_root = os.getenv("PROJECT_ROOT", os.getcwd())
credentials_path = os.path.join(project_root, os.getenv("GCP_CREDENTIALS_PATH"))

# Constants variables
silver_columns = [
    "event_ts", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session", "year_month",
]
event_types = ["view", "cart", "remove_from_cart", "purchase"]

year_months = ["2019-10", "2019-11", "2019-12", "2020-01", "2020-02", "2020-03", "2020-04"]

not_null_columns = [
    "event_ts", "event_type", "product_id", "user_id", "price", 
    "brand", "category_code",
]

row_count_min = 400_000_000
row_count_max = 420_000_000

# Validation
def validate() -> bool:
    logger.info("Starting GX validation of silver events table")

    context = gx.get_context()
    connection_string = f"bigquery://{project}/{dataset}?credentials_path={credentials_path}"
    data_source = context.data_sources.add_bigquery(
        name="clickstream_bigquery",
        connection_string=connection_string,
    )
    suite = context.suites.add_or_update(gx.ExpectationSuite(name="silver_events_suite"))

    # Row count checks
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=row_count_min, max_value=row_count_max
        )
    )

    # Column name match
    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=silver_columns, exact_match=True
        )
    )

    # Not null checks
    for column in not_null_columns:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

    # Accepted values for event_types, year_month (since data is only for 7 months)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="event_type", value_set=event_types)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="year_month", value_set=year_months)
    )

    # Price checks -- minimum and negative prices
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="price",
            min_value=0,
            strict_min=True,
            row_condition=Column("event_type") == "purchase",
        )
    )

    logger.info(f"Suite built with {len(suite.expectations)} expectations")

    # Pointing GX to the actual silver data
    table_asset = data_source.add_table_asset(
        name="silver_events", table_name="silver_events"
    )
    batch_definition = table_asset.add_batch_definition_whole_table("silver_events_batch")
    batch = batch_definition.get_batch()

    # Running the checks
    logger.info("Running validation against the Silver external table")
    validation_result = batch.validate(suite)

    results = validation_result.results
    passed = sum(1 for r in results if r.success)
    logger.info(f"Validation complete. {passed}/{len(results)} expectations passed")

    for result in results:
        name = f"{result.expectation_config.type} {result.expectation_config.kwargs.get('column', '')}"
        if result.success:
            logger.info(f"PASS: {name}")
        else:
            logger.error(f"FAIL: {name}. Details: {result.result}. Error: {str(result.exception_info)[:300]}")

    if validation_result.success:
        logger.info("Silver passed validation. Safe to proceed.")
    else:
        logger.error("Silver failed validation.")
    return validation_result.success


if __name__ == "__main__":
    result = validate()
    print("PASSED" if result else "FAILED")