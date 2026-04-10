import great_expectations as gx
import pandas as pd
from utils.snowflake_utils import get_connection

sample_size = 500000

target_month = '2026-01'

def get_raw_trips_sample(month, sample_size):
    print('Connecting to Snowflake')
    conn = get_connection()

    query = f'''
        SELECT * FROM TAXI_DB.RAW.RAW_TRIPS TABLESAMPLE ({sample_size} ROWS)
        WHERE SOURCE_MONTH = '{month}'
        '''
    
    print(f'Pulling in sample data for {month}')
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    cursor.close()
    
    numeric_columns = ['FARE_AMOUNT', 'TRIP_DISTANCE', 'TOTAL_AMOUNT']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    datetime_columns = ['TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME']
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    conn.close()

    print(f'Sample data has been pulled. {len(df)} rows x {len(df.columns)} columns')
    return df

def build_expectations_suite(context):
    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name='taxi_raw_suite')
    )

    #Null checks
    not_null_columns = [
        'VENDORID',
        'TPEP_PICKUP_DATETIME',
        'TPEP_DROPOFF_DATETIME',
        'TRIP_DISTANCE',
        'PULOCATIONID',
        'DOLOCATIONID',
        'PAYMENT_TYPE',
        'FARE_AMOUNT',
        'TOTAL_AMOUNT',
        'SOURCE_MONTH'
    ]

    for col in not_null_columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column = col)
        )

    #Time stamp validity
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A='TPEP_DROPOFF_DATETIME',
            column_B='TPEP_PICKUP_DATETIME',
            mostly=0.97
        )
    )

    #Fare Validity
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column='FARE_AMOUNT',
            min_value=0,
            mostly=0.88
        )
    )

    #Trip distance Validity
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column='TRIP_DISTANCE',
            min_value=0,
            mostly=0.95
        )
    )

    #RatecodeID check
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column = 'RATECODEID',
            value_set = ['1.0', '2.0', '3.0', '4.0', '5.0', '6.0', '99.0'],
            mostly = 0.97
        )
    )

    #Row count check
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value = 200000,
            max_value = 600000
        )
    )

    expected_columns = [
        'VENDORID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME',
        'PASSENGER_COUNT', 'TRIP_DISTANCE', 'RATECODEID',
        'STORE_AND_FWD_FLAG', 'PULOCATIONID', 'DOLOCATIONID',
        'PAYMENT_TYPE', 'FARE_AMOUNT', 'EXTRA', 'MTA_TAX',
        'TIP_AMOUNT', 'TOLLS_AMOUNT', 'IMPROVEMENT_SURCHARGE',
        'TOTAL_AMOUNT', 'CONGESTION_SURCHARGE', 'AIRPORT_FEE',
        'CBD_CONGESTION_FEE', 'SOURCE_MONTH'
    ]

    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set = expected_columns,
            exact_match=True
            # exact_match=True means no extra or missing columns allowed
        )
    )

    print(f'Expectation suite built with {len(suite.expectations)} expectations')
    return suite

#### Validation ######

def run_validation(month):
    df = get_raw_trips_sample(month, sample_size)

    context = gx.get_context()
    suite = build_expectations_suite(context)
    data_source = context.data_sources.add_pandas('taxi_raw_data')
    data_asset = data_source.add_dataframe_asset('raw_trips_sample')
    batch_definition = data_asset.add_batch_definition_whole_dataframe('raw_trips_batch')
    batch = batch_definition.get_batch(
        batch_parameters = {'dataframe': df}
    )

    print(f'Running validation for month: {month}')

    validation_result = batch.validate(suite)

    print_validation_summary(validation_result, month)

    return validation_result.success

def print_validation_summary(validation_result, month):
    results = validation_result.results
    total = len(results)
    passed = sum(1 for r in results if r.success)
    failed = total - passed

    print(f'\nValidation Results: {month}')
    print(f'Total: {total} | Passed: {passed} | Failed: {failed}')
    print(f"Overall: {'PASS' if validation_result.success else 'FAIL'}")

    if failed > 0:
        print('Data validation failed')
        for result in results:
            if not result.success:
                print(f'{result.expectation_config.type}')
                print(f'{result.result}')

    else:
        print('Data validation passed')
        for result in results:
            if result.success:
                print(f'{result.expectation_config.type}')

if __name__ == "__main__":
    result = run_validation(target_month)
    if result:
        print("Validation passed. Safe to proceed with dbt")
    else:
        print("Validation failed. Investigate before running dbt")

    




