import pyarrow.parquet as pq
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import os
from utils.snowflake_utils import get_connection

data_dir = 'data/raw'

months = ['2025-11', '2025-12', '2026-01']

def load_trip_file(conn, year_month: str):
    filename = f"yellow_tripdata_{year_month}.parquet"
    filepath = os.path.join(data_dir, filename)

    print(f'Loading {filename}')

    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'RAW_TRIPS'
    """)
    table_exists = cursor.fetchone()[0] > 0

    if table_exists:
        cursor.execute(f"""
            SELECT COUNT(*) FROM TAXI_DB.RAW.RAW_TRIPS
            WHERE SOURCE_MONTH = '{year_month}'
        """)
        already_loaded = cursor.fetchone()[0]
        if already_loaded > 0:
            print(f'Data already loaded for {year_month}. Skipping.')
            cursor.close()
            return

    cursor.close()

    parquet_file = pq.ParquetFile(filepath)
    total_row_groups = parquet_file.metadata.num_row_groups
    print(f"Total row groups: {total_row_groups}")

    for i in range(total_row_groups):
        chunk = parquet_file.read_row_group(i).to_pandas()

        for col in chunk.columns:
            chunk[col] = chunk[col].astype(str)
        chunk = chunk.replace({'NaT': None, 'nan': None})
        chunk['SOURCE_MONTH'] = year_month
        chunk.columns = [col.upper() for col in chunk.columns]

        print(f"Loading row group {i+1}/{total_row_groups} - {len(chunk):,} rows")

        success, _, nrows, _ = write_pandas(
            conn=conn,
            df=chunk,
            table_name='RAW_TRIPS',
            database='TAXI_DB',
            schema='RAW',
            auto_create_table=True,
            overwrite=False
        )

        if not success:
            raise Exception(f"Row group {i+1} failed to load for {year_month}")

        print(f"Row group {i+1} loaded - {nrows:,} rows")

    print(f"Load complete for {year_month}")


def load_zone_lookup(conn):
    filepath = os.path.join(data_dir, 'taxi_zone_lookup.csv')

    df = pd.read_csv(filepath, dtype=str)
    df = df.replace({'nan': None})
    df.columns = [col.upper() for col in df.columns]

    print('Loading taxi_zone_lookup.csv')

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name='RAW_ZONE_LOOKUP',
        database='TAXI_DB',
        schema='RAW',
        auto_create_table=True,
        overwrite=True
    )

    if success:
        print(f'Data load successful. Loaded {nrows} rows in {nchunks} chunks')
    else:
        print('Data load failed.')


def verify_load(conn):
    cursor = conn.cursor()

    print('Checking row count of the raw trips data')
    cursor.execute("SELECT COUNT(*) FROM TAXI_DB.RAW.RAW_TRIPS")
    rows = cursor.fetchone()[0]
    print(f"Total rows is {rows}")

    print('Checking rows per month of data')
    cursor.execute("""
        SELECT SOURCE_MONTH, COUNT(*)
        FROM TAXI_DB.RAW.RAW_TRIPS
        GROUP BY SOURCE_MONTH
        ORDER BY SOURCE_MONTH
    """)
    print('\n Rows by source month:')
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]}")

    print('Checking zone lookup table')
    cursor.execute("SELECT COUNT(*) FROM TAXI_DB.RAW.RAW_ZONE_LOOKUP")
    lookup_rows = cursor.fetchone()[0]
    print(f'Lookup table has {lookup_rows} rows. (expected: 265)')

    print('Spot check -- confirm the data looks fine and SOURCE_MONTH is present')
    cursor.execute("""
        SELECT TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME,
               FARE_AMOUNT, TRIP_DISTANCE, SOURCE_MONTH
        FROM TAXI_DB.RAW.RAW_TRIPS
        LIMIT 5
    """)
    print("Sample rows from RAW_TRIPS:")
    for row in cursor.fetchall():
        print(row)

    cursor.close()


if __name__ == "__main__":
    conn = get_connection()
    print('Connection established')

    for month in months:
        load_trip_file(conn, month)

    load_zone_lookup(conn)
    verify_load(conn)

    conn.close()
    print('Process has been completed and connection is closed')