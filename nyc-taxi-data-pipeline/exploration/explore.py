import pandas as pd
import os
import requests
from dotenv import load_dotenv

load_dotenv()

data_dir = 'data/raw'
os.makedirs(data_dir, exist_ok=True)

months = ['2025-11', '2025-12', '2026-01']

base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
zone_lookup_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

def download_trip_data(year_month: str):
    filename = f"yellow_tripdata_{year_month}.parquet"
    filepath = os.path.join(data_dir, filename)
    print(filepath)

    if os.path.exists(filepath):
        print(f"Path: {filepath} exists. Skipping download")
        return filepath
    
    url = f"{base_url}/{filename}"
    print(f"Downloading {filename}")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded {filename}")
    return filepath

def download_zone_lookup():
    filepath = os.path.join(data_dir, 'taxi_zone_lookup.csv')

    if os.path.exists(filepath):
        print("File exists. Skipping download")
        return filepath

    print('Downloading taxi_zone_lookup.csv')
    response = requests.get(zone_lookup_url)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        f.write(response.content)

    print("Downloaded taxi_zone_lookup.csv")
    return filepath

def explore_trip_file(filepath: str):
    filename = os.path.basename(filepath)
    print(f"\n{'='*100}")
    print(f"Exploring {filename}")
    print(f"\n{'='*100}")

    df = pd.read_parquet(filepath)

    #Shape of data
    print(f"Dimension of data: {len(df):,} rows x {len(df.columns)} columns")

    #Column exploration
    print('\nExploring columns')
    for col in df.columns:
        null_pct = round((df[col].isna().sum() / len(df)) * 100, 2)
        print(f"{col:} data type is {str(df[col].dtype)}, null_percentage is {null_pct}%")

    #Timestamp checks
    print('Checking range of timestamp')
    print(f'Range of pickup timestamp is from {df['tpep_pickup_datetime'].min()} -> {df['tpep_pickup_datetime'].max()}')
    print(f'Range of dropoff timestamp is from {df['tpep_dropoff_datetime'].min()} -> {df['tpep_dropoff_datetime'].max()}')

    print('Checking pickup and dropoff times')
    impossible_times = df[df['tpep_pickup_datetime'] >= df['tpep_dropoff_datetime']]
    print(f"There are {len(impossible_times)} cases where pickup time is greater than or equal to time of dropff")

    print('Checking fares')
    zero_neg_fare = df[df['fare_amount'] <= 0]
    print(f"There are {len(zero_neg_fare)} cases where fares are zero or negative")

    print('Checking trip distance')
    zero_neg_dist = df[df['trip_distance'] <= 0]
    print(f"There are {len(zero_neg_dist)} cases where trip distance is zero or negative")

    print('Checking passenger counts')
    invalid_pass_count = df[(df['passenger_count'] <= 0) | (df['passenger_count'] > 6)]
    print(f"There are {len(invalid_pass_count)} cases where passenger counts are zero or negative or greater than 6")

    #Categorical data distributions
    print('\nChecking categorical data disctribution')
    print(f'Payment types: {df['payment_type'].unique()}')
    print(df['payment_type'].value_counts().to_string(header=False))

    print('RatecodeID:')
    print(df['RatecodeID'].value_counts().to_string(header=False))

    print('store_and_fwd_flag:')
    print(df['store_and_fwd_flag'].value_counts().to_string(header=False))

    #Referential integrity checks
    zone_path = os.path.join(data_dir, "taxi_zone_lookup.csv")
    if os.path.exists(zone_path):
        zones = pd.read_csv(zone_path)
        valid_ids = set(zones['LocationID'].values)

        pu_invalid = df[~df['PULocationID'].isin(valid_ids)]
        do_invalid = df[~df['DOLocationID'].isin(valid_ids)]

        print('Zone ID referential integrity:')
        print(f"PULocationID not in zone lookup: {len(pu_invalid):,}")
        print(f"DOLocationID not in zone lookup: {len(do_invalid):,}")

    #Numerical distribution
    print('\n Checking fare amount distribution')
    print(df['fare_amount'].describe().to_string())

    print('\n Checking trip distance distribution')
    print(df['trip_distance'].describe().to_string())

def explore_zone_lookup(filepath: str):

    print(f"\n{'='*100}")
    print(f"Exploring taxi_zone_lookup.csv")
    print(f'\n{'='*100}')
    
    
    df = pd.read_csv(filepath)

    print(f"\nLookup table has {len(df):,} rows and {len(df.columns)} columns")

    print("\nSample rows")
    print(df.head(10))

    print('Checking Borough distribution')
    print(df['Borough'].value_counts().to_string())

    print(f"\n LocationID range: {df['LocationID'].min()} -> {df['LocationID'].max()}")
    print(f"Unique LocationIDs: {df['LocationID'].nunique()}")
    print(f"Any nulls: {df.isna().any().any()}")

if __name__ == '__main__':
    print('\nDownloading files')
    trip_files = []
    for month in months:
        filepath = download_trip_data(month)
        trip_files.append(filepath)
    zone_filepath = download_zone_lookup()

    print("\nExploring trip data")
    explore_trip_file(trip_files[0])  #checking only the first downloaded month

    print("\nExploring zone lookup")
    explore_zone_lookup(zone_filepath)

    #row count check across all 3 files
    print('Checking row counts across the 3 files')
    for filepath in trip_files:
        df = pd.read_parquet(filepath, columns=['VendorID'])
        print(f'There are {len(df):,} rows in {os.path.basename(filepath)}')






