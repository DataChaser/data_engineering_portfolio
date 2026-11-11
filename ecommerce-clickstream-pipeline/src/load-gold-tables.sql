-- Loads the Gold Parquet files from the bucket into BigQuery tables.
-- OVERWRITE replaces the data on a rerun. Partitioning and clustering only apply when a table is created.

-- Fact table data load
LOAD DATA OVERWRITE `ecommerce-clickstream.clickstream.fact_events`
PARTITION BY DATE_TRUNC(event_date, MONTH)
CLUSTER BY event_type, product_id
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://ecommerce-clickstream-processed/gold/fact_events/*.parquet']
);

-- Product table data load
LOAD DATA OVERWRITE `ecommerce-clickstream.clickstream.dim_product`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://ecommerce-clickstream-processed/gold/dim_product/*.parquet']
);

-- Customer table data load
LOAD DATA OVERWRITE `ecommerce-clickstream.clickstream.dim_customer`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://ecommerce-clickstream-processed/gold/dim_customer/*.parquet']
);

-- Funnel results: 8 rows, one per month plus a total.
LOAD DATA OVERWRITE `ecommerce-clickstream.clickstream.funnel_by_month`
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://ecommerce-clickstream-processed/gold/funnel_by_month/*.parquet']
);