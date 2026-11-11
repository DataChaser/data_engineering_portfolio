-- External table over the Silver Parquet files; the data stays in the bucket.
CREATE OR REPLACE EXTERNAL TABLE `ecommerce-clickstream.clickstream.silver_events`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ecommerce-clickstream-processed/silver/events/*.parquet']
);