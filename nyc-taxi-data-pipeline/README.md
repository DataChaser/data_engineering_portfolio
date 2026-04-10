# NYC Yellow Taxi Pipeline

A production-grade batch data pipeline processing 12.2 million NYC Yellow Taxi trip records across three months (November 2025 - January 2026). The pipeline ingests raw Parquet files from the NYC TLC public dataset, validates data quality at the raw layer using Great Expectations, transforms the data through a layered dbt project, and surfaces three analytical mart tables in Snowflake.

## Stack

| Component | Detail |
|---|---|
| Data source | NYC TLC Yellow Taxi Trip Records (Public CloudFront CDN) |
| Volume | 12,211,339 rows across 3 months |
| Raw storage | Snowflake - TAXI_DB.RAW |
| Transformation | dbt - staging, intermediate, marts |
| Quality layer | Great Expectations 1.x at raw + dbt tests at transform |
| Orchestration | Airflow DAG (quality gate architecture) |
| Language | Python 3.13, SQL |

---

## Project structure
nyc-taxi-data-pipeline/
├── data/raw/  #create a folder for your use. 
├── exploration/
│   └── explore.py                    # Initial data exploration, download helpers
├── ingestion/
│   └── load.py                       # Raw load to Snowflake
├── expectations/
│   └── taxi_raw_suite.py             # Raw DQ checks using Great Expectations
├── utils/
│   └── snowflake_utils.py            # Snowflake connection
├── dbt_project/nyc_taxi_data_pipeline/
│   ├── models/
│   │   ├── staging/                  # stg_trips, stg_zone_lookup (views)
│   │   ├── intermediate/             # int_trips_joined (view)
│   │   └── mart/                    # mart_daily_revenue, mart_pickup_zones,
│   │                                 # mart_payment_summary (tables)
│   ├── tests/
│   │   └── assert_pickup_before_dropoff.sql
│   ├── macros/
│   │   └── generate_schema_name.sql  # For clean schema names
│   └── dbt_project.yml
├── dags/
│   └── taxi_pipeline_dag.py          # Airflow DA
├── .env.example                      # Credential template
├── requirements.txt
└── README.md

---

## Key Design Decisions

**Raw layer stores everything as VARCHAR.** The raw layer is an audit record of what arrived from the source. Casting at ingest means a schema change from the TLC would break the pipeline. VARCHAR never breaks at the boundary. Casting happens in dbt staging where it is documented, tested, and reversible.

**Two separate quality layers.** Great Expectations at the raw layer asks: is the source data good enough to process? dbt tests at the transform layer ask: is my transformation logic correct? These are different questions. Having both means a failure is attributable to the right layer - source degradation versus transformation bug.

**GX thresholds set from raw data exploration, not arbitrary 100%.** The NYC taxi dataset has known issues: historical timestamps from 2008, negative fares from refunds, 24% null passenger_count. Thresholds like `mostly=0.97` reflect reality. A 100% threshold on data with known quality issues creates false failures.

**LEFT JOIN in int_trips_joined.** An INNER JOIN silently drops trips whose location ID doesn't match the zone lookup. With 12M rows that data loss is hard to detect. LEFT JOIN preserves all trips and surfaces unresolved zone IDs as NULLs - visible and auditable.

**TriggerRule.ALL_DONE on the log task.** Ensures a pipeline run record is always written to PIPELINE_LOGS regardless of success or failure. Without this, a GX failure leaves no audit trail.

---

## Data Quality Findings

| Issue | Finding | Decision |
|---|---|---|
| Fare <= 0 | 9.5% of rows | Filtered in staging - structurally invalid |
| Distance <= 0 | 2.6% of rows | Filtered in staging |
| Timestamp violations | 1.5% pickup >= dropoff | GX threshold set to mostly=0.97 |
| RatecodeID = 99 | Valid per TLC data dictionary | Added to accepted_values |
| VendorID 6, 7 | Valid per March 2025 TLC update | Added to accepted_values |
| Historical timestamps | Records from 2008 in 2025 files | Known TLC issue - filtered in staging |
| payment_type = 0 | Flex Fare - documented by TLC | Added to accepted_values |

---

## Snowflake Structure

| Table / View | Description |
|---|---|
| RAW.RAW_TRIPS | 12M rows - all columns VARCHAR |
| RAW.RAW_ZONE_LOOKUP | 265 rows - borough and zone mappings |
| RAW.PIPELINE_LOGS | Audit log - pipeline run status per month |
| STAGING.STG_TRIPS | dbt view - typed, filtered, cleaned trips |
| STAGING.STG_ZONE_LOOKUP | dbt view - typed zone lookup |
| INTERMEDIATE.INT_TRIPS_JOINED | dbt view - trips enriched with zone names |
| MARTS.MART_DAILY_REVENUE | dbt table - daily revenue by borough |
| MARTS.MART_PICKUP_ZONES | dbt table - trip volume by zone and hour |
| MARTS.MART_PAYMENT_SUMMARY | dbt table - revenue and tip rate by payment type |

---

## How to Run Locally

**Prerequisites:** Python 3.10+, Snowflake account, dbt CLI

```bash
# 1. Clone and navigate
git clone https://github.com/your-username/data-engineering-portfolio.git
cd data-engineering-portfolio/nyc-taxi-data-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up credentials
cp .env.example .env
# Edit .env with your Snowflake credentials

# 4. Create Snowflake schemas (run in Snowflake worksheet)
# CREATE DATABASE TAXI_DB;
# CREATE SCHEMA TAXI_DB.RAW;
# CREATE SCHEMA TAXI_DB.STAGING;
# CREATE SCHEMA TAXI_DB.INTERMEDIATE;
# CREATE SCHEMA TAXI_DB.MARTS;

# 5. Load raw data
python -m ingestion.load

# 6. Run GX validation
python -m expectations.taxi_raw_suite

# 7. Run dbt
cd dbt_project/nyc_taxi_data_pipeline
dbt build --full-refresh
# Expected: 39 tests passing, 0 errors, 0 skips
```

---

## Known Limitations

**Airflow local execution.** The DAG was designed and coded in full - quality gate branching, XCom, TriggerRule.ALL_DONE, Snowflake audit log. Local execution was constrained by available RAM (3.8GB vs Docker Compose's 4GB requirement). The DAG runs without modification on a managed Airflow service or cloud VM.

**Hardcoded TARGET_MONTH.** The DAG processes a single hardcoded month. In production this would be derived from the Airflow execution date for true monthly automation.

**Full refresh materialisation.** dbt models use full refresh. Incremental models would be appropriate for the mart layer in production to avoid reprocessing historical data on every run.
