# Economic Indicators Tracker

A live data pipeline tracking 6 US macroeconomic indicators from the FRED API, transformed with dbt, and served through a publicly deployed Streamlit dashboard.

Live dashboard: [Economic Indicators Tracker](https://econ-indicators-tracker.streamlit.app/)

## Architecture
FRED API → Python → Snowflake (RAW) → dbt → Snowflake (MART) → Streamlit

## Indicators Tracked

| Indicator | FRED Series ID | Frequency | Unit |
|---|---|---|---|
| GDP | GDP | Quarterly | Billions USD |
| CPI | CPIAUCSL | Monthly | Index (1982-84 = 100) |
| Unemployment Rate | UNRATE | Monthly | Percent |
| Federal Funds Rate | FEDFUNDS | Monthly | Percent |
| Housing Starts | HOUST | Monthly | Thousands of Units |
| Retail Sales | RSXFS | Monthly | Millions USD |


## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, FRED API |
| Warehouse | Snowflake |
| Transformation | dbt |
| Dashboard | Streamlit, Plotly |
| Deployment | Streamlit Cloud |

## Project Structure
econ-indicators-tracker/
├── ingestion/
│   ├── extract.py              # pulls data from FRED API
│   └── load.py                 # loads raw data into Snowflake
├── dbt_project/
│   └── economic_indicators/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   ├── stg_fred_observations.sql
│       │   │   └── stg_fred_observations.yml
│       │   └── mart/
│       │       ├── mart_economic_indicators.sql
│       │       └── mart_economic_indicators.yml
│       └── macros/
│           └── generate_schema_name.sql
├── streamlit_app/
│   ├── app.py  # deployed dashboard
│   └── requirements.txt  # streamlit cloud dependencies
├── .env.example
├── requirements.txt  # for local development and running
└── README.md


## dbt Models

### Staging — `stg_fred_observations`
Cleans and standardizes raw FRED data. Casts types, filters null dates, and adds indicator context. Materialized as a view.

### Mart — `mart_economic_indicators`
Builds on staging to calculate month-on-month percentage change per indicator using a `LAG()` window function. MoM % change is calculated as `((current - previous) / previous) * 100`. This is the table the Streamlit dashboard queries directly. Materialized as a table.

## Setup

### Prerequisites
- Python 3.9+
- Snowflake account
- FRED API key - get one free at https://fred.stlouisfed.org/docs/api/api_key.html

### 1. Clone the repo

    git clone https://github.com/DataChaser/data-engineering-portfolio.git
    cd data-engineering-portfolio/econ-indicators-tracker

### 2. Create a virtual environment and install dependencies

    python -m venv venv
    venv\Scripts\activate
    pip install -r requirements.txt

### 3. Set up credentials

Copy `.env.example` to `.env` and fill in your Snowflake credentials and FRED API key.

### 4. Set up Snowflake

Run this in a Snowflake worksheet:

    CREATE DATABASE ECONOMIC_INDICATORS;
    CREATE SCHEMA ECONOMIC_INDICATORS.RAW;
    CREATE SCHEMA ECONOMIC_INDICATORS.STAGING;
    CREATE SCHEMA ECONOMIC_INDICATORS.MART;

### 5. Configure dbt

Add your Snowflake connection to ~/.dbt/profiles.yml:

    economic_indicators:
      target: dev
      outputs:
        dev:
          type: snowflake
          account: "your_account"
          user: "your_user"
          password: "your_password"
          role: "your_role"
          warehouse: your_warehouse
          database: ECONOMIC_INDICATORS
          schema: STAGING
          threads: 4

### 6. Run the ingestion pipeline

    cd ingestion
    python extract.py
    python load.py

### 7. Run dbt

    cd dbt_project/economic_indicators
    dbt build

### 8. Run the dashboard locally

    cd streamlit_app
    streamlit run app.py