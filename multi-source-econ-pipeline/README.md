# Multi-Source Economic Data Pipeline

A data pipeline that ingests macroeconomic data from two public APIs, loads it into Snowflake, and transforms it through a layered dbt project with incremental loading and data quality tests.

## Data Flow

FRED API + World Bank API (two separate sources) -> Python Ingestion -> Snowflake RAW -> dbt Staging -> dbt Intermediate -> dbt Mart

## Tech Stack

- **Python** - API ingestion scripts with incremental loading logic
- **Snowflake** - cloud data warehouse, hosts raw and transformed layers
- **dbt** - transformation layer with staging, intermediate, and mart models
- **FRED API** - US macroeconomic indicators (Federal Reserve)
- **World Bank API** - global economic indicators across 7 countries

## Data Sources

### FRED (Federal Reserve Economic Data)
- `GNPCA` - Real GNP (annual)
- `UNRATE` - US Unemployment Rate (monthly)
- `CPIAUCSL` - Consumer Price Index (monthly)

### World Bank
- `NY.GDP.MKTP.CD` - GDP in current USD
- `FP.CPI.TOTL.ZG` - Annual inflation rate
- `SL.UEM.TOTL.ZS` - Unemployment rate

Countries: USA, GBR, DEU, JPN, IND, BRA, CHN (US, UK, Germany, Japan, India, Brazil, China)

## Project Structure
```
econ-pipeline/
├── econ_pipeline/                  # dbt project
│   ├── models/
│   │   ├── staging/                # raw data
│   │   ├── intermediate/           # joins and enriches across sources
│   │   └── mart/                   # final analytical output
│   └── dbt_project.yml
├── ingestion/                      # Python ingestion scripts
│   ├── fred_ingest.py
│   ├── worldbank_ingest.py
│   └── utils.py
├── .env
├── requirements.txt
└── README.md

```

## dbt Layer Design

| Layer | Materialization | Purpose |
|---|---|---|
| Staging | View | Clean and type-cast raw source data |
| Intermediate | View | Join and pivot across sources |
| Mart | Table | Final analytical output, query-ready |

## Key Engineering Concepts

**Incremental Loading** - ingestion scripts check the latest date/year already loaded in Snowflake and only pulls new data from the API. Running the scripts multiple times never produces duplicates.

**dbt Layering** - separation between raw, staging, intermediate, and mart layers. Each layer has a single responsibility. Transformations are never done in ingestion scripts.

**Data Quality Tests** - 14 dbt tests across staging and mart layers covering null checks and accepted value validation. Tests run after every `dbt run` to catch data issues early.

## Setup

### Prerequisites
- Python 3.9+
- Snowflake account
- FRED API key (free at fred.stlouisfed.org)

### Installation
```bash
git clone https://github.com/yourusername/econ-pipeline.git
cd econ-pipeline
python -m venv your_virtual_environment_name
source venv/bin/activate  # If using Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in your project directory. Copy the below contents to `.env` file and fill in your credentials:
```
FRED_API_KEY=your_key_here
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

Configure your dbt profile at `~/.dbt/profiles.yml` - see `profiles.yml.example` for the expected structure.

### Snowflake Setup

Run this in a Snowflake worksheet before first use:
```sql
CREATE WAREHOUSE IF NOT EXISTS ECON_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS ECON_DB;
CREATE SCHEMA IF NOT EXISTS ECON_DB.RAW;
CREATE SCHEMA IF NOT EXISTS ECON_DB.DBT_DEV;
```

### Running the Pipeline
```bash
# Step 1 - ingest raw data
cd ingestion
python fred_ingest.py
python worldbank_ingest.py

# Step 2 - run dbt transformations
cd ../econ_pipeline
dbt run

# Step 3 - run data quality tests
dbt test
```

## Output

The final mart table `MART_GLOBAL_ECONOMIC_SNAPSHOT` contains one row per country per year from 2000 onwards with the following metrics:

- GDP in billions USD
- Inflation rate (%)
- Unemployment rate (%)
- US benchmark indicators from FRED for the same year
- Unemployment delta vs US

## Things to add next

- Orchestration with Apache Airflow to schedule daily runs
- dbt snapshots to track how indicators change over time
- Streamlit dashboard built on top of the mart table
- Extended country coverage and additional indicators