import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

PCT_INDICATORS = ["Unemployment Rate", "Federal Funds Rate"]
RAW_INDICATORS = ["GDP (Billions USD)", "CPI (Index)", "Housing Starts (000s)", "Retail Sales (Millions USD)"]


def get_secret(key: str):
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key)


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=get_secret("SNOWFLAKE_ACCOUNT"),
        user=get_secret("SNOWFLAKE_USER"),
        password=get_secret("SNOWFLAKE_PASSWORD"),
        warehouse="ECON_WH",
        database="ECONOMIC_INDICATORS",
        schema="MART",
        role=get_secret("SNOWFLAKE_ROLE"),
    )


def format_value(indicator: str, value: float) -> str:
    if pd.isna(value):
        return "N/A"
    if indicator in PCT_INDICATORS:
        return f"{value:.2f}%"
    return f"{value:,.2f}"


def get_delta_color(indicator: str) -> str:
    inverse = ["CPI (Index)", "Unemployment Rate"]
    neutral = ["Federal Funds Rate"]
    if indicator in inverse:
        return "inverse"
    elif indicator in neutral:
        return "off"
    return "normal"


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    conn = get_snowflake_connection()
    query = """
        SELECT
            OBSERVATION_DATE,
            INDICATOR_NAME,
            SERIES_ID,
            VALUE,
            PREV_VALUE,
            MOM_PCT_CHANGE,
            DBT_UPDATED_AT
        FROM MART.MART_ECONOMIC_INDICATORS
        ORDER BY INDICATOR_NAME, OBSERVATION_DATE
    """
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [col.lower() for col in df.columns]
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    return df


def get_latest(df: pd.DataFrame):
    return (
        df
        .sort_values("observation_date")
        .groupby("indicator_name")
        .last()
        .reset_index()
    )


#  PAGE CONFIG 
st.set_page_config(page_title="Economic Indicators Tracker", layout="wide")

#  LOAD DATA 
df = load_data()
latest = get_latest(df)

#  HEADER 
st.title("Economic Indicators Tracker")
st.caption("US macroeconomic indicators sourced from FRED -- updated monthly")

st.divider()

#  METRIC CARDS 
indicators = latest["indicator_name"].tolist()
cols = st.columns(len(indicators))

for col, (_, row) in zip(cols, latest.iterrows()):
    col.metric(
        label=row["indicator_name"],
        value=format_value(row["indicator_name"], row["value"]),
        delta=f"{row['mom_pct_change']:.2f}%" if pd.notna(row["mom_pct_change"]) else "N/A",
        delta_color=get_delta_color(row["indicator_name"]),
    )

st.divider()

#  SIDEBAR 
st.sidebar.header("Controls")
selected = st.sidebar.selectbox("Select Indicator", indicators)
years = st.sidebar.slider("Years of history", min_value=1, max_value=24, value=10)

#  CHART 
chart_df = df[df["indicator_name"] == selected].copy()
cutoff = pd.Timestamp.today() - pd.DateOffset(years=years)
chart_df = chart_df[chart_df["observation_date"] >= pd.Timestamp(cutoff)]

if selected in PCT_INDICATORS:
    y_label = f"{selected} (%)"
else:
    y_label = selected

st.subheader(f"{selected} -- Last {years} Years")

fig = px.line(
    chart_df,
    x="observation_date",
    y="value",
    labels={
        "observation_date": "Date",
        "value": y_label
    }
)

fig.update_xaxes(
    dtick="M12",
    tickformat="%Y",
    tickangle=0,
)

fig.update_layout(
    hovermode="x unified",
    yaxis_title=y_label,
    xaxis_title="",
)

st.plotly_chart(fig, use_container_width=True)