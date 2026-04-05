import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        warehouse="ECON_WH",
        database="ECONOMIC_INDICATORS",
        schema="MART",
        role=st.secrets["SNOWFLAKE_ROLE"],
    )

@st.cache_data(ttl=1800)
def load_data():
    conn = get_snowflake_connection()
    query = """
        SELECT
            OBSERVATION_DATE,
            INDICATOR_NAME,
            SERIES_ID,
            VALUE,
            PREV_VALUE,
            MOM_CHANGE,
            DBT_UPDATED_AT
        FROM MART.MART_ECONOMIC_INDICATORS
        ORDER BY INDICATOR_NAME, OBSERVATION_DATE
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # lowercase column names for easier access in the rest of the app
    df.columns = [col.lower() for col in df.columns]
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    return df


def get_latest(df: pd.DataFrame):
    return df.sort_values("observation_date").groupby("indicator_name").last().reset_index()


def get_delta_color(indicator: str):
    """
    Returns the correct delta_color for st.metric based on the indicator.
    For unemployment and inflation, a positive change is bad so we invert the color.
    """
    inverse = ["Inflation", "Unemployment"]
    neutral = ["Interest Rate"]

    if indicator in inverse:
        return "inverse"
    elif indicator in neutral:
        return "off"
    return "normal"


# PAGE CONFIG
st.set_page_config(page_title="Economic Indicators Tracker", layout="wide")

# LOAD DATA 
df = load_data()
latest = get_latest(df)

# HEADER
st.title("Economic Indicators Tracker")
st.caption("US macroeconomic indicators sourced from FRED")

st.divider()

# METRIC CARDS
indicators = latest["indicator_name"].tolist()
cols = st.columns(len(indicators))

for col, (_, row) in zip(cols, latest.iterrows()):
    col.metric(
        label=row["indicator_name"],
        value=f"{row['value']:.2f}",
        delta=f"{row['mom_change']:.2f}" if pd.notna(row["mom_change"]) else "N/A",
        delta_color=get_delta_color(row["indicator_name"]),
    )

st.divider()

# --- SIDEBAR ---
st.sidebar.header("Controls")

selected = st.sidebar.selectbox("Select Indicator", indicators)

years = st.sidebar.slider("Years of history", min_value=1, max_value=24, value=10)

# CHART
chart_df = df[df["indicator_name"] == selected].copy()

# filter to the selected number of years
cutoff = pd.Timestamp.today() - pd.DateOffset(years=years)
chart_df = chart_df[chart_df["observation_date"] >= cutoff]

st.subheader(f"{selected} -- Last {years} Years")

chart_df["year"] = chart_df["observation_date"].dt.year

fig = px.line(
    chart_df,
    x="observation_date",
    y="value",
    title=f"{selected} -- Last {years} Years",
    labels={
        "observation_date": "Date",
        "value": selected
    }
)

fig.update_xaxes(
    dtick="M12",          # tick every 12 months
    tickformat="%Y",      # show only the year label
    tickangle=0,          # keep labels horizontal
)

fig.update_layout(
    hovermode="x unified",   # shows all values at a date on hover
    yaxis_title=selected,
    xaxis_title="",
)

st.plotly_chart(fig, use_container_width=True)
