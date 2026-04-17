import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

load_dotenv()

# Get connection
def get_secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key)
    
def get_connection():
    return snowflake.connector.connect(
        account = get_secret("SNOWFLAKE_ACCOUNT"),
        user = get_secret("SNOWFLAKE_USER"),
        password = get_secret("SNOWFLAKE_PASSWORD"),
        warehouse = get_secret("SNOWFLAKE_WAREHOUSE"),
        database = "TAXI_DB",
        schema = "MARTS",
        role = "ACCOUNTADMIN"
    )

# Loading data
@st.cache_data(ttl=3600)
def load_daily_revenue():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            pickup_date,
            pickup_borough,
            total_trips,
            total_revenue,
            avg_fare,
            avg_tip
        FROM TAXI_DB.MART.MART_DAILY_REVENUE
        ORDER BY pickup_date, pickup_borough
    """, conn)
    conn.close()
    df.columns = [col.lower() for col in df.columns]
    df["pickup_date"] = pd.to_datetime(df["pickup_date"])
    return df

@st.cache_data(ttl=3600)
def load_pickup_zones():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            pickup_borough,
            pickup_zone,
            hour_of_day,
            total_trips
        FROM TAXI_DB.MART.MART_PICKUP_ZONES
        ORDER BY pickup_borough, pickup_zone, hour_of_day
    """, conn)
    conn.close()
    df.columns = [col.lower() for col in df.columns]
    return df

@st.cache_data(ttl=3600)
def load_payment_summary():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            payment_label,
            total_trips,
            total_revenue,
            avg_tip_rate_pct
        FROM TAXI_DB.MART.MART_PAYMENT_SUMMARY
        ORDER BY total_trips DESC
    """, conn)
    conn.close()
    df.columns = [col.lower() for col in df.columns]
    return df

@st.cache_data(ttl=3600)
def load_trip_duration():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT
            trip_date,
            pickup_borough,
            avg_duration_minutes,
            median_duration_minutes,
            total_trips
        FROM TAXI_DB.MART.MART_TRIP_DURATION
        ORDER BY trip_date, pickup_borough
    """, conn)
    conn.close()
    df.columns = [col.lower() for col in df.columns]
    df["trip_date"] = pd.to_datetime(df["trip_date"])
    return df

# Streamlit app section
st.set_page_config(
    page_title="NYC Yellow Taxi Trips Dashboard",
    layout="wide"
)

st.title("NYC Yellow Taxi Trips Dashboard")
st.caption("Nov 2025 - Jan 2026 | Source: NYC TLC Trip Records | Transformed with dbt on Snowflake")
st.divider()

# Chart 1: Daily Revenue by Borough
st.subheader("Daily Revenue by Borough")
st.caption("Total fare + tip revenue per day, split by pickup borough")

df_revenue = load_daily_revenue()

boroughs = sorted(df_revenue['pickup_borough'].unique().tolist())
selected_boroughs = st.multiselect(
    "Filter Borough",
    options=boroughs,
    default=boroughs,
    key="borough_filter"
)

if selected_boroughs:
    df_revenue_filtered = df_revenue[df_revenue['pickup_borough'].isin(selected_boroughs)]
else:
    df_revenue_filtered = df_revenue

fig_revenue = px.line(
    df_revenue_filtered,
    x = 'pickup_date',
    y = 'total_revenue',
    color = 'pickup_borough',
    labels = {
        'pickup_date': 'date',
        'total_revenue': 'Total Revenue ($)',
        'pickup_borough': 'Borough'
    }
)

fig_revenue.update_layout(
    xaxis_title = "",
    yaxis_title = "Total Revenue ($)",
    legend_title = "Borough"
)

st.plotly_chart(fig_revenue, use_container_width=True)

st.divider()

# Chart 2: Pickup Zone Heatmap by Hour
st.subheader("Pickup Zone Activity by Hour of Day")
st.caption("Trip volume by zone and hour (darker = more trips)")

df_zones = load_pickup_zones()
borough = sorted(df_zones['pickup_borough'].unique().tolist())

selected_borough = st.selectbox(
    "Select Borough",
    options = borough,
    index = 0,
    key = "zone_borough_filter"
)

df_zones_filtered = df_zones[df_zones['pickup_borough'] == selected_borough]

heatmap_data = df_zones_filtered.pivot_table(
    values = 'total_trips',
    index = 'pickup_zone',
    columns = 'hour_of_day',
    fill_value = 0    #if no trips then impute as 0
)

top_zones = df_zones_filtered.groupby('pickup_zone')['total_trips'].sum().nlargest(20).index  #top 20 zones to keep chart readable
heatmap_data = heatmap_data.loc[heatmap_data.index.isin(top_zones)]

fig_heatmap = go.Figure(
    data = go.Heatmap(
        x = [f"{h:02d}:00" for h in heatmap_data.columns],
        y = heatmap_data.index.tolist(),
        z = heatmap_data.values,
        colorscale = 'Blues',
    )
)

fig_heatmap.update_layout(
    xaxis_title="Hour of Day",
    yaxis_title="Pickup Zone",
    height=600
)
st.plotly_chart(fig_heatmap, use_container_width=True)

st.divider()

# Chart 3: Payment Type Breakdown
st.subheader("Payment Type Breakdown")
st.caption("Trip volume and average tip rate by payment method")

df_payment = load_payment_summary()

col1, col2 = st.columns(2)

with col1:
    fig_trips = px.bar(
        df_payment,
        x="payment_label",
        y="total_trips",
        labels={
            "payment_label": "Payment Type",
            "total_trips": "Total Trips"
        },
        title="Trip Volume by Payment Type",
        color="payment_label"
    )
    fig_trips.update_layout(showlegend = False, xaxis_title = "", yaxis_title="Total Trips")
    st.plotly_chart(fig_trips, use_container_width=True)

with col2:
    fig_tip = px.bar(
       df_payment,
        x="payment_label",
        y="avg_tip_rate_pct",
        labels={
            "payment_label": "Payment Type",
            "avg_tip_rate_pct": "Avg Tip Rate (%)"
        },
        title="Average Tip Rate by Payment Type",
        color="payment_label" 
    )
    fig_tip.update_layout(showlegend=False, xaxis_title="", yaxis_title="Avg Tip Rate (%)")
    st.plotly_chart(fig_tip, use_container_width=True)

st.divider()

# Chart 4: Avg vs Median Trip Duration by Borough
st.subheader("Trip Duration by Borough")
st.caption("Average vs median duration in minutes - gap between the two shows how much outliers skew the average")

df_duration = load_trip_duration()

df_duration_agg = df_duration.groupby("pickup_borough").agg(
    avg_duration_minutes=("avg_duration_minutes", "mean"),
    median_duration_minutes=("median_duration_minutes", "mean"),
    total_trips=("total_trips", "sum")
).reset_index()   #Aggregation across boroughs to remove the daily granularity

df_duration_melted = df_duration_agg.melt(
    id_vars="pickup_borough",
    value_vars=["avg_duration_minutes", "median_duration_minutes"],
    var_name="metric",
    value_name="minutes"
)
df_duration_melted["metric"] = df_duration_melted["metric"].map({
    "avg_duration_minutes": "Average",
    "median_duration_minutes": "Median"
})

fig_duration = px.bar(
    df_duration_melted,
    x="pickup_borough",
    y="minutes",
    color="metric",
    barmode="group",
    labels={
        "pickup_borough": "Borough",
        "minutes": "Duration (minutes)",
        "metric": "Metric"
    },
    color_discrete_map={
        "Average": "#3B82F6",
        "Median": "#10B981"
    }
)
fig_duration.update_layout(
    xaxis_title="",
    yaxis_title="Duration (minutes)",
    legend_title="Metric"
)
st.plotly_chart(fig_duration, use_container_width=True)

