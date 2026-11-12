import logging
import sys
from pyspark.sql import SparkSession, functions as f

# Logging setup
logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("session_metrics")

# Command line arguments
base_path = sys.argv[1]  #processed data bucket
silver_path = f"{base_path}/silver/events"  # reading data from silver folder
sessions_path = f"{base_path}/gold/sessions" # writing data in gold layer

# Spark session start
spark = SparkSession.builder.appName("session_metrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logger.info("Starting session metrics build")

# Data operations
df = spark.read.parquet(silver_path)
df = df.filter(f.col("user_session").isNotNull())

sessions = df.groupBy("user_session", "user_id").agg(
    f.min("event_ts").alias("session_start"),
    f.max("event_ts").alias("session_end"),
    f.count(f.lit(1)).alias("event_count"),
    f.sum((f.col("event_type") == "view").cast("int")).alias("view_count"),
    f.sum((f.col("event_type") == "cart").cast("int")).alias("cart_count"),
    f.sum((f.col("event_type") == "purchase").cast("int")).alias("purchase_count"),
    f.sum(
        f.when(f.col("event_type") == "purchase", f.col("price")).otherwise(0.0)
    ).alias("revenue"),
)

sessions = sessions.withColumn(
    "duration_seconds",
    f.col("session_end").cast("long") - f.col("session_start").cast("long"),
)

sessions.write.mode("overwrite").parquet(sessions_path)

# Data Summary
saved = spark.read.parquet(sessions_path)
logger.info("Session summary:")
saved.agg(
    f.count(f.lit(1)).alias("sessions"),
    f.round(f.avg("event_count"), 2).alias("avg_events_per_session"),
    f.round(f.avg("duration_seconds") / 60, 2).alias("avg_duration_min"),
    f.round(f.percentile_approx("duration_seconds", 0.5) / 60, 2).alias("median_duration_min"),
    f.round(f.avg((f.col("cart_count") > 0).cast("int")) * 100, 2).alias("pct_sessions_with_cart"),
    f.round(f.avg((f.col("purchase_count") > 0).cast("int")) * 100, 2).alias("pct_sessions_with_purchase"),
    f.round(f.sum("revenue"), 2).alias("total_revenue"),
).show(vertical=True)

# Spark session stop
spark.stop()