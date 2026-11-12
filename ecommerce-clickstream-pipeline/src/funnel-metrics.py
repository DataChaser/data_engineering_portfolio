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
logger = logging.getLogger("funnel_metrics")

# Command line arguments
base_path = sys.argv[1]

sessions_path = f"{base_path}/gold/sessions"
funnel_path = f"{base_path}/gold/funnel_by_month"

#  Start Spark session
spark = (
    SparkSession.builder.appName("funnel_metrics")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Starting funnel metrics job")

# Data Operations
sessions = spark.read.parquet(sessions_path)
sessions = sessions.withColumn("month", f.date_format("session_start", "yyyy-MM"))

has_view = f.col("view_count") > 0
has_cart = f.col("cart_count") > 0
has_purchase = f.col("purchase_count") > 0

funnel = sessions.rollup("month").agg(
    f.count(f.lit(1)).alias("sessions"),
    f.sum(has_view.cast("int")).alias("sessions_with_view"),
    f.sum(has_cart.cast("int")).alias("sessions_with_cart"),
    f.sum(has_purchase.cast("int")).alias("sessions_with_purchase"),
    f.sum((has_cart & ~has_purchase).cast("int")).alias("cart_abandoned_sessions"),
    f.sum((has_purchase & ~has_cart).cast("int")).alias("purchase_without_cart_sessions"),
)

funnel = funnel.withColumn("month", f.coalesce(f.col("month"), f.lit("all")))

funnel = (
    funnel
    .withColumn("pct_with_view", f.round(f.col("sessions_with_view") / f.col("sessions") * 100, 2))
    .withColumn("pct_with_cart", f.round(f.col("sessions_with_cart") / f.col("sessions") * 100, 2))
    .withColumn("pct_with_purchase", f.round(f.col("sessions_with_purchase") / f.col("sessions") * 100, 2))
    .withColumn("cart_abandonment_pct", f.round(f.col("cart_abandoned_sessions") / f.col("sessions_with_cart") * 100, 2))
)

funnel.coalesce(1).write.mode("overwrite").parquet(funnel_path)

# Data Summary ----------
saved = spark.read.parquet(funnel_path).orderBy("month")
logger.info("Funnel counts per month:")
saved.select(
    "month", "sessions", "sessions_with_view", "sessions_with_cart",
    "sessions_with_purchase", "cart_abandoned_sessions", "purchase_without_cart_sessions",
).show(truncate=False)
logger.info("Funnel percentages per month:")
saved.select(
    "month", "pct_with_view", "pct_with_cart", "pct_with_purchase", "cart_abandonment_pct",
).show(truncate=False)

# Stop Spark session
spark.stop()