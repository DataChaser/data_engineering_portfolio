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
logger = logging.getLogger("customer_summary")

# Command line arguments
base_path = sys.argv[1]

sessions_path = f"{base_path}/gold/sessions"
customers_path = f"{base_path}/gold/customers"

spark = SparkSession.builder.appName("customer_summary").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logger.info("Starting customer summary job")

# Data operations
sessions = spark.read.parquet(sessions_path)
orders = sessions.filter(f.col("purchase_count") > 0)

customers = orders.groupBy("user_id").agg(
    f.max("session_end").alias("last_purchase_time"),
    f.count(f.lit(1)).alias("order_count"),
    f.round(f.sum("revenue"), 2).alias("total_spent"),
)

customers.coalesce(1).write.mode("overwrite").parquet(customers_path)

# Data Summary
saved = spark.read.parquet(customers_path)
logger.info("Customer summary:")
saved.agg(
    f.count(f.lit(1)).alias("customers"),
    f.sum("order_count").alias("total_orders"),
    f.round(f.sum("total_spent"), 2).alias("total_spent"),
    f.round(f.avg((f.col("order_count") == 1).cast("int")) * 100, 2).alias("pct_bought_once"),
    f.round(f.avg("total_spent"), 2).alias("avg_spend"),
    f.max("order_count").alias("max_orders"),
).show(vertical=True)

# Shut the Spark session
spark.stop()