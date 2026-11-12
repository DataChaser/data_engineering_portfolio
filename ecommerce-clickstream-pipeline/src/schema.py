import logging
import sys
from pyspark.sql import SparkSession, functions as f

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("schema")

# Command line arguments
base_path = sys.argv[1]

# Read paths
silver_path = f"{base_path}/silver/events"
sessions_path = f"{base_path}/gold/sessions"
buyers_path = f"{base_path}/gold/customers"

# Write paths
fact_path = f"{base_path}/gold/fact_events"
dim_product_path = f"{base_path}/gold/dim_product"
dim_customer_path = f"{base_path}/gold/dim_customer"

spark = (
    SparkSession.builder.appName("schema")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info("Starting schema build")

# Data Operations
silver = spark.read.parquet(silver_path)

# TABLE 1: fact_events -- one row per event
fact = silver.filter(f.col("user_session").isNotNull())
fact = fact.withColumn("event_date", f.to_date("event_ts"))
fact = fact.select("event_ts", "event_date", "event_type", "user_id", 
                   "user_session", "product_id", "price",)
fact.write.mode("overwrite").parquet(fact_path)
logger.info("fact_events written")

# TABLE 2: dim_product -- one row per product
products = silver.groupBy("product_id").agg(
    f.max("category_id").alias("category_id"),
    f.max(f.when(f.col("category_code") != "unknown", f.col("category_code"))).alias("category_code"),
    f.max(f.when(f.col("brand") != "unknown", f.col("brand"))).alias("brand"),
    f.countDistinct(f.when(f.col("category_code") != "unknown", f.col("category_code"))).alias("category_variants"),
    f.countDistinct(f.when(f.col("brand") != "unknown", f.col("brand"))).alias("brand_variants"),
)
products = products.cache()
brand_conflicts = products.filter(f.col("brand_variants") > 1).count()
category_conflicts = products.filter(f.col("category_variants") > 1).count()
logger.info(f"Products with more than one real brand: {brand_conflicts:,}")
logger.info(f"Products with more than one real category: {category_conflicts:,}")
products = (
    products
    .withColumn("category_code", f.coalesce(f.col("category_code"), f.lit("unknown")))
    .withColumn("brand", f.coalesce(f.col("brand"), f.lit("unknown")))
    # Choose the final columns, in a fixed order, and drop the two check columns.
    .select("product_id", "category_id", "category_code", "brand")
)
products.coalesce(1).write.mode("overwrite").parquet(dim_product_path)
logger.info("dim_product written")

# TABLE 3: dim_customer -- one row per user
sessions = spark.read.parquet(sessions_path)
users = sessions.groupBy("user_id").agg(
    f.min("session_start").alias("first_seen"),
    f.max("session_end").alias("last_seen"),
    f.count(f.lit(1)).alias("session_count"),
    f.sum("event_count").alias("event_count"),
)
buyers = spark.read.parquet(buyers_path)
dim_customer = users.join(buyers, on="user_id", how="left")
dim_customer = dim_customer.fillna({"order_count": 0, "total_spent": 0.0})
dim_customer = dim_customer.select(
    "user_id", "first_seen", "last_seen", "session_count", "event_count",
    "order_count", "last_purchase_time", "total_spent",
)
dim_customer.coalesce(1).write.mode("overwrite").parquet(dim_customer_path)
logger.info("dim_customer written")

# Data summary
fact_rows = spark.read.parquet(fact_path).count()
product_rows = spark.read.parquet(dim_product_path).count()
saved_customers = spark.read.parquet(dim_customer_path)

logger.info(f"fact_events rows: {fact_rows}")
logger.info(f"dim_product rows: {product_rows}")

logger.info("dim_customer summary:")
saved_customers.agg(
    f.count(f.lit(1)).alias("customers"),
    f.sum("event_count").alias("total_events"),
    f.sum((f.col("order_count") > 0).cast("int")).alias("buyers"),
    f.sum("order_count").alias("total_orders"),
    f.round(f.sum("total_spent"), 2).alias("total_spent"),
).show(vertical=True)

spark.stop()