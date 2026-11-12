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
logger = logging.getLogger("data-cleaning")

# Path setups
base_path = sys.argv[1]
year_month = sys.argv[2] if len(sys.argv) > 2 else None
bronze_path = f"{base_path}/bronze/events"
silver_path = f"{base_path}/silver/events"

# Constants -- allowed values, columns, formats etc.
allowed_event_types = ["view", "cart", "remove_from_cart", "purchase"]
event_time_format = "yyyy-MM-dd HH:mm:ss 'UTC'"
silver_job_columns = [
    "event_ts", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session", "year_month",
]

# Spark setup
spark = (
    SparkSession.builder.appName("data-cleaning")
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
logger.info(f"Starting light data cleaning job. Month filter: {year_month or 'all months'}")

# Data operations
df = spark.read.parquet(bronze_path)
if year_month:
    df = df.filter(f.col("year_month")==year_month)
bronze_rows = df.count()
logger.info(f"Bronze total rows: {bronze_rows:,}")
if bronze_rows == 0:
    logger.error("No Bronze rows found. Check the month argument. Nothing was written.")
    spark.stop()
    sys.exit(1)

df = df.withColumn("event_ts", f.to_timestamp("event_time", event_time_format))
df = df.fillna({'category_code': 'unknown', 'brand': 'unknown'}) 
df = df.select(silver_job_columns)
df = df.dropDuplicates()

# Writing output to silver path
df.write.mode("overwrite").parquet(silver_path)

# Summary checks
silver_rows = spark.read.parquet(silver_path).count()
removed_rows = bronze_rows - silver_rows
logger.info(f"Silver rows written: {silver_rows:,}")
logger.info(f"Duplicate rows removed: {removed_rows:,}")
logger.info(f"({removed_rows / bronze_rows * 100:.2f}% of Bronze)")

# Shutting off spark session
spark.stop()



