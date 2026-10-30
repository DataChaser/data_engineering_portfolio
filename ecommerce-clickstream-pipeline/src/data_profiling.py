import os
import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

# Setup spark
spark = (
    SparkSession.builder.master("local[*]").appName("data_profile")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN") # keep warnings and errors only in log.

# Defining the schema
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", LongType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
])

# Read data
df = (
    spark.read
    .csv("datasets/2019-Oct.csv.gz", header=True, schema=schema)
    .limit(1_000_000).cache()
)

# Checks

# 1. Number of rows, types and quick look at the data
print("==== 1. Row and column checks")
total_rows = df.count()
print(f"Total: {total_rows} rows")
df.printSchema()
df.show(10, truncate=False)

# 2. Event type list
print("==== 2. Event type check")
df.groupby("event_type").count().orderBy(f.desc("count")).show()

# 3. Null % per column
print("==== 3. Null % per column")
df.select([
    f.round(f.sum(f.col(c).isNull().cast("int")) / total_rows * 100, 2).alias(c)
    for c in df.columns
]).show(vertical=True)

# 4. Price checks
print("==== 4. Price data sanity checks")
df.select(
    f.min("price").alias("min_price"), f.max("price").alias("max_price"),
    f.avg("price").alias("avg_price"),
    f.percentile_approx("price", [0.5, 0.99]).alias("p50_p99"),
).show(truncate=False)
print('Number of rows with price <= 0', df.filter(f.col("price") <= 0).count())
# Checking which event_type 0 or less than 0 price belongs in
df.filter(f.col("price") <= 0).groupBy("event_type").count().show()
df.filter(f.col("price") <= 0).show(5, truncate=False)

# 5. Timetamps check
print("==== 5. Timestamp checks")
with_ts = df.withColumn(
    "event_ts", f.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")
)
print("Unparsed timestamps:",
      with_ts.filter(f.col("event_time").isNotNull() & f.col("event_ts").isNull()).count())
with_ts.select(f.min("event_ts"), f.max("event_ts")).show(truncate=False)

# 6. Duplicate row checks
print("==== 6. Duplicate rows check")
print("Exact duplicate rows:", total_rows - df.dropDuplicates().count())

# Stop spark session
spark.stop()