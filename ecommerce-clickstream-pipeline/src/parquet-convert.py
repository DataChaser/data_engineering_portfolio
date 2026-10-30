import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Command line arguments
source_path = sys.argv[1] #where to read from (a file, or a pattern like *.csv.gz)
output_path = sys.argv[2] #where to write to

spark = (
    SparkSession.builder.appName("parquet_convert")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

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

# Reading the files in GCS
df = spark.read.csv(source_path, header=True, schema=schema)
# Adding year-month column
df = df.withColumn("year_month", f.substring("event_time", 1, 7))
# Writing to parquet
df.write.mode("overwrite").parquet(output_path)

# Quick sanity check
spark.read.parquet(output_path).groupBy("year_month").count().orderBy("year_month").show()

spark.stop()