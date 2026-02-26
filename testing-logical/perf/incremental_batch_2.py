#!/usr/bin/env python3
"""
Incremental Batch 2 - Read and append all records
Usage: spark-submit incremental_batch_2.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session (for EMR, no need to specify master)
spark = SparkSession.builder \
    .appName("WideTimestampExample-IncrementalBatch2") \
    .getOrCreate()

# S3 path
data_path = "s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts"

print(f"🚀 Starting incremental batch 2...")
print(f"📍 Reading from: {data_path}")

# Read the existing parquet data
df = spark.read.parquet(data_path)

print(f"📊 Read {df.count()} records")
print(f"💾 Appending all data back to: {data_path}")

# Append all data back
df.write.mode("append").parquet(data_path)

print("✅ Incremental batch 2 completed successfully")

# Stop the spark session
spark.stop()
