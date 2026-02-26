#!/usr/bin/env python3
"""
01_prepare_incremental_batches.py

Prepares all source data needed before running the write benchmarks:
  1. Creates 3 incremental batch parquets from the existing logical-type source
     (each batch = 100 records covering 100 unique partition_col values = 100 FG updates)
  2. Generates a no-logical-type parquet source (500 plain string columns, 10K partitions)
  3. Creates 3 incremental batches from the no-logical source
  4. Generates no_logical_schema.avsc and uploads it to S3

Run:
    /home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \\
        --conf spark.driver.memory=8g \\
        --conf spark.executor.memory=6g \\
        testing-logical/perf/emr/01_prepare_incremental_batches.py

The script is idempotent: re-running overwrites existing staging data.
"""

import json
import os

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

# ── S3 paths ──────────────────────────────────────────────────────────────────
S3_BUCKET = "performance-benchmark-datasets-us-west-2"
S3_PREFIX = "hudi-bench/pavijars"
S3_BASE = f"s3://{S3_BUCKET}/{S3_PREFIX}"

LOGICAL_SOURCE   = f"{S3_BASE}/data/wide_500cols_10000parts"
NO_LOGICAL_SOURCE = f"{S3_BASE}/data/no_logical_500cols_10000parts"
STAGING_BASE     = f"{S3_BASE}/data/incremental_staging"

# Incremental batches: 3 non-overlapping sets of 100 FG records.
# col_1 values follow the pattern "value_{partition_index}_1" from initial_batch.scala.
BATCH_RANGES = {
    "batch_2": (1,   100),   # FGs 1-100   (used for write 2)
    "batch_3": (101, 200),   # FGs 101-200 (used for write 3, with upgraded JAR)
    "batch_4": (201, 300),   # FGs 201-300 (used for write 4)
}

NUM_COLS       = 500
NUM_PARTITIONS = 10_000


def create_batches(spark, src_df, schema_type: str):
    """Filter and write 3 incremental batch parquets from src_df to S3 staging."""
    print(f"\n--- Creating incremental batches for '{schema_type}' source ---")
    for batch_name, (start, end) in BATCH_RANGES.items():
        values = [f"value_{i}_1" for i in range(start, end + 1)]
        batch_df = src_df.filter(F.col("col_1").isin(values))
        out_path = f"{STAGING_BASE}/{schema_type}/{batch_name}"
        # coalesce to a small number of files; each batch is only 100 rows
        batch_df.coalesce(4).write.mode("overwrite").parquet(out_path)
        count = batch_df.count()
        print(f"  {batch_name}: {count} records (FGs {start}-{end}) → {out_path}")


def generate_no_logical_source(spark):
    """Generate 10K-partition parquet with 500 plain string columns (no timestamps)."""
    print(f"\n--- Generating no-logical-type parquet source ({NUM_PARTITIONS} partitions) ---")

    # Build column expressions: col_1..col_500 as string, plus partition_col
    col_exprs = [
        F.concat(F.lit("value_"), F.col("id"), F.lit(f"_{i}")).alias(f"col_{i}")
        for i in range(1, NUM_COLS + 1)
    ]
    col_exprs.append(
        F.format_string("partition_%05d", F.col("id")).alias("partition_col")
    )

    df = spark.range(1, NUM_PARTITIONS + 1).select(*col_exprs)
    df.repartition(NUM_PARTITIONS, F.col("partition_col")) \
      .write.mode("overwrite").parquet(NO_LOGICAL_SOURCE)
    print(f"  Written {NUM_PARTITIONS} records → {NO_LOGICAL_SOURCE}")
    return df


def generate_no_logical_avsc():
    """Build no_logical_schema.avsc and upload to S3."""
    print("\n--- Generating no_logical_schema.avsc ---")

    fields = [
        {"name": f"col_{i}", "type": ["null", "string"], "default": None}
        for i in range(1, NUM_COLS + 1)
    ]
    fields.append({"name": "partition_col", "type": ["null", "string"], "default": None})

    avsc = {
        "type":      "record",
        "name":      "no_logical_schema",
        "namespace": "com.example.hudi",
        "fields":    fields,
    }

    local_path = "/tmp/no_logical_schema.avsc"
    with open(local_path, "w") as fh:
        json.dump(avsc, fh, indent=2)

    s3_key = f"{S3_PREFIX}/no_logical_schema.avsc"
    s3 = boto3.client("s3")
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"  Uploaded → s3://{S3_BUCKET}/{s3_key}")

    # Also copy to local JARS_DIR so the write benchmark can reference it directly
    jars_dir = "/home/hadoop/hudi-jars"
    os.makedirs(jars_dir, exist_ok=True)
    import shutil
    shutil.copy(local_path, f"{jars_dir}/no_logical_schema.avsc")
    print(f"  Copied   → {jars_dir}/no_logical_schema.avsc")


# ── Main ──────────────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("PrepareIncrementalBatches") \
    .getOrCreate()

print("======================================================")
print(" Preparing benchmark source data")
print("======================================================")

# Part 1: Incremental batches for the logical-type source (already exists in S3)
print(f"\nReading logical-type source: {LOGICAL_SOURCE}")
logical_src = spark.read.parquet(LOGICAL_SOURCE)
print(f"  Total records: {logical_src.count()}")
create_batches(spark, logical_src, "logical")

# Part 2: Generate no-logical-type source data
no_logical_src = generate_no_logical_source(spark)

# Part 3: Incremental batches for no-logical source
# Re-read from S3 to use persisted data (avoids recomputing the large DF)
no_logical_src_s3 = spark.read.parquet(NO_LOGICAL_SOURCE)
create_batches(spark, no_logical_src_s3, "no_logical")

# Part 4: Generate and upload no-logical Avro schema
generate_no_logical_avsc()

print("\n======================================================")
print(" Preparation complete. Staging paths:")
for schema_type in ("logical", "no_logical"):
    for batch_name in BATCH_RANGES:
        print(f"   {STAGING_BASE}/{schema_type}/{batch_name}/")
print("======================================================")

spark.stop()
