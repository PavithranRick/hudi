#!/usr/bin/env python3
"""
Generate Update Batches for Hudi Benchmark

This script generates different types of update batches:
- write2: Full dataset (all partitions)
- update1: Targeted update (100 file groups)
- update2: Targeted update (100 file groups, different records)

Usage:
  spark-submit generate_update_batch.py <input_path> <output_path> <batch_type> [num_partitions]

Parameters:
  input_path: S3 path to source Parquet data
  output_path: S3 path for output Parquet data
  batch_type: Type of batch (write2, update1, update2)
  num_partitions: Number of partitions to update (default: 100 for updates, all for write2)
"""

import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, unix_timestamp

def generate_write2_batch(spark, input_path, output_path):
    """
    Generate write2 batch - full dataset reread
    This simulates a full upsert operation
    """
    print(f"📊 Generating write2 batch (full dataset)")
    print(f"   Input: {input_path}")
    print(f"   Output: {output_path}")

    # Read all data
    df = spark.read.parquet(input_path)

    # Update precombine_field to ensure these records are newer
    # Add a small increment to make them win during upsert
    current_ts = int(time.time() * 1000)
    updated_df = df.withColumn("precombine_field", lit(current_ts))

    count = updated_df.count()
    print(f"   Records: {count}")

    # Write to output
    updated_df.write.mode("overwrite").parquet(output_path)
    print(f"✅ write2 batch generated successfully")

    return count

def generate_update_batch(spark, input_path, output_path, num_partitions, batch_suffix):
    """
    Generate targeted update batch - updates only specified number of partitions
    """
    print(f"📊 Generating {batch_suffix} batch (targeted update)")
    print(f"   Input: {input_path}")
    print(f"   Output: {output_path}")
    print(f"   Target partitions: {num_partitions}")

    # Read all data
    df = spark.read.parquet(input_path)

    # Select specific partitions for update
    # For update1: partitions 1-100
    # For update2: partitions 101-200 (to avoid overlap)
    if batch_suffix == "update1":
        start_partition = 1
    else:  # update2
        start_partition = 101

    end_partition = start_partition + num_partitions - 1

    partition_values = [f"partition_{i:05d}" for i in range(start_partition, end_partition + 1)]

    # Filter to selected partitions
    filtered_df = df.filter(col("partition_key").isin(partition_values))

    # Update precombine_field to ensure these records win during upsert
    # Use current timestamp plus offset to make them newer than previous writes
    current_ts = int(time.time() * 1000)
    offset = 1000 if batch_suffix == "update1" else 2000
    updated_df = filtered_df.withColumn("precombine_field", lit(current_ts + offset))

    count = updated_df.count()
    print(f"   Records: {count}")
    print(f"   Partition range: {start_partition} to {end_partition}")

    # Write to output
    updated_df.write.mode("overwrite").parquet(output_path)
    print(f"✅ {batch_suffix} batch generated successfully")

    return count

def main():
    if len(sys.argv) < 4:
        print("Usage: generate_update_batch.py <input_path> <output_path> <batch_type> [num_partitions]")
        print("batch_type: write2, update1, update2")
        print("num_partitions: number of partitions to update (default: 100 for updates)")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    batch_type = sys.argv[3]
    num_partitions = int(sys.argv[4]) if len(sys.argv) > 4 else 100

    # Validate batch type
    if batch_type not in ["write2", "update1", "update2"]:
        print(f"ERROR: Invalid batch_type '{batch_type}'. Must be: write2, update1, or update2")
        sys.exit(1)

    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"GenerateUpdateBatch-{batch_type}") \
        .getOrCreate()

    try:
        # Generate appropriate batch
        if batch_type == "write2":
            count = generate_write2_batch(spark, input_path, output_path)
        else:
            count = generate_update_batch(spark, input_path, output_path, num_partitions, batch_type)

        print(f"\n{'='*60}")
        print(f"Batch generation completed: {batch_type}")
        print(f"Total records: {count}")
        print(f"Output location: {output_path}")
        print(f"{'='*60}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
