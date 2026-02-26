#!/usr/bin/env python3
"""
Hudi Read Benchmark Script
Performs snapshot read with DISTINCT query and measures latency

Usage:
  spark-submit --jars <hudi-bundle.jar> read_benchmark.py <table_path> <config_type> <table_type> <hudi_version> <scenario>

Parameters:
  table_path: S3 path to Hudi table
  config_type: 'config_a' or 'config_b'
  table_type: 'COPY_ON_WRITE' or 'MERGE_ON_READ'
  hudi_version: '0.15.0' or '0.16.0-SNAPSHOT'
  scenario: 'baseline' or 'experimental'
"""

import sys
import time
from pyspark.sql import SparkSession

def run_read_benchmark(table_path, config_type, table_type, hudi_version, scenario):
    """
    Run read benchmark on Hudi table

    Measures latency for snapshot read with DISTINCT query
    Metadata table is disabled to force scanning all file groups
    """
    print("="*60)
    print("Hudi Read Benchmark")
    print("="*60)
    print(f"Table: {table_path}")
    print(f"Config: {config_type}")
    print(f"Table Type: {table_type}")
    print(f"Hudi Version: {hudi_version}")
    print(f"Scenario: {scenario}")
    print("="*60)

    # Create Spark session with Hudi configuration
    spark = SparkSession.builder \
        .appName(f"ReadBenchmark-{config_type}-{table_type}-{hudi_version}") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    try:
        # Disable metadata table and data skipping for true file group scanning
        spark.conf.set("hoodie.metadata.enable", "false")
        spark.conf.set("hoodie.enable.data.skipping", "false")

        print("📖 Reading Hudi table...")
        # Read Hudi table
        df = spark.read.format("hudi").load(table_path)

        # Determine query column based on config type
        if config_type == "config_a":
            # Config A has logical timestamp columns
            query_col = "ts_millis_50"
        else:
            # Config B has all string columns
            query_col = "col_50"

        print(f"🔍 Executing query: SELECT DISTINCT {query_col} ...")
        print("⏱️  Starting timer...")

        # Start timing
        start_time = time.time()

        # Execute query - DISTINCT count on specific column
        # This forces reading all data and performing aggregation
        result = df.select(query_col).distinct().count()

        # End timing
        end_time = time.time()
        latency = end_time - start_time

        print(f"✅ Query completed")
        print(f"📊 Distinct values found: {result}")
        print(f"⏱️  Read latency: {latency:.2f} seconds")

        # Log metrics to CSV
        metrics_line = f"{config_type},{table_type},{scenario},{hudi_version},read,{latency:.2f}"
        with open("/tmp/benchmark_metrics.csv", "a") as f:
            f.write(metrics_line + "\n")

        print(f"📊 Metrics logged: {metrics_line}")
        print("="*60)

        return latency

    except Exception as e:
        print(f"❌ ERROR during read benchmark: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        spark.stop()

def main():
    if len(sys.argv) != 6:
        print("Usage: read_benchmark.py <table_path> <config_type> <table_type> <hudi_version> <scenario>")
        print("\nParameters:")
        print("  table_path: S3 path to Hudi table")
        print("  config_type: 'config_a' or 'config_b'")
        print("  table_type: 'COPY_ON_WRITE' or 'MERGE_ON_READ'")
        print("  hudi_version: '0.15.0' or '0.16.0-SNAPSHOT'")
        print("  scenario: 'baseline' or 'experimental'")
        sys.exit(1)

    table_path = sys.argv[1]
    config_type = sys.argv[2]
    table_type = sys.argv[3]
    hudi_version = sys.argv[4]
    scenario = sys.argv[5]

    # Validate parameters
    if config_type not in ["config_a", "config_b"]:
        print(f"ERROR: Invalid config_type '{config_type}'. Must be 'config_a' or 'config_b'")
        sys.exit(1)

    if table_type not in ["COPY_ON_WRITE", "MERGE_ON_READ"]:
        print(f"ERROR: Invalid table_type '{table_type}'. Must be 'COPY_ON_WRITE' or 'MERGE_ON_READ'")
        sys.exit(1)

    if scenario not in ["baseline", "experimental"]:
        print(f"ERROR: Invalid scenario '{scenario}'. Must be 'baseline' or 'experimental'")
        sys.exit(1)

    # Run benchmark
    run_read_benchmark(table_path, config_type, table_type, hudi_version, scenario)

if __name__ == "__main__":
    main()
