#!/usr/bin/env python3
"""
03_read_benchmark.py

Timed snapshot read of a Hudi table with metadata and data-skipping disabled.
Runs distinct(<timestamp_col>).count() so every file group is read.

Usage:
    /home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \\
        --conf spark.driver.memory=8g \\
        --conf spark.executor.memory=6g \\
        --conf spark.executor.cores=3 \\
        --conf spark.dynamicAllocation.enabled=true \\
        --jars /home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-<version>.jar \\
        testing-logical/perf/emr/03_read_benchmark.py \\
        --table-path  s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical \\
        --label       cow_logical_0.15.0 \\
        --schema-type logical \\
        --results-file /home/hadoop/benchmark_results/read_results.csv

Schema-type notes:
  logical    → measures distinct(ts_millis_100)  [first ts_millis column in the schema]
  no_logical → measures distinct(col_100)        [equivalent plain-string column]
"""

import argparse
import csv
import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

# ── Argument parsing ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Hudi read benchmark")
parser.add_argument("--table-path",   required=True,  help="S3 path to the Hudi table")
parser.add_argument("--label",        required=True,  help="Identifier written to the results CSV")
parser.add_argument(
    "--schema-type",
    choices=["logical", "no_logical"],
    default="logical",
    help="Use 'logical' for ts_millis/ts_micros tables, 'no_logical' for plain-string tables",
)
parser.add_argument(
    "--results-file",
    default="/home/hadoop/benchmark_results/read_results.csv",
    help="Path to the CSV file where results are appended",
)
args = parser.parse_args()

# The initial_batch.scala schema places ts_millis columns at every 100th column
# (ts_millis_100, ts_millis_200, ...). ts_millis_100 is used here as the
# representative column for the distinct read.
MEASURE_COL = "ts_millis_100" if args.schema_type == "logical" else "col_100"

# ── Spark session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName(f"HudiReadBenchmark-{args.label}") \
    .getOrCreate()

# Disable metadata table so every file group is read via direct listing.
# Disable data skipping so no column stats short-circuit the full scan.
spark.conf.set("hoodie.metadata.enable",       "false")
spark.conf.set("hoodie.enable.data.skipping",  "false")

# ── Benchmark ─────────────────────────────────────────────────────────────────
print(f"Table:        {args.table_path}")
print(f"Label:        {args.label}")
print(f"Measure col:  {MEASURE_COL}")
print(f"Results file: {args.results_file}")
print("")

start = time.time()

df = spark.read.format("hudi").load(args.table_path)
distinct_count = df.select(MEASURE_COL).distinct().count()

elapsed = time.time() - start

print(f"distinct({MEASURE_COL}) = {distinct_count}")
print(f"Elapsed: {elapsed:.2f}s")

# ── Write results ─────────────────────────────────────────────────────────────
row = {
    "timestamp":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "label":           args.label,
    "schema_type":     args.schema_type,
    "measure_col":     MEASURE_COL,
    "distinct_count":  distinct_count,
    "elapsed_seconds": f"{elapsed:.2f}",
}

results_file = args.results_file
os.makedirs(os.path.dirname(results_file), exist_ok=True)
write_header = not os.path.exists(results_file)

with open(results_file, "a", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=row.keys())
    if write_header:
        writer.writeheader()
    writer.writerow(row)

print(f"Result written to: {results_file}")

spark.stop()
