#!/bin/bash
# Read Wrapper Script
# Executes Hudi read benchmark with proper configuration

set -e

# Usage check
if [ "$#" -lt 5 ]; then
    echo "Usage: $0 <HUDI_VERSION> <TABLE_PATH> <CONFIG_TYPE> <TABLE_TYPE> <SCENARIO>"
    echo ""
    echo "Parameters:"
    echo "  HUDI_VERSION: 0.15.0 or 0.16.0-SNAPSHOT"
    echo "  TABLE_PATH: S3 path to Hudi table"
    echo "  CONFIG_TYPE: config_a or config_b"
    echo "  TABLE_TYPE: COPY_ON_WRITE or MERGE_ON_READ"
    echo "  SCENARIO: baseline or experimental"
    exit 1
fi

# Parameters
HUDI_VERSION=$1
TABLE_PATH=$2
CONFIG_TYPE=$3
TABLE_TYPE=$4
SCENARIO=$5

# Configuration
SPARK_HOME="${SPARK_HOME:-/home/hadoop/spark-3.5.0-bin-hadoop3}"
JAR_BASE="/home/hadoop/hudi_jars"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Hudi bundle JAR
HUDI_BUNDLE="${JAR_BASE}/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar"

echo "=========================================="
echo "Hudi Read Benchmark"
echo "=========================================="
echo "Version: ${HUDI_VERSION}"
echo "Table: ${TABLE_PATH}"
echo "Config: ${CONFIG_TYPE}"
echo "Table Type: ${TABLE_TYPE}"
echo "Scenario: ${SCENARIO}"
echo "=========================================="

# Validate JAR exists
if [ ! -f "$HUDI_BUNDLE" ]; then
    echo "ERROR: Hudi bundle not found: $HUDI_BUNDLE"
    exit 1
fi

# Execute read benchmark
$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 8g \
    --executor-memory 16g \
    --executor-cores 4 \
    --num-executors 6 \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
    --jars "$HUDI_BUNDLE" \
    "${SCRIPTS_DIR}/read_benchmark.py" \
    "$TABLE_PATH" "$CONFIG_TYPE" "$TABLE_TYPE" "$HUDI_VERSION" "$SCENARIO"

if [ $? -eq 0 ]; then
    echo "✅ Read benchmark completed successfully"
else
    echo "❌ Read benchmark failed"
    exit 1
fi

echo "=========================================="
