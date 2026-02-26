#!/bin/bash
# Hudi Write Wrapper Script
# Parameterized HoodieStreamer execution with performance metrics collection

set -e

# Usage check
if [ "$#" -lt 7 ]; then
    echo "Usage: $0 <HUDI_VERSION> <TABLE_TYPE> <CONFIG_TYPE> <SOURCE_PATH> <TARGET_PATH> <CHECKPOINT_KEY> <ENABLE_COMPACTION>"
    echo ""
    echo "Parameters:"
    echo "  HUDI_VERSION: 0.15.0 or 0.16.0-SNAPSHOT"
    echo "  TABLE_TYPE: COPY_ON_WRITE or MERGE_ON_READ"
    echo "  CONFIG_TYPE: config_a or config_b"
    echo "  SOURCE_PATH: S3 path to source Parquet data"
    echo "  TARGET_PATH: S3 path for Hudi table"
    echo "  CHECKPOINT_KEY: Unique checkpoint key (e.g., write1_1234567890)"
    echo "  ENABLE_COMPACTION: true or false (only for MERGE_ON_READ)"
    exit 1
fi

# Parameters
HUDI_VERSION=$1
TABLE_TYPE=$2
CONFIG_TYPE=$3
SOURCE_PATH=$4
TARGET_PATH=$5
CHECKPOINT_KEY=$6
ENABLE_COMPACTION=$7

# Configuration
SPARK_HOME="${SPARK_HOME:-/home/hadoop/spark-3.5.0-bin-hadoop3}"
JAR_BASE="/home/hadoop/hudi_jars"
S3_SCHEMA_BASE="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars"

# Hudi JARs
HUDI_SPARK_BUNDLE="${JAR_BASE}/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar"
HUDI_UTILITIES="${JAR_BASE}/hudi-utilities-slim-spark3.5-bundle_2.12-${HUDI_VERSION}.jar"

# Schema file
SCHEMA_FILE="${S3_SCHEMA_BASE}/full_schema.avsc"

# Table name
TABLE_NAME="benchmark_${CONFIG_TYPE}_${TABLE_TYPE}_$(echo $HUDI_VERSION | tr '.' '_' | tr '-' '_')"

echo "=========================================="
echo "Hudi Write Operation"
echo "=========================================="
echo "Version: ${HUDI_VERSION}"
echo "Table Type: ${TABLE_TYPE}"
echo "Config: ${CONFIG_TYPE}"
echo "Source: ${SOURCE_PATH}"
echo "Target: ${TARGET_PATH}"
echo "Checkpoint: ${CHECKPOINT_KEY}"
echo "Compaction: ${ENABLE_COMPACTION}"
echo "=========================================="

# Validate JARs exist
if [ ! -f "$HUDI_SPARK_BUNDLE" ]; then
    echo "ERROR: Hudi Spark bundle not found: $HUDI_SPARK_BUNDLE"
    exit 1
fi

if [ ! -f "$HUDI_UTILITIES" ]; then
    echo "ERROR: Hudi Utilities bundle not found: $HUDI_UTILITIES"
    exit 1
fi

# Start timing
START_TIME=$(date +%s)
echo "⏱️  Start time: $(date)"

# Build spark-submit command
CMD_ARGS=(
    --master yarn
    --deploy-mode client
    --driver-memory 8g
    --executor-memory 16g
    --executor-cores 4
    --num-executors 6
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"
    --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog"
    --conf "spark.eventLog.enabled=false"
    --conf "spark.ui.enabled=true"
    --jars "$HUDI_SPARK_BUNDLE"
    --class org.apache.hudi.utilities.streamer.HoodieStreamer
    "$HUDI_UTILITIES"
    --table-type "${TABLE_TYPE}"
    --source-class org.apache.hudi.utilities.sources.ParquetDFSSource
    --target-base-path "${TARGET_PATH}"
    --target-table "${TABLE_NAME}"
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
    --hoodie-conf "hoodie.deltastreamer.source.dfs.root=${SOURCE_PATH}"
    --hoodie-conf "hoodie.deltastreamer.schemaprovider.source.schema.file=${SCHEMA_FILE}"
    --hoodie-conf "hoodie.deltastreamer.schemaprovider.target.schema.file=${SCHEMA_FILE}"
    --hoodie-conf "hoodie.datasource.write.recordkey.field=id"
    --hoodie-conf "hoodie.datasource.write.precombine.field=precombine_field"
    --hoodie-conf "hoodie.datasource.write.partitionpath.field=partition_key"
    --hoodie-conf "hoodie.datasource.write.drop.partition.columns=false"
    --hoodie-conf "hoodie.table.partition.fields=partition_key"
    --hoodie-conf "hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator"
    --hoodie-conf "hoodie.metadata.enable=false"
    --hoodie-conf "hoodie.deltastreamer.checkpoint.key=${CHECKPOINT_KEY}"
    --hoodie-conf "hoodie.parquet.small.file.limit=-1"
    --source-ordering-field precombine_field
    --checkpoint 0
)

# Add compaction config for MERGE_ON_READ
if [ "$TABLE_TYPE" == "MERGE_ON_READ" ]; then
    if [ "$ENABLE_COMPACTION" == "true" ]; then
        CMD_ARGS+=(--hoodie-conf "hoodie.compact.inline=true")
        CMD_ARGS+=(--hoodie-conf "hoodie.compact.inline.max.delta.commits=1")
    else
        CMD_ARGS+=(--hoodie-conf "hoodie.compact.inline=false")
        CMD_ARGS+=(--hoodie-conf "hoodie.compact.inline.max.delta.commits=999")
    fi
fi

# Execute HoodieStreamer
echo "🚀 Executing HoodieStreamer..."
$SPARK_HOME/bin/spark-submit "${CMD_ARGS[@]}"

# Check exit status
if [ $? -ne 0 ]; then
    echo "❌ ERROR: HoodieStreamer failed"
    exit 1
fi

# End timing
END_TIME=$(date +%s)
LATENCY=$((END_TIME - START_TIME))

echo "⏱️  End time: $(date)"
echo "✅ Write operation completed successfully"
echo "⏱️  Latency: ${LATENCY} seconds"

# Extract scenario and operation name from checkpoint key
# Format: write1_timestamp or similar
OPERATION=$(echo "$CHECKPOINT_KEY" | cut -d'_' -f1)

# Determine scenario based on version and target path
if echo "$TARGET_PATH" | grep -q "baseline"; then
    SCENARIO="baseline"
else
    SCENARIO="experimental"
fi

# Log metrics to CSV
METRICS_LINE="${CONFIG_TYPE},${TABLE_TYPE},${SCENARIO},${HUDI_VERSION},${OPERATION},${LATENCY}"
echo "$METRICS_LINE" >> /tmp/benchmark_metrics.csv

echo "📊 Metrics logged: $METRICS_LINE"
echo "=========================================="
