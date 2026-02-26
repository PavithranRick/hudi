#!/bin/bash
# 02_write_benchmark.sh
#
# Run the 4-write HoodieStreamer benchmark for one (TABLE_TYPE, SCHEMA_TYPE) pair.
#
# Write sequence:
#   Write 1  →  0.15.0 JAR   Full initial load  (10K FGs created)
#   Write 2  →  0.15.0 JAR   Stage batch_2 first (100 FG updates)
#              [upgrade: swap to 0.16.0-SNAPSHOT]
#   Write 3  →  0.16.0-SNAP  Stage batch_3 first (100 FG updates)
#   Write 4  →  0.16.0-SNAP  Stage batch_4 first (100 FG updates)
#
# Usage:
#   ./02_write_benchmark.sh <TABLE_TYPE> <SCHEMA_TYPE>
#
#   TABLE_TYPE:   COPY_ON_WRITE | MERGE_ON_READ
#   SCHEMA_TYPE:  logical | no_logical
#
# Results are appended to $RESULTS_DIR/write_results.csv.

set -euo pipefail

TABLE_TYPE="${1:?Usage: $0 <TABLE_TYPE> <SCHEMA_TYPE>}"
SCHEMA_TYPE="${2:?Usage: $0 <TABLE_TYPE> <SCHEMA_TYPE>}"

# ── Validate args ─────────────────────────────────────────────────────────────
[[ "$TABLE_TYPE" == "COPY_ON_WRITE" || "$TABLE_TYPE" == "MERGE_ON_READ" ]] || {
    echo "ERROR: TABLE_TYPE must be COPY_ON_WRITE or MERGE_ON_READ"; exit 1; }
[[ "$SCHEMA_TYPE" == "logical" || "$SCHEMA_TYPE" == "no_logical" ]] || {
    echo "ERROR: SCHEMA_TYPE must be logical or no_logical"; exit 1; }

# ── Configuration ─────────────────────────────────────────────────────────────
SPARK_HOME="/home/hadoop/spark-3.5.0-bin-hadoop3"
JARS_DIR="/home/hadoop/hudi-jars"
RESULTS_DIR="/home/hadoop/benchmark_results"
RESULTS_FILE="$RESULTS_DIR/write_results.csv"

S3_BASE="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars"
STAGING_BASE="${S3_BASE}/data/incremental_staging/${SCHEMA_TYPE}"

# Derive short names and paths
TABLE_SHORT=$([ "$TABLE_TYPE" = "COPY_ON_WRITE" ] && echo "cow" || echo "mor")
TABLE_LABEL="${TABLE_SHORT}_${SCHEMA_TYPE}"
TARGET_PATH="${S3_BASE}/data/hudi_${TABLE_LABEL}"
TARGET_TABLE="hudi_${TABLE_LABEL}"

if [ "$SCHEMA_TYPE" = "logical" ]; then
    SOURCE_PATH="${S3_BASE}/data/wide_500cols_10000parts"
    SCHEMA_FILE="${S3_BASE}/full_schema.avsc"
else
    SOURCE_PATH="${S3_BASE}/data/no_logical_500cols_10000parts"
    SCHEMA_FILE="${S3_BASE}/no_logical_schema.avsc"
fi

# MOR-specific inline compaction flags (empty array for COW)
EXTRA_HOODIE_CONFS=()
if [ "$TABLE_TYPE" = "MERGE_ON_READ" ]; then
    EXTRA_HOODIE_CONFS=(
        "--hoodie-conf" "hoodie.compact.inline=true"
        "--hoodie-conf" "hoodie.compact.inline.max.delta.commits=1"
    )
fi

# ── Helpers ───────────────────────────────────────────────────────────────────

# Copy a staged incremental batch into the live source dir so HoodieStreamer
# picks it up on the next run (new files = new checkpoint entries).
stage_batch() {
    local batch_name="$1"
    local dest="${SOURCE_PATH}/${batch_name}"
    echo "  Staging ${batch_name} → ${dest}/"
    aws s3 cp "${STAGING_BASE}/${batch_name}/" "${dest}/" --recursive --quiet
    echo "  Staged."
}

# Run one HoodieStreamer write, time it, and append to the results CSV.
run_write() {
    local write_num="$1"
    local jar_version="$2"

    local bundle_jar="${JARS_DIR}/hudi-spark3.5-bundle_2.12-${jar_version}.jar"
    local slim_jar="${JARS_DIR}/hudi-utilities-slim-spark3.5-bundle_2.12-${jar_version}.jar"
    local label="${TABLE_LABEL}_write${write_num}_${jar_version}"

    echo ""
    echo "────────────────────────────────────────────────────"
    echo " Write ${write_num} | ${TABLE_TYPE} | ${SCHEMA_TYPE} | JAR: ${jar_version}"
    echo "────────────────────────────────────────────────────"

    local start_ts
    start_ts=$(date +%s)

    "$SPARK_HOME/bin/spark-submit" \
        --conf "spark.driver.memory=8g" \
        --conf "spark.executor.memory=6g" \
        --conf "spark.executor.cores=3" \
        --conf "spark.dynamicAllocation.enabled=true" \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.shuffle.partitions=200" \
        --class org.apache.hudi.utilities.streamer.HoodieStreamer \
        --jars "$bundle_jar" \
        "$slim_jar" \
        --table-type "$TABLE_TYPE" \
        --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
        --target-base-path "$TARGET_PATH" \
        --target-table "$TARGET_TABLE" \
        --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
        --hoodie-conf "hoodie.deltastreamer.source.dfs.root=${SOURCE_PATH}" \
        --hoodie-conf "hoodie.deltastreamer.schemaprovider.source.schema.file=${SCHEMA_FILE}" \
        --hoodie-conf "hoodie.deltastreamer.schemaprovider.target.schema.file=${SCHEMA_FILE}" \
        --hoodie-conf "hoodie.datasource.write.recordkey.field=col_1" \
        --hoodie-conf "hoodie.datasource.write.precombine.field=col_1" \
        --hoodie-conf "hoodie.datasource.write.partitionpath.field=partition_col" \
        --source-ordering-field col_1 \
        "${EXTRA_HOODIE_CONFS[@]}"

    local end_ts elapsed
    end_ts=$(date +%s)
    elapsed=$((end_ts - start_ts))

    echo " Write ${write_num} completed in ${elapsed}s"
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),${label},${TABLE_TYPE},${SCHEMA_TYPE},${write_num},${jar_version},${elapsed}" \
        >> "$RESULTS_FILE"
}

# ── Main sequence ─────────────────────────────────────────────────────────────
mkdir -p "$RESULTS_DIR"

# Ensure results CSV has a header
if [ ! -f "$RESULTS_FILE" ]; then
    echo "timestamp,label,table_type,schema_type,write_num,jar_version,elapsed_seconds" \
        > "$RESULTS_FILE"
fi

echo "======================================================"
echo " Write Benchmark: ${TABLE_TYPE} | ${SCHEMA_TYPE}"
echo "======================================================"
echo " Source:  ${SOURCE_PATH}"
echo " Target:  ${TARGET_PATH}"
echo " Schema:  ${SCHEMA_FILE}"
echo ""

# Write 1: full initial load — HoodieStreamer reads all files in SOURCE_PATH
run_write 1 "0.15.0"

# Write 2: stage batch_2 (FGs 1-100), then write with 0.15.0
stage_batch "batch_2"
run_write 2 "0.15.0"

# [Binary upgrade] Write 3: stage batch_3 (FGs 101-200), switch to 0.16.0-SNAPSHOT
echo ""
echo "  >>> Upgrading binary to 0.16.0-SNAPSHOT for writes 3 & 4 <<<"
stage_batch "batch_3"
run_write 3 "0.16.0-SNAPSHOT"

# Write 4: stage batch_4 (FGs 201-300), continue with 0.16.0-SNAPSHOT
stage_batch "batch_4"
run_write 4 "0.16.0-SNAPSHOT"

echo ""
echo "======================================================"
echo " Write benchmark complete: ${TABLE_LABEL}"
echo " Results → ${RESULTS_FILE}"
echo "======================================================"
