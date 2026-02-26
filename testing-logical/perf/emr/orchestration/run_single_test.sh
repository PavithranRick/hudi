#!/bin/bash
# Single Test Runner
# Runs one complete test scenario (baseline OR experimental)
# Executes 4 writes + 1 read

set -e

# Usage check
if [ "$#" -lt 4 ]; then
    echo "Usage: $0 <CONFIG_TYPE> <TABLE_TYPE> <SCENARIO> <ENABLE_COMPACTION>"
    echo ""
    echo "Parameters:"
    echo "  CONFIG_TYPE: config_a or config_b"
    echo "  TABLE_TYPE: COPY_ON_WRITE or MERGE_ON_READ"
    echo "  SCENARIO: baseline or experimental"
    echo "  ENABLE_COMPACTION: true or false (only applies to MERGE_ON_READ)"
    exit 1
fi

# Parameters
CONFIG_TYPE=$1
TABLE_TYPE=$2
SCENARIO=$3
ENABLE_COMPACTION=$4

echo "=========================================="
echo "Running Single Test"
echo "=========================================="
echo "Config: ${CONFIG_TYPE}"
echo "Table Type: ${TABLE_TYPE}"
echo "Scenario: ${SCENARIO}"
echo "Compaction: ${ENABLE_COMPACTION}"
echo "=========================================="

# S3 paths
S3_BASE="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars"
RAW_INITIAL="${S3_BASE}/data/raw_${CONFIG_TYPE}_initial"
RAW_WRITE2="${S3_BASE}/data/raw_${CONFIG_TYPE}_write2"
RAW_UPDATE1="${S3_BASE}/data/raw_${CONFIG_TYPE}_update1"
RAW_UPDATE2="${S3_BASE}/data/raw_${CONFIG_TYPE}_update2"

# Construct table path with compaction suffix if applicable
TABLE_SUFFIX=""
if [ "$TABLE_TYPE" == "MERGE_ON_READ" ] && [ "$ENABLE_COMPACTION" == "true" ]; then
    TABLE_SUFFIX="_compact"
fi

TABLE_PATH="${S3_BASE}/tables/${CONFIG_TYPE}_${TABLE_TYPE}${TABLE_SUFFIX}_${SCENARIO}"

# Script locations
BENCHMARK_DIR="/home/hadoop/benchmark"
WRITE_SCRIPT="${BENCHMARK_DIR}/write_ops/hudi_write.sh"
READ_SCRIPT="${BENCHMARK_DIR}/read_ops/run_read.sh"

# Determine versions based on scenario
if [ "$SCENARIO" == "baseline" ]; then
    # Baseline: All operations use 0.15.0
    WRITE1_VER="0.15.0"
    WRITE2_VER="0.15.0"
    WRITE3_VER="0.15.0"
    WRITE4_VER="0.15.0"
    READ_VER="0.15.0"
else
    # Experimental: Writes 1-2 use 0.15.0, writes 3-4 and read use 0.16.0-SNAPSHOT
    WRITE1_VER="0.15.0"
    WRITE2_VER="0.15.0"
    WRITE3_VER="0.16.0-SNAPSHOT"
    WRITE4_VER="0.16.0-SNAPSHOT"
    READ_VER="0.16.0-SNAPSHOT"
fi

echo ""
echo "Version Plan:"
echo "  Write 1: ${WRITE1_VER}"
echo "  Write 2: ${WRITE2_VER}"
echo "  Write 3: ${WRITE3_VER}"
echo "  Write 4: ${WRITE4_VER}"
echo "  Read: ${READ_VER}"
echo ""

# Write 1: Initial load (10,000 partitions)
echo "=========================================="
echo "Write 1: Initial load (10,000 partitions)"
echo "Version: ${WRITE1_VER}"
echo "=========================================="
$WRITE_SCRIPT "$WRITE1_VER" "$TABLE_TYPE" "$CONFIG_TYPE" \
    "$RAW_INITIAL" "$TABLE_PATH" "write1_$(date +%s)" "$ENABLE_COMPACTION"
echo ""

# Write 2: Full upsert/append
echo "=========================================="
echo "Write 2: Full upsert/append"
echo "Version: ${WRITE2_VER}"
echo "=========================================="
sleep 2  # Small delay to ensure unique checkpoint timestamps
$WRITE_SCRIPT "$WRITE2_VER" "$TABLE_TYPE" "$CONFIG_TYPE" \
    "$RAW_WRITE2" "$TABLE_PATH" "write2_$(date +%s)" "$ENABLE_COMPACTION"
echo ""

# Write 3: Targeted update (100 file groups)
echo "=========================================="
echo "Write 3: Targeted update (100 file groups)"
echo "Version: ${WRITE3_VER}"
echo "=========================================="
sleep 2
$WRITE_SCRIPT "$WRITE3_VER" "$TABLE_TYPE" "$CONFIG_TYPE" \
    "$RAW_UPDATE1" "$TABLE_PATH" "write3_$(date +%s)" "$ENABLE_COMPACTION"
echo ""

# Write 4: Targeted update (100 file groups)
echo "=========================================="
echo "Write 4: Targeted update (100 file groups)"
echo "Version: ${WRITE4_VER}"
echo "=========================================="
sleep 2
$WRITE_SCRIPT "$WRITE4_VER" "$TABLE_TYPE" "$CONFIG_TYPE" \
    "$RAW_UPDATE2" "$TABLE_PATH" "write4_$(date +%s)" "$ENABLE_COMPACTION"
echo ""

# Read: Snapshot read with DISTINCT query
echo "=========================================="
echo "Read: Snapshot read with DISTINCT query"
echo "Version: ${READ_VER}"
echo "=========================================="
$READ_SCRIPT "$READ_VER" "$TABLE_PATH" "$CONFIG_TYPE" "$TABLE_TYPE" "$SCENARIO"
echo ""

echo "=========================================="
echo "✅ Test completed: ${CONFIG_TYPE} / ${TABLE_TYPE} / ${SCENARIO}"
echo "=========================================="
