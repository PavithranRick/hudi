#!/bin/bash
# run_all_benchmarks.sh
#
# Master orchestrator. Runs all write benchmarks followed by all read benchmarks
# and prints a consolidated summary at the end.
#
# Phase 1 — Write benchmarks (4 scenarios):
#   COW  + logical type
#   MOR  + logical type
#   COW  + no logical type
#   MOR  + no logical type
#
# Phase 2 — Read benchmarks (8 runs: 4 tables × 2 JAR versions):
#   Each table is read once with 0.15.0 JAR and once with 0.16.0-SNAPSHOT JAR.
#
# Usage:
#   ./run_all_benchmarks.sh [--skip-writes] [--skip-reads]
#
#   --skip-writes   Jump straight to Phase 2 (tables must already exist)
#   --skip-reads    Run only Phase 1

set -euo pipefail

SKIP_WRITES=false
SKIP_READS=false
for arg in "$@"; do
    case "$arg" in
        --skip-writes) SKIP_WRITES=true ;;
        --skip-reads)  SKIP_READS=true  ;;
        *) echo "Unknown flag: $arg"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_HOME="/home/hadoop/spark-3.5.0-bin-hadoop3"
JARS_DIR="/home/hadoop/hudi-jars"
RESULTS_DIR="/home/hadoop/benchmark_results"
WRITE_RESULTS="$RESULTS_DIR/write_results.csv"
READ_RESULTS="$RESULTS_DIR/read_results.csv"
LOG_FILE="$RESULTS_DIR/run_$(date +%Y%m%d_%H%M%S).log"

S3_BASE="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars"

mkdir -p "$RESULTS_DIR"

echo "======================================================" | tee -a "$LOG_FILE"
echo " Hudi Performance Benchmark  —  $(date)"             | tee -a "$LOG_FILE"
echo " Log file: $LOG_FILE"                                  | tee -a "$LOG_FILE"
echo "======================================================"| tee -a "$LOG_FILE"

# ── Phase 1: Write benchmarks ─────────────────────────────────────────────────
if [ "$SKIP_WRITES" = false ]; then
    echo "" | tee -a "$LOG_FILE"
    echo "=== PHASE 1: Write Benchmarks ===" | tee -a "$LOG_FILE"

    for TABLE_TYPE in COPY_ON_WRITE MERGE_ON_READ; do
        for SCHEMA_TYPE in logical no_logical; do
            echo "" | tee -a "$LOG_FILE"
            echo ">>> $TABLE_TYPE | $SCHEMA_TYPE" | tee -a "$LOG_FILE"
            "$SCRIPT_DIR/02_write_benchmark.sh" "$TABLE_TYPE" "$SCHEMA_TYPE" \
                2>&1 | tee -a "$LOG_FILE"
        done
    done
else
    echo "" | tee -a "$LOG_FILE"
    echo "=== PHASE 1: Skipped (--skip-writes) ===" | tee -a "$LOG_FILE"
fi

# ── Phase 2: Read benchmarks ──────────────────────────────────────────────────
if [ "$SKIP_READS" = false ]; then
    echo "" | tee -a "$LOG_FILE"
    echo "=== PHASE 2: Read Benchmarks ===" | tee -a "$LOG_FILE"

    for JAR_VERSION in "0.15.0" "0.16.0-SNAPSHOT"; do
        BUNDLE_JAR="${JARS_DIR}/hudi-spark3.5-bundle_2.12-${JAR_VERSION}.jar"

        for TABLE_SHORT in cow mor; do
            for SCHEMA_TYPE in logical no_logical; do
                TABLE_LABEL="${TABLE_SHORT}_${SCHEMA_TYPE}"
                TABLE_PATH="${S3_BASE}/data/hudi_${TABLE_LABEL}"
                LABEL="${TABLE_LABEL}_jar_${JAR_VERSION}"

                echo "" | tee -a "$LOG_FILE"
                echo ">>> Read: $LABEL" | tee -a "$LOG_FILE"

                "$SPARK_HOME/bin/spark-submit" \
                    --conf "spark.driver.memory=8g" \
                    --conf "spark.executor.memory=6g" \
                    --conf "spark.executor.cores=3" \
                    --conf "spark.dynamicAllocation.enabled=true" \
                    --conf "spark.sql.adaptive.enabled=true" \
                    --jars "$BUNDLE_JAR" \
                    "$SCRIPT_DIR/03_read_benchmark.py" \
                    --table-path   "$TABLE_PATH" \
                    --label        "$LABEL" \
                    --schema-type  "$SCHEMA_TYPE" \
                    --results-file "$READ_RESULTS" \
                    2>&1 | tee -a "$LOG_FILE"
            done
        done
    done
else
    echo "" | tee -a "$LOG_FILE"
    echo "=== PHASE 2: Skipped (--skip-reads) ===" | tee -a "$LOG_FILE"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo "" | tee -a "$LOG_FILE"
echo "======================================================" | tee -a "$LOG_FILE"
echo " RESULTS SUMMARY" | tee -a "$LOG_FILE"
echo "======================================================" | tee -a "$LOG_FILE"

if [ -f "$WRITE_RESULTS" ]; then
    echo "" | tee -a "$LOG_FILE"
    echo "Write latencies (seconds):" | tee -a "$LOG_FILE"
    column -t -s',' "$WRITE_RESULTS" | tee -a "$LOG_FILE"
fi

if [ -f "$READ_RESULTS" ]; then
    echo "" | tee -a "$LOG_FILE"
    echo "Read latencies (seconds):" | tee -a "$LOG_FILE"
    column -t -s',' "$READ_RESULTS" | tee -a "$LOG_FILE"
fi

echo "" | tee -a "$LOG_FILE"
echo "Full results:"                       | tee -a "$LOG_FILE"
echo "  Writes: $WRITE_RESULTS"            | tee -a "$LOG_FILE"
echo "  Reads:  $READ_RESULTS"             | tee -a "$LOG_FILE"
echo "  Log:    $LOG_FILE"                 | tee -a "$LOG_FILE"
echo "======================================================" | tee -a "$LOG_FILE"
