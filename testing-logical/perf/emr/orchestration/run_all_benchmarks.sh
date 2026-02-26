#!/bin/bash
# Main Orchestrator Script
# Runs all 12 benchmark test scenarios
# Test matrix: 2 configs × 3 table types × 2 scenarios = 12 tests

set -e

echo "=========================================="
echo "Hudi Performance Benchmark Suite"
echo "=========================================="
echo "Total scenarios: 12"
echo "  - 2 data configs (config_a, config_b)"
echo "  - 3 table types (COW, MOR, MOR+compaction)"
echo "  - 2 scenarios per config (baseline, experimental)"
echo ""
echo "Each scenario includes:"
echo "  - 4 write operations"
echo "  - 1 read operation"
echo "=========================================="
echo ""

# Configuration
BENCHMARK_DIR="/home/hadoop/benchmark"
SINGLE_TEST_SCRIPT="${BENCHMARK_DIR}/orchestration/run_single_test.sh"
S3_BASE="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test start time
SUITE_START_TIME=$(date +%s)
echo "⏱️  Start time: $(date)"
echo ""

# Test matrix
CONFIGS=("config_a" "config_b")
TABLE_TYPES=("COPY_ON_WRITE" "MERGE_ON_READ" "MERGE_ON_READ")
COMPACTION_SETTINGS=("false" "false" "true")
TABLE_NAMES=("COW" "MOR" "MOR+Compact")
SCENARIOS=("baseline" "experimental")

# Initialize test counter
TEST_NUM=1
TOTAL_TESTS=12

# Iterate through all combinations
for CONFIG in "${CONFIGS[@]}"; do
    for i in "${!TABLE_TYPES[@]}"; do
        TABLE_TYPE="${TABLE_TYPES[$i]}"
        COMPACTION="${COMPACTION_SETTINGS[$i]}"
        TABLE_NAME="${TABLE_NAMES[$i]}"

        for SCENARIO in "${SCENARIOS[@]}"; do
            echo ""
            echo "=========================================="
            echo -e "${BLUE}Test ${TEST_NUM}/${TOTAL_TESTS}${NC}"
            echo "=========================================="
            echo "Config: ${CONFIG}"
            echo "Table Type: ${TABLE_NAME}"
            echo "Scenario: ${SCENARIO}"
            if [ "$TABLE_TYPE" == "MERGE_ON_READ" ]; then
                echo "Compaction: ${COMPACTION}"
            fi
            echo "=========================================="

            # Record test start time
            TEST_START_TIME=$(date +%s)

            # Run single test
            if $SINGLE_TEST_SCRIPT "$CONFIG" "$TABLE_TYPE" "$SCENARIO" "$COMPACTION"; then
                TEST_END_TIME=$(date +%s)
                TEST_DURATION=$((TEST_END_TIME - TEST_START_TIME))
                echo -e "${GREEN}✅ Test ${TEST_NUM}/${TOTAL_TESTS} completed successfully${NC}"
                echo "⏱️  Test duration: ${TEST_DURATION} seconds"
            else
                echo -e "\033[0;31m❌ Test ${TEST_NUM}/${TOTAL_TESTS} FAILED${NC}"
                echo "Continuing with remaining tests..."
            fi

            TEST_NUM=$((TEST_NUM + 1))
        done
    done
done

# Suite end time
SUITE_END_TIME=$(date +%s)
SUITE_DURATION=$((SUITE_END_TIME - SUITE_START_TIME))

echo ""
echo "=========================================="
echo -e "${GREEN}All benchmarks completed!${NC}"
echo "=========================================="
echo "⏱️  Total duration: ${SUITE_DURATION} seconds ($(($SUITE_DURATION / 60)) minutes)"
echo ""

# Display metrics summary
echo "📊 Metrics collected:"
METRICS_COUNT=$(wc -l < /tmp/benchmark_metrics.csv)
echo "   Total entries: ${METRICS_COUNT} (including header)"
echo "   Expected: 61 (1 header + 60 operations)"
echo ""

# Upload results to S3
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="metrics_${TIMESTAMP}.csv"
S3_RESULTS_PATH="${S3_BASE}/results/${RESULTS_FILE}"

echo "📤 Uploading results to S3..."
aws s3 cp /tmp/benchmark_metrics.csv "${S3_RESULTS_PATH}"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Results uploaded successfully${NC}"
    echo "📍 Location: ${S3_RESULTS_PATH}"
else
    echo -e "\033[0;31m❌ Failed to upload results to S3${NC}"
fi

echo ""
echo "=========================================="
echo "Next Steps"
echo "=========================================="
echo "1. Download results:"
echo "   aws s3 cp ${S3_RESULTS_PATH} ./"
echo ""
echo "2. Analyze results:"
echo "   python3 ${BENCHMARK_DIR}/results/analyze_results.py ${RESULTS_FILE}"
echo ""
echo "3. View raw metrics:"
echo "   cat /tmp/benchmark_metrics.csv"
echo "=========================================="
