#!/bin/bash
# Data Generation Orchestrator
# Generates all required datasets for the benchmark

set -e

echo "=========================================="
echo "Hudi Benchmark Data Generation"
echo "=========================================="

# Configuration
S3_BASE="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data"
SPARK_HOME="${SPARK_HOME:-/home/hadoop/spark-3.5.0-bin-hadoop3}"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}Step 1/8: Generating Config A initial data (with logical timestamp columns)${NC}"
$SPARK_HOME/bin/spark-shell \
  -i "${SCRIPTS_DIR}/generate_config_a.scala" \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g

echo -e "${GREEN}✅ Config A initial data generated${NC}"
echo ""

echo -e "${BLUE}Step 2/8: Generating Config B initial data (without logical timestamp columns)${NC}"
$SPARK_HOME/bin/spark-shell \
  -i "${SCRIPTS_DIR}/generate_config_b.scala" \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g

echo -e "${GREEN}✅ Config B initial data generated${NC}"
echo ""

echo -e "${BLUE}Step 3/8: Generating Config A write2 batch (full dataset)${NC}"
$SPARK_HOME/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g \
  "${SCRIPTS_DIR}/generate_update_batch.py" \
  "${S3_BASE}/raw_config_a_initial" \
  "${S3_BASE}/raw_config_a_write2" \
  "write2"

echo -e "${GREEN}✅ Config A write2 batch generated${NC}"
echo ""

echo -e "${BLUE}Step 4/8: Generating Config A update1 batch (100 file groups)${NC}"
$SPARK_HOME/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g \
  "${SCRIPTS_DIR}/generate_update_batch.py" \
  "${S3_BASE}/raw_config_a_initial" \
  "${S3_BASE}/raw_config_a_update1" \
  "update1" \
  100

echo -e "${GREEN}✅ Config A update1 batch generated${NC}"
echo ""

echo -e "${BLUE}Step 5/8: Generating Config A update2 batch (100 file groups)${NC}"
$SPARK_HOME/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g \
  "${SCRIPTS_DIR}/generate_update_batch.py" \
  "${S3_BASE}/raw_config_a_initial" \
  "${S3_BASE}/raw_config_a_update2" \
  "update2" \
  100

echo -e "${GREEN}✅ Config A update2 batch generated${NC}"
echo ""

echo -e "${BLUE}Step 6/8: Generating Config B write2 batch (full dataset)${NC}"
$SPARK_HOME/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g \
  "${SCRIPTS_DIR}/generate_update_batch.py" \
  "${S3_BASE}/raw_config_b_initial" \
  "${S3_BASE}/raw_config_b_write2" \
  "write2"

echo -e "${GREEN}✅ Config B write2 batch generated${NC}"
echo ""

echo -e "${BLUE}Step 7/8: Generating Config B update1 batch (100 file groups)${NC}"
$SPARK_HOME/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g \
  "${SCRIPTS_DIR}/generate_update_batch.py" \
  "${S3_BASE}/raw_config_b_initial" \
  "${S3_BASE}/raw_config_b_update1" \
  "update1" \
  100

echo -e "${GREEN}✅ Config B update1 batch generated${NC}"
echo ""

echo -e "${BLUE}Step 8/8: Generating Config B update2 batch (100 file groups)${NC}"
$SPARK_HOME/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=16g \
  "${SCRIPTS_DIR}/generate_update_batch.py" \
  "${S3_BASE}/raw_config_b_initial" \
  "${S3_BASE}/raw_config_b_update2" \
  "update2" \
  100

echo -e "${GREEN}✅ Config B update2 batch generated${NC}"
echo ""

echo "=========================================="
echo -e "${GREEN}All data generation completed!${NC}"
echo "=========================================="
echo ""
echo "Verifying data in S3..."
aws s3 ls ${S3_BASE}/ --recursive --human-readable --summarize | tail -20

echo ""
echo -e "${YELLOW}Expected datasets:${NC}"
echo "  - raw_config_a_initial"
echo "  - raw_config_a_write2"
echo "  - raw_config_a_update1"
echo "  - raw_config_a_update2"
echo "  - raw_config_b_initial"
echo "  - raw_config_b_write2"
echo "  - raw_config_b_update1"
echo "  - raw_config_b_update2"
