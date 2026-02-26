#!/bin/bash
# Script to run the timestamp benchmark on EMR cluster
# This script should be run from the EMR master node

set -e

echo "🚀 Starting Wide Timestamp Benchmark on EMR"
echo "================================================"

# Configuration
S3_BUCKET="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars"
DATA_PATH="${S3_BUCKET}/data/wide_500cols_10000parts"
SCRIPTS_PATH="${S3_BUCKET}/scripts"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Step 1: Upload scripts to S3 if not already there
echo -e "${BLUE}📤 Uploading scripts to S3...${NC}"
aws s3 cp initial_batch.scala ${SCRIPTS_PATH}/
aws s3 cp incremental_batch_1.py ${SCRIPTS_PATH}/
aws s3 cp incremental_batch_2.py ${SCRIPTS_PATH}/

# Step 2: Run initial batch
echo -e "${BLUE}🔨 Running initial batch (Scala)...${NC}"
spark-shell -i initial_batch.scala \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true

echo -e "${GREEN}✅ Initial batch completed${NC}"

# Step 3: Run incremental batch 1
echo -e "${BLUE}🔨 Running incremental batch 1 (Python)...${NC}"
spark-submit \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  incremental_batch_1.py

echo -e "${GREEN}✅ Incremental batch 1 completed${NC}"

# Step 4: Run incremental batch 2
echo -e "${BLUE}🔨 Running incremental batch 2 (Python)...${NC}"
spark-submit \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  incremental_batch_2.py

echo -e "${GREEN}✅ Incremental batch 2 completed${NC}"

# Step 5: Verify data in S3
echo -e "${BLUE}📊 Checking data in S3...${NC}"
aws s3 ls ${DATA_PATH}/ --recursive --human-readable --summarize

echo "================================================"
echo -e "${GREEN}✅ All batches completed successfully!${NC}"
echo "📍 Data location: ${DATA_PATH}"
