#!/bin/bash
# EMR Bootstrap Script for Hudi Performance Benchmark
# Sets up the EMR environment with Spark 3.5 and Hudi JARs

set -e

echo "=========================================="
echo "EMR Bootstrap: Hudi Performance Benchmark"
echo "=========================================="

# Configuration
S3_BUCKET="s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars"
SPARK_VERSION="3.5.0"
SPARK_HOME="/home/hadoop/spark-${SPARK_VERSION}-bin-hadoop3"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

# Step 1: Download and extract Spark 3.5.0
echo -e "${BLUE}Step 1/4: Setting up Spark ${SPARK_VERSION}${NC}"
if [ ! -d "$SPARK_HOME" ]; then
    echo "Downloading Spark ${SPARK_VERSION}..."
    cd /home/hadoop
    aws s3 cp "${S3_BUCKET}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" .
    tar -xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz"
    rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz"
    echo -e "${GREEN}✅ Spark ${SPARK_VERSION} installed${NC}"
else
    echo "Spark ${SPARK_VERSION} already present"
fi

# Set SPARK_HOME environment variable
export SPARK_HOME="${SPARK_HOME}"
echo "export SPARK_HOME=${SPARK_HOME}" >> /home/hadoop/.bashrc

# Step 2: Download Hudi JARs
echo -e "${BLUE}Step 2/4: Downloading Hudi JARs${NC}"
mkdir -p /home/hadoop/hudi_jars
cd /home/hadoop/hudi_jars

HUDI_VERSIONS=("0.15.0" "0.16.0-SNAPSHOT")

for VERSION in "${HUDI_VERSIONS[@]}"; do
    echo "Downloading Hudi ${VERSION} bundles..."

    # Spark bundle
    if [ ! -f "hudi-spark3.5-bundle_2.12-${VERSION}.jar" ]; then
        aws s3 cp "${S3_BUCKET}/hudi-spark3.5-bundle_2.12-${VERSION}.jar" .
        echo "  - hudi-spark3.5-bundle_2.12-${VERSION}.jar ✓"
    else
        echo "  - hudi-spark3.5-bundle_2.12-${VERSION}.jar (already exists)"
    fi

    # Utilities bundle
    if [ ! -f "hudi-utilities-slim-spark3.5-bundle_2.12-${VERSION}.jar" ]; then
        aws s3 cp "${S3_BUCKET}/hudi-utilities-slim-spark3.5-bundle_2.12-${VERSION}.jar" .
        echo "  - hudi-utilities-slim-spark3.5-bundle_2.12-${VERSION}.jar ✓"
    else
        echo "  - hudi-utilities-slim-spark3.5-bundle_2.12-${VERSION}.jar (already exists)"
    fi
done

echo -e "${GREEN}✅ Hudi JARs downloaded${NC}"

# Step 3: Download benchmark scripts
echo -e "${BLUE}Step 3/4: Downloading benchmark scripts${NC}"
mkdir -p /home/hadoop/benchmark
cd /home/hadoop/benchmark

# Download entire scripts directory from S3
aws s3 sync "${S3_BUCKET}/scripts/" /home/hadoop/benchmark/ --exclude "*.md"

# Make all shell scripts executable
find /home/hadoop/benchmark -name "*.sh" -exec chmod +x {} \;
find /home/hadoop/benchmark -name "*.py" -exec chmod +x {} \;

echo -e "${GREEN}✅ Benchmark scripts downloaded${NC}"

# Step 4: Initialize metrics CSV
echo -e "${BLUE}Step 4/4: Initializing metrics collection${NC}"
mkdir -p /home/hadoop/benchmark/results

# Create metrics CSV with header
echo "config_type,table_type,scenario,hudi_version,operation,latency_seconds" > /tmp/benchmark_metrics.csv

echo -e "${GREEN}✅ Metrics CSV initialized${NC}"

# Verification
echo ""
echo "=========================================="
echo "Bootstrap Verification"
echo "=========================================="
echo "Spark Home: ${SPARK_HOME}"
ls -lh ${SPARK_HOME}/bin/spark-submit || echo "WARNING: spark-submit not found"

echo ""
echo "Hudi JARs:"
ls -lh /home/hadoop/hudi_jars/*.jar 2>/dev/null || echo "WARNING: No Hudi JARs found"

echo ""
echo "Benchmark Scripts:"
ls -lh /home/hadoop/benchmark/*.sh 2>/dev/null || echo "INFO: No top-level shell scripts"

echo ""
echo -e "${GREEN}=========================================="
echo "Bootstrap completed successfully!"
echo "==========================================${NC}"
echo ""
echo "Ready to run benchmarks. SSH to master and execute:"
echo "  cd /home/hadoop/benchmark"
echo "  ./run_all_benchmarks.sh"
