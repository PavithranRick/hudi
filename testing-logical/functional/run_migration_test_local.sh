#!/bin/bash
#
# Wrapper to run the Hudi version migration test with local paths.
# Set the variables below for your machine, then run: ./run_migration_test_local.sh
#
# Prerequisites: see HUDI_MIGRATION_TEST_LOCAL_GUIDE.md
#

# --- Set these for your environment ---
export JAR_BASE_PATH="${JAR_BASE_PATH:-$HOME/hudi_migration_test/jars}"
export DATA_BASE_PATH="${DATA_BASE_PATH:-$HOME/hudi_migration_test/data}"
export TEST_BASE_PATH="${TEST_BASE_PATH:-$HOME/hudi_migration_test/tests/timestamp_test}"
export LOG_DIR="${LOG_DIR:-./logs}"

# Spark: script looks for spark-3.4*-bin-hadoop3 under SPARK_LIBRARIES_PATH.
# If your Spark is elsewhere, set SPARK_HOME and we'll patch the script behavior via env.
export SPARK_LIBRARIES_PATH="${SPARK_LIBRARIES_PATH:-$HOME/libraries}"

# Directory where run_hudi_version_migration_test.sh lives
SCRIPT_DIR="${SCRIPT_DIR:-$(dirname "$0")}"
MIGRATION_SCRIPT="${SCRIPT_DIR}/run_hudi_version_migration_test.sh"

if [ ! -f "$MIGRATION_SCRIPT" ]; then
  echo "ERROR: Migration script not found: $MIGRATION_SCRIPT"
  echo "Set SCRIPT_DIR or run this wrapper from the same directory as run_hudi_version_migration_test.sh"
  exit 1
fi

echo "Using:"
echo "  JAR_BASE_PATH=$JAR_BASE_PATH"
echo "  DATA_BASE_PATH=$DATA_BASE_PATH"
echo "  TEST_BASE_PATH=$TEST_BASE_PATH"
echo "  SPARK_LIBRARIES_PATH=$SPARK_LIBRARIES_PATH"
echo "  LOG_DIR=$LOG_DIR"
echo ""

bash "$MIGRATION_SCRIPT"
