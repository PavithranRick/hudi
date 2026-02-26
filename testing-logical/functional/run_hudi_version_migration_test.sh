#!/bin/bash

# Script to test HUDI version migration for multiple Spark and HUDI version combinations
# This script runs the old version first to create a table, then runs the new version to read from it

set -e  # Exit on error

# Log directory for individual test case logs
LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "$LOG_DIR"

# Base paths
JAR_BASE_PATH="$HOME/local_testing/jars"
DATA_BASE_PATH="$HOME/local_testing/data"
TEST_BASE_PATH="$HOME/local_testing/tests/timestamp_test"

# Source data folders for each step
# Update these paths to match your actual data folders
# If your data is in raw3, raw4, etc., update these variables accordingly
SOURCE_DATA_STEP1="${DATA_BASE_PATH}/raw1"      # Step 1: initial data
SOURCE_DATA_STEP1_REPEAT="${DATA_BASE_PATH}/raw1"    # Step 1 repeat (MOR): additional data for log files
SOURCE_DATA_STEP2="${DATA_BASE_PATH}/raw2"           # Step 2: incremental data

# Source data folders for each step
# Update these if your data is in different folders
SOURCE_DATA_STEP1="${DATA_BASE_PATH}/raw1"  # Step 1: initial data
SOURCE_DATA_STEP1_REPEAT="${DATA_BASE_PATH}/raw1"  # Step 1 repeat (MOR): additional data for log files
SOURCE_DATA_STEP2="${DATA_BASE_PATH}/raw2"  # Step 2: incremental data

# Test configurations: SPARK_VERSION OLD_HUDI_VERSION NEW_HUDI_VERSION TABLE_TYPE ENABLE_COMPACTION
# For COPY_ON_WRITE tables, ENABLE_COMPACTION is ignored (use "false" as placeholder)
# For MERGE_ON_READ tables, ENABLE_COMPACTION can be "true" or "false"
declare -a TEST_CONFIGS=(
  # "3.3 0.15.0 0.16.0-SNAPSHOT COPY_ON_WRITE false"
  # "3.3 0.15.0 0.16.0-SNAPSHOT MERGE_ON_READ false"
  # "3.3 0.15.0 0.16.0-SNAPSHOT MERGE_ON_READ true"
  # "3.4 0.15.0 1.1.1 COPY_ON_WRITE false"
  # "3.4 0.15.0 1.1.1 MERGE_ON_READ true"
  # "3.4 0.15.0 1.1.1 MERGE_ON_READ false"
  # "3.4 0.15.0 0.16.0-SNAPSHOT COPY_ON_WRITE false"
  # "3.4 0.15.0 0.16.0-SNAPSHOT MERGE_ON_READ true"
  "3.4 0.15.0 0.16.0-SNAPSHOT MERGE_ON_READ false"
  # "3.5 0.15.0 0.16.0-SNAPSHOT COPY_ON_WRITE false"
  # "3.5 0.15.0 0.16.0-SNAPSHOT MERGE_ON_READ true"
  # "3.5 0.15.0 0.16.0-SNAPSHOT MERGE_ON_READ false"
  # "3.4 0.14.1 0.14.2-SNAPSHOT COPY_ON_WRITE false"
  # "3.4 0.14.1 0.14.2-SNAPSHOT MERGE_ON_READ true"
  # "3.4 0.14.1 0.14.2-SNAPSHOT MERGE_ON_READ false"
  # "3.4 0.14.0 0.14.2-SNAPSHOT MERGE_ON_READ true"
  # "3.4 0.14.0 0.14.2-SNAPSHOT MERGE_ON_READ false"
  # "3.4 0.14.0 0.14.2-SNAPSHOT COPY_ON_WRITE false"
)

# Function to run HoodieStreamer with given parameters
run_streamer() {
  local SPARK_CMD=$1
  local SPARK_VERSION=$2
  local HUDI_VERSION=$3
  local TARGET_TABLE=$4
  local SOURCE_DATA_ROOT=$5
  local IS_NEW_VERSION=$6
  local TABLE_TYPE=$7
  local ADDITIONAL_HOODIE_CONF=${8:-""}
  local IGNORE_CHECKPOINT=${9:-""}

  local HUDI_SPARK_BUNDLE_JAR="${JAR_BASE_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar"
  local HUDI_UTILITIES_SLIM_JAR="${JAR_BASE_PATH}/hudi-utilities-slim-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar"

  echo "=========================================="
  echo "Running with Spark ${SPARK_VERSION}, HUDI ${HUDI_VERSION}, Table Type ${TABLE_TYPE}"
  echo "Target table: ${TARGET_TABLE}"
  echo "Is new version: ${IS_NEW_VERSION}"
  echo "=========================================="

  if [ ! -f "$HUDI_SPARK_BUNDLE_JAR" ]; then
    echo "ERROR: JAR not found: $HUDI_SPARK_BUNDLE_JAR"
    return 1
  fi

  if [ ! -f "$HUDI_UTILITIES_SLIM_JAR" ]; then
    echo "ERROR: JAR not found: $HUDI_UTILITIES_SLIM_JAR"
    return 1
  fi

  export HUDI_SPARK_BUNDLE_JAR
  export HUDI_UTILITIES_SLIM_JAR

  # Build the command with checkpoint handling
  # The issue is that ParquetDFSSource uses file modification times as checkpoints
  # Since all steps share the same table, they share checkpoint metadata
  # We need to use a different checkpoint key for each step to isolate them
  # Using a unique checkpoint key per step ensures each step is independent
  local CHECKPOINT_KEY_CONF=""
  if [ -n "$IGNORE_CHECKPOINT" ]; then
    # Use a unique checkpoint key for this step
    # This makes the checkpoint independent from other steps
    CHECKPOINT_KEY_CONF="--hoodie-conf hoodie.deltastreamer.checkpoint.key=$IGNORE_CHECKPOINT"
    # Also set checkpoint to 0 to force reading all files
    CHECKPOINT_CONF="--checkpoint 0"
  fi

  # Build the full command
  # Using --jars with empty SPARK_CONF_DIR to avoid conflicts from spark-defaults.conf
  local CMD_ARGS=(
    --master "local[*]"
    --conf "spark.eventLog.enabled=false"
    --conf "spark.ui.enabled=false"
    --jars "$HUDI_SPARK_BUNDLE_JAR"
    --class org.apache.hudi.utilities.streamer.HoodieStreamer
    "$HUDI_UTILITIES_SLIM_JAR"
    --props "${DATA_BASE_PATH}/timestamp/timestampdfs-source.properties"
    --table-type "${TABLE_TYPE}"
    --source-class org.apache.hudi.utilities.sources.ParquetDFSSource
    --target-base-path "file://${TEST_BASE_PATH}/${TARGET_TABLE}"
    --target-table "${TARGET_TABLE}"
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider
    --hoodie-conf "hoodie.deltastreamer.source.dfs.root=file://${SOURCE_DATA_ROOT}"
    --hoodie-conf "hoodie.deltastreamer.schemaprovider.source.schema.file=file://${DATA_BASE_PATH}/timestamp/schema.avsc"
    --hoodie-conf "hoodie.deltastreamer.schemaprovider.target.schema.file=file://${DATA_BASE_PATH}/timestamp/schema.avsc"
    --hoodie-conf "hoodie.datasource.write.recordkey.field=id"
    --hoodie-conf "hoodie.datasource.write.precombine.field=event_name"
    --hoodie-conf "hoodie.datasource.write.partitionpath.field=partition"
    --hoodie-conf "hoodie.datasource.write.drop.partition.columns=false"
    --hoodie-conf "hoodie.table.partition.fields=partition"
    --hoodie-conf "hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator"
    --hoodie-conf "hoodie.metadata.enable=true"
    --hoodie-conf "hoodie.metadata.index.column.stats.enable=true"
    --hoodie-conf "hoodie.enable.data.skipping=true"
    --source-ordering-field event_name
    --hoodie-conf "hoodie.parquet.small.file.limit=-1"
  )

  # Add checkpoint key if provided
  if [ -n "$CHECKPOINT_KEY_CONF" ]; then
    CMD_ARGS+=($CHECKPOINT_KEY_CONF)
  fi

  # Add checkpoint if provided
  if [ -n "$CHECKPOINT_CONF" ]; then
    CMD_ARGS+=($CHECKPOINT_CONF)
  fi

  # Add additional hoodie-conf if provided
  if [ -n "$ADDITIONAL_HOODIE_CONF" ]; then
    CMD_ARGS+=($ADDITIONAL_HOODIE_CONF)
  fi

  # Execute the command
  $SPARK_CMD "${CMD_ARGS[@]}"

  if [ $? -ne 0 ]; then
    echo "ERROR: Streamer failed for Spark ${SPARK_VERSION}, HUDI ${HUDI_VERSION}, Table Type ${TABLE_TYPE}"
    return 1
  fi

  echo "SUCCESS: Streamer completed for Spark ${SPARK_VERSION}, HUDI ${HUDI_VERSION}, Table Type ${TABLE_TYPE}"
  echo ""
}

# Function to read and print table content
read_table_content() {
  local SPARK_CMD=$1
  local SPARK_VERSION=$2
  local HUDI_VERSION=$3
  local TARGET_TABLE=$4
  local TABLE_TYPE=$5

  local HUDI_SPARK_BUNDLE_JAR="${JAR_BASE_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${HUDI_VERSION}.jar"
  local TABLE_PATH="file://${TEST_BASE_PATH}/${TARGET_TABLE}"

  echo "=========================================="
  echo "Reading table content: ${TARGET_TABLE}"
  echo "=========================================="

  if [ ! -f "$HUDI_SPARK_BUNDLE_JAR" ]; then
    echo "ERROR: JAR not found: $HUDI_SPARK_BUNDLE_JAR"
    return 1
  fi

  # Create a temporary Python script to read and print the table
  # Use macOS-compatible mktemp syntax
  local TEMP_SCRIPT=$(mktemp)
  # Rename to have .py extension for spark-submit
  mv "$TEMP_SCRIPT" "${TEMP_SCRIPT}.py"
  TEMP_SCRIPT="${TEMP_SCRIPT}.py"
  cat > "$TEMP_SCRIPT" <<'PYTHON_EOF'
from pyspark.sql import SparkSession
import sys

if len(sys.argv) < 3:
    print("Usage: script.py <table_path> <table_name>")
    sys.exit(1)

table_path = sys.argv[1]
table_name = sys.argv[2]

spark = SparkSession.builder \
    .appName("ReadHudiTable") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.eventLog.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .config("hoodie.enable.data.skipping", "true") \
    .config("hoodie.metadata.enable", "true") \
    .getOrCreate()

try:
    # Read table
    df = spark.read.format("hudi").load(table_path)
    print("=" * 50)
    print(f"Table: {table_name}")
    row_count = df.count()
    print(f"Row count: {row_count}")
    print("=" * 50)
    
    # Check partition column values
    if "partition" in df.columns:
        print("Partition column statistics:")
        df.select("partition").distinct().show()
        null_count = df.filter(df.partition.isNull()).count()
        print(f"Rows with null partition: {null_count} out of {row_count}")
    
    df.show(100, truncate=False)
    print("=" * 50)
    print("Schema:")
    df.printSchema()
    print("=" * 50)
    
    # Show partition paths
    if "_hoodie_partition_path" in df.columns:
        print("Partition paths in table:")
        df.select("_hoodie_partition_path").distinct().show(truncate=False)
    print("=" * 50)
except Exception as e:
    print(f"ERROR reading table: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
PYTHON_EOF

  $SPARK_CMD \
    --master "local[*]" \
    --conf spark.eventLog.enabled=false \
    --conf spark.ui.enabled=false \
    --jars $HUDI_SPARK_BUNDLE_JAR \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.sql.parquet.enableVectorizedReader=false \
    --conf spark.sql.codegen.wholeStage=false \
    "$TEMP_SCRIPT" "$TABLE_PATH" "$TARGET_TABLE"

  local EXIT_CODE=$?
  rm -f "$TEMP_SCRIPT"

  if [ $EXIT_CODE -ne 0 ]; then
    echo "ERROR: Failed to read table ${TARGET_TABLE}"
    return 1
  fi

  echo ""
}

# Function to check for log files in MOR table
check_log_files() {
  local TARGET_TABLE=$1
  local ENABLE_COMPACTION=$2
  local TABLE_PATH="${TEST_BASE_PATH}/${TARGET_TABLE}"

  echo "=========================================="
  echo "Checking for log files in MOR table: ${TARGET_TABLE}"
  echo "Compaction enabled: ${ENABLE_COMPACTION}"
  echo "=========================================="

  # Check if table path exists
  if [ ! -d "$TABLE_PATH" ]; then
    echo "ERROR: Table path does not exist: $TABLE_PATH"
    return 1
  fi

  # HUDI log files:
  # - Start with a dot (LOG_FILE_PREFIX = ".")
  # - Contain ".log" in the filename (DELTA_EXTENSION = ".log")
  # - Pattern: .<fileId>_<baseCommitTime>.log.<version>_<writeToken>
  # Find log files: files that start with . and contain .log
  local LOG_FILES=$(find "$TABLE_PATH" -type f -name ".*.log*" ! -path "*/.hoodie/*" 2>/dev/null | wc -l | tr -d ' ')

  echo "Found ${LOG_FILES} log file(s) in table"

  if [ "$ENABLE_COMPACTION" = "false" ]; then
    # Compaction is disabled, so log files should exist
    if [ "$LOG_FILES" -eq 0 ]; then
      echo "ERROR: Compaction is disabled but no log files found. Expected log files to exist."
      echo "Table path: $TABLE_PATH"
      echo "Listing files in table (excluding .hoodie directory):"
      find "$TABLE_PATH" -type f ! -path "*/.hoodie/*" | head -30
      echo ""
      echo "Listing partition directories:"
      find "$TABLE_PATH" -type d ! -path "*/.hoodie/*" | head -10
      return 1
    else
      echo "SUCCESS: Log files exist as expected (compaction disabled)"
      echo "Sample log files found:"
      find "$TABLE_PATH" -type f -name ".*.log*" ! -path "*/.hoodie/*" 2>/dev/null | head -5
      return 0
    fi
  else
    # Compaction is enabled, log files may or may not exist (depending on compaction status)
    # But we don't fail if they don't exist since compaction might have already run
    if [ "$LOG_FILES" -gt 0 ]; then
      echo "INFO: Log files found (compaction enabled - they may be compacted in next commit)"
    else
      echo "INFO: No log files found (compaction enabled - may have been compacted)"
    fi
    return 0
  fi
}

# Main execution
for config in "${TEST_CONFIGS[@]}"; do
  read -r SPARK_VERSION OLD_HUDI_VERSION NEW_HUDI_VERSION TABLE_TYPE ENABLE_COMPACTION <<< "$config"

  # Generate log file name for this test case
  # Format: spark3.4_0.15.0_to_0.16.0-SNAPSHOT_COW.log
  # or: spark3.4_0.15.0_to_0.16.0-SNAPSHOT_MOR_compaction_false.log
  OLD_HUDI_VERSION_SANITIZED="${OLD_HUDI_VERSION//\./_}"
  NEW_HUDI_VERSION_SANITIZED="${NEW_HUDI_VERSION//\./_}"
  NEW_HUDI_VERSION_SANITIZED="${NEW_HUDI_VERSION_SANITIZED//-/_}"
  
  if [ "$TABLE_TYPE" = "MERGE_ON_READ" ]; then
    LOG_FILE_NAME="spark${SPARK_VERSION}_${OLD_HUDI_VERSION_SANITIZED}_to_${NEW_HUDI_VERSION_SANITIZED}_MOR_compaction_${ENABLE_COMPACTION}.log"
  else
    LOG_FILE_NAME="spark${SPARK_VERSION}_${OLD_HUDI_VERSION_SANITIZED}_to_${NEW_HUDI_VERSION_SANITIZED}_COW.log"
  fi
  
  TEST_LOG_FILE="${LOG_DIR}/${LOG_FILE_NAME}"
  
  # Run each test case in a subshell with output redirected to its log file
  (
    echo "########################################################################"
    echo "Starting test: Spark ${SPARK_VERSION}, Old HUDI ${OLD_HUDI_VERSION} -> New HUDI ${NEW_HUDI_VERSION}, Table Type ${TABLE_TYPE}"
    if [ "$TABLE_TYPE" = "MERGE_ON_READ" ]; then
      echo "Compaction: ${ENABLE_COMPACTION}"
    fi
    echo "Log file: ${TEST_LOG_FILE}"
    echo "Started at: $(date)"
    echo "########################################################################"
    echo ""

  # Try to use spark-3.x command/alias to set SPARK_HOME
  # First, try to execute it as a command (if it's a script or function)
  SPARK_CMD_NAME="spark-${SPARK_VERSION}"
  
  # Try to get SPARK_HOME by evaluating the alias/command
  # Since spark-3.x aliases set SPARK_HOME, we'll try to extract it
  # Find Spark installation based on version
  # Try to find spark-X.X* directory dynamically
  SPARK_HOME_DIR=$(ls -d $HOME/libraries/spark-${SPARK_VERSION}*-bin-hadoop3 2>/dev/null | head -1)
  if [ -n "$SPARK_HOME_DIR" ] && [ -d "$SPARK_HOME_DIR" ]; then
    export SPARK_HOME="$SPARK_HOME_DIR"
  else
    echo "ERROR: Could not find Spark ${SPARK_VERSION} installation in $HOME/libraries/"
    continue
  fi

  if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: SPARK_HOME directory not found: $SPARK_HOME"
    continue
  fi

  if [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
    echo "ERROR: spark-submit not found in $SPARK_HOME/bin/"
    continue
  fi

  # Create empty Spark config directory to avoid loading spark-defaults.conf
  # This prevents conflicts from user's default Spark configurations
  EMPTY_SPARK_CONF_DIR=$(mktemp -d)
  export SPARK_CONF_DIR="$EMPTY_SPARK_CONF_DIR"
  echo "Using empty SPARK_CONF_DIR: $SPARK_CONF_DIR"

  SPARK_CMD="$SPARK_HOME/bin/spark-submit"
  echo "Using SPARK_HOME: $SPARK_HOME"
  echo "Using Spark command: $SPARK_CMD"

  # Generate unique table name for this combination
  # For MOR tables, include compaction setting in table name
  if [ "$TABLE_TYPE" = "MERGE_ON_READ" ]; then
    COMPACTION_SUFFIX="${ENABLE_COMPACTION}"
    TARGET_TABLE="hudi_table_${SPARK_VERSION}_${OLD_HUDI_VERSION}_to_${NEW_HUDI_VERSION//\./_}_${TABLE_TYPE}_compaction_${COMPACTION_SUFFIX}"
  else
    TARGET_TABLE="hudi_table_${SPARK_VERSION}_${OLD_HUDI_VERSION}_to_${NEW_HUDI_VERSION//\./_}_${TABLE_TYPE}"
  fi
  TARGET_TABLE="${TARGET_TABLE//-/_}"

  # Step 1: Run with old HUDI version (create table)
  # Touch files to update modification time and use unique checkpoint key
  echo "Step 1: Using source data from: ${SOURCE_DATA_STEP1}"
  echo "Step 1: Touching source files to update modification time..."
  find "${SOURCE_DATA_STEP1}" -type f -exec touch {} \; 2>/dev/null || true
  
  # Verify partition field exists in source files (if partition is configured)
  echo "Step 1: Verifying partition field in source files from ${SOURCE_DATA_STEP1}..."
  
  # Create a temporary Python script to check source files
  TEMP_CHECK_SCRIPT=$(mktemp)
  mv "$TEMP_CHECK_SCRIPT" "${TEMP_CHECK_SCRIPT}.py"
  TEMP_CHECK_SCRIPT="${TEMP_CHECK_SCRIPT}.py"
  cat > "$TEMP_CHECK_SCRIPT" <<'PYTHON_EOF'
from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder \
    .appName("CheckPartition") \
    .config("spark.eventLog.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()
try:
    source_path = sys.argv[1]
    print(f"Reading from: {source_path}")
    df = spark.read.parquet(source_path)
    print(f"Total rows: {df.count()}")
    print("Sample data:")
    df.show(5, truncate=False)
    if "partition" in df.columns:
        null_count = df.filter(df.partition.isNull()).count()
        total_count = df.count()
        distinct_values = df.select("partition").distinct().count()
        print(f"Partition field found: total rows={total_count}, null partition rows={null_count}, distinct partition values={distinct_values}")
        if null_count == total_count:
            print("WARNING: All partition values are null!")
        df.select("partition").distinct().show(truncate=False)
    else:
        print("WARNING: Partition field not found in source files!")
    print("Schema:")
    df.printSchema()
except Exception as e:
    print(f"Error checking source files: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
PYTHON_EOF
  
  $SPARK_CMD --master "local[*]" \
    --conf spark.eventLog.enabled=false \
    --conf spark.ui.enabled=false \
    --jars "${JAR_BASE_PATH}/hudi-spark${SPARK_VERSION}-bundle_2.12-${OLD_HUDI_VERSION}.jar" \
    "$TEMP_CHECK_SCRIPT" "${SOURCE_DATA_STEP1}"
  
  # Clean up temporary script
  rm -f "$TEMP_CHECK_SCRIPT"
  
  CHECKPOINT_STEP1="step1_$(date +%s)_${RANDOM}"
  echo "Step 1: Creating table with old HUDI version ${OLD_HUDI_VERSION}..."
  echo "Step 1: Reading from source: ${SOURCE_DATA_STEP1}"
  run_streamer "$SPARK_CMD" "$SPARK_VERSION" "$OLD_HUDI_VERSION" "$TARGET_TABLE" "${SOURCE_DATA_STEP1}" false "$TABLE_TYPE" "" "$CHECKPOINT_STEP1"
  read_table_content "$SPARK_CMD" "$SPARK_VERSION" "$OLD_HUDI_VERSION" "$TARGET_TABLE" "$TABLE_TYPE"

  # For MERGE_ON_READ tables, repeat step 1 once to generate log files
  if [ "$TABLE_TYPE" = "MERGE_ON_READ" ]; then
    echo "Step 1 (repeat for MOR): Using source data from: ${SOURCE_DATA_STEP1_REPEAT}"
    echo "Step 1 (repeat for MOR): Touching source files to update modification time..."
    find "${SOURCE_DATA_STEP1_REPEAT}" -type f -exec touch {} \; 2>/dev/null || true
    CHECKPOINT_STEP1_REPEAT="step1_repeat_$(date +%s)_${RANDOM}"
    
    # Build compaction configuration based on ENABLE_COMPACTION parameter
    # Note: hoodie.compact.inline must be true for MOR tables, but we can control
    # when compaction happens using hoodie.compact.inline.max.delta.commits
    COMPACTION_CONF=""
    if [ "$ENABLE_COMPACTION" = "true" ]; then
      echo "Step 1 (repeat for MOR): Compaction enabled - will compact log files after generation"
      COMPACTION_CONF="--hoodie-conf hoodie.compact.inline=true --hoodie-conf hoodie.compact.inline.max.delta.commits=1"
    else
      echo "Step 1 (repeat for MOR): Compaction effectively disabled - log files will remain uncompacted"
      echo "  (Setting max.delta.commits to 999 to prevent compaction from triggering)"
      COMPACTION_CONF="--hoodie-conf hoodie.compact.inline=true --hoodie-conf hoodie.compact.inline.max.delta.commits=999"
    fi
    
    echo "Step 1 (repeat for MOR): Running again with old HUDI version ${OLD_HUDI_VERSION} to create log files..."
    echo "Step 1 (repeat for MOR): Reading from source: ${SOURCE_DATA_STEP1_REPEAT}"
    run_streamer "$SPARK_CMD" "$SPARK_VERSION" "$OLD_HUDI_VERSION" "$TARGET_TABLE" "${SOURCE_DATA_STEP1_REPEAT}" false "$TABLE_TYPE" "$COMPACTION_CONF" "$CHECKPOINT_STEP1_REPEAT"
    read_table_content "$SPARK_CMD" "$SPARK_VERSION" "$OLD_HUDI_VERSION" "$TARGET_TABLE" "$TABLE_TYPE"
  fi

  # For MERGE_ON_READ tables, check for log files before step 2
  if [ "$TABLE_TYPE" = "MERGE_ON_READ" ]; then
    echo "Checking for log files before Step 2..."
    check_log_files "$TARGET_TABLE" "$ENABLE_COMPACTION"
    if [ $? -ne 0 ]; then
      echo "ERROR: Log file check failed. Aborting test for ${TARGET_TABLE}"
      continue
    fi
  fi

  # Step 2: Run with new HUDI version (read from existing table)
  # Touch files to update modification time and use unique checkpoint key
  echo "Step 2: Using source data from: ${SOURCE_DATA_STEP2}"
  echo "Step 2: Touching source files to update modification time..."
  find "${SOURCE_DATA_STEP2}" -type f -exec touch {} \; 2>/dev/null || true
  CHECKPOINT_STEP2="step2_$(date +%s)_${RANDOM}"
  echo "Step 2: Reading table with new HUDI version ${NEW_HUDI_VERSION}..."
  echo "Step 2: Reading from source: ${SOURCE_DATA_STEP2}"
  run_streamer "$SPARK_CMD" "$SPARK_VERSION" "$NEW_HUDI_VERSION" "$TARGET_TABLE" "${SOURCE_DATA_STEP2}" true "$TABLE_TYPE" "" "$CHECKPOINT_STEP2"
  read_table_content "$SPARK_CMD" "$SPARK_VERSION" "$NEW_HUDI_VERSION" "$TARGET_TABLE" "$TABLE_TYPE"

    echo "########################################################################"
    echo "Completed test: Spark ${SPARK_VERSION}, Old HUDI ${OLD_HUDI_VERSION} -> New HUDI ${NEW_HUDI_VERSION}, Table Type ${TABLE_TYPE}"
    echo "Log file: ${TEST_LOG_FILE}"
    echo "Completed at: $(date)"
    echo "########################################################################"
    echo ""
    echo ""
    
    # Cleanup temporary Spark config directory
    if [ -n "$EMPTY_SPARK_CONF_DIR" ] && [ -d "$EMPTY_SPARK_CONF_DIR" ]; then
      rm -rf "$EMPTY_SPARK_CONF_DIR"
    fi
  ) 2>&1 | tee "$TEST_LOG_FILE"
  
  echo "Test case completed. Log saved to: ${TEST_LOG_FILE}"
  echo ""
done

echo "All tests completed!"
