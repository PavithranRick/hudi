# Wide Timestamp Benchmark on EMR

This directory contains scripts for running a wide table benchmark with timestamp columns on Amazon EMR.

## Overview

The benchmark creates a Parquet dataset with:
- **500 columns** (including timestamp columns with logical types)
- **10,000 partitions**
- Multiple timestamp representations (millis and micros)
- Incremental batch operations

## Files

- `initial_batch.scala` - Generates the initial dataset with 500 columns
- `incremental_batch_1.py` - Filters and appends specific records
- `incremental_batch_2.py` - Reads and appends all records
- `run_on_emr.sh` - Automated script to run all batches sequentially

## S3 Configuration

- **Bucket**: `s3://performance-benchmark-datasets-us-west-2`
- **Base Path**: `hudi-bench/pavijars`
- **Data Output**: `s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts`

## Prerequisites

1. **EMR Cluster**: Launch an EMR cluster with Spark installed
   - Recommended: EMR 6.x or 7.x
   - Instance types: m5.xlarge or larger for master, m5.2xlarge or larger for core nodes

2. **IAM Permissions**: Ensure the EMR cluster has S3 read/write permissions to the benchmark bucket

3. **S3 Bucket Access**: Verify access to `s3://performance-benchmark-datasets-us-west-2`

## Usage

### Option 1: Run All Batches Automatically

```bash
# From the EMR master node
./run_on_emr.sh
```

This will:
1. Upload scripts to S3
2. Run the initial batch (Scala)
3. Run incremental batch 1 (Python)
4. Run incremental batch 2 (Python)
5. Verify the data in S3

### Option 2: Run Batches Individually

#### Initial Batch (Scala)

```bash
spark-shell -i initial_batch.scala \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4
```

#### Incremental Batch 1 (Python)

```bash
spark-submit \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  incremental_batch_1.py
```

#### Incremental Batch 2 (Python)

```bash
spark-submit \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=8g \
  incremental_batch_2.py
```

## Data Schema

The generated dataset includes:
- **Regular columns**: `col_1` to `col_499` (StringType)
- **Timestamp columns** (every 50th column):
  - Even: `ts_millis_X` (LongType - milliseconds since epoch)
  - Odd: `ts_micros_X` (TimestampType - microsecond precision)
- **Partition column**: `partition_col` (StringType)

## Performance Tuning

Adjust these Spark configurations based on your cluster size:

```bash
--conf spark.driver.memory=4g              # Driver memory
--conf spark.executor.memory=8g            # Executor memory
--conf spark.executor.cores=4              # Cores per executor
--conf spark.dynamicAllocation.enabled=true
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.shuffle.partitions=200
```

## Verification

Check the data in S3:

```bash
aws s3 ls s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts/ \
  --recursive --human-readable --summarize
```

## Troubleshooting

### S3 Access Issues
- Verify IAM role attached to EMR cluster has S3 permissions
- Check bucket policies and access controls

### Out of Memory Errors
- Increase executor memory: `--conf spark.executor.memory=16g`
- Reduce parallelism: `--conf spark.default.parallelism=100`

### Slow Performance
- Enable dynamic allocation: `--conf spark.dynamicAllocation.enabled=true`
- Use larger instance types for core nodes
- Optimize shuffle partitions: `--conf spark.sql.shuffle.partitions=200`

## Notes

- The initial batch generates approximately 10,000 partitions with 500 columns each
- Each incremental batch appends data, doubling the dataset size
- Total data size will vary based on compression and actual values
- Monitor EMR cluster metrics during execution for optimization opportunities
