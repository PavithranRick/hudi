# Hudi Performance Benchmark on EMR - Logical Timestamp Fix Testing

## Overview

This benchmark suite measures the performance impact of Apache Hudi's logical timestamp fix (introduced in version 0.16.0-SNAPSHOT). It compares write and read latencies between the baseline (0.15.0 without the fix) and experimental (0.15.0 → 0.16.0-SNAPSHOT upgrade path with the fix).

### Test Objective

**Primary Goal**: Verify that the logical timestamp fix does not introduce performance regressions in Hudi operations.

**Test Scenarios**:
- **Baseline**: All operations use Hudi 0.15.0 (without logical timestamp fix)
- **Experimental**: Operations start with 0.15.0 and upgrade to 0.16.0-SNAPSHOT (with logical timestamp fix)

### Test Matrix

The benchmark runs **12 test scenarios** (2 configs × 3 table types × 2 scenarios):

**Data Configurations**:
- **Config A**: 500 columns with logical timestamp types (every 50th column)
- **Config B**: 500 columns with ALL string types (no logical timestamps) - control group

**Table Types**:
- COPY_ON_WRITE
- MERGE_ON_READ (without compaction)
- MERGE_ON_READ (with compaction)

**Each test scenario includes**:
1. Write 1: Initial load (10,000 partitions)
2. Write 2: Full upsert/append
3. Write 3: Targeted update (100 file groups out of 10,000)
4. Write 4: Targeted update (100 file groups out of 10,000)
5. Read: Snapshot read with `DISTINCT` query, metadata disabled

## Quick Start

### Step 1: Upload Scripts to S3

```bash
cd /Users/pavithran/sandbox/hudi-dup/testing-logical/perf/emr
aws s3 sync . s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/scripts/ \
  --exclude "*.md" --exclude ".DS_Store" --exclude "*.old"
```

### Step 2: Launch EMR Cluster

```bash
aws emr create-cluster \
  --name "Hudi-Performance-Benchmark" \
  --release-label emr-7.11.0 \
  --applications Name=Spark Name=Hadoop \
  --instance-type m5.xlarge \
  --instance-count 4 \
  --use-default-roles \
  --bootstrap-actions \
    Path=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/scripts/orchestration/emr_bootstrap.sh \
  --ec2-attributes KeyName=your-key-pair
```

### Step 3: Generate Test Data

```bash
# SSH to EMR master
ssh -i your-key.pem hadoop@<master-public-dns>

# Generate data
cd /home/hadoop/benchmark/data_generation
./run_data_generation.sh
```

**Expected time**: 2-3 hours

### Step 4: Run Benchmarks

```bash
cd /home/hadoop/benchmark/orchestration
./run_all_benchmarks.sh
```

**Expected time**: 4-6 hours

### Step 5: Analyze Results

```bash
# Download results
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/results/ ./ --recursive

# Analyze
python3 /home/hadoop/benchmark/results/analyze_results.py metrics_<timestamp>.csv
```

## Directory Structure

```
emr/
├── README.md
├── data_generation/
│   ├── generate_config_a.scala        # Config A: With logical types
│   ├── generate_config_b.scala        # Config B: Without logical types
│   ├── generate_update_batch.py       # Update batch generator
│   └── run_data_generation.sh         # Data generation orchestrator
├── write_ops/
│   ├── hudi_write.sh                  # HoodieStreamer wrapper with timing
│   └── write_config.properties        # Base configuration
├── read_ops/
│   ├── read_benchmark.py              # Read benchmark with timing
│   └── run_read.sh                    # Read wrapper
├── orchestration/
│   ├── emr_bootstrap.sh               # EMR environment setup
│   ├── run_single_test.sh             # Single scenario runner
│   └── run_all_benchmarks.sh          # Main orchestrator (12 tests)
└── results/
    └── analyze_results.py             # Results analyzer
```

## Prerequisites

- **S3 Bucket**: `s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/`
- **EMR 7.11.0** with Spark 3.5.0
- **Hudi Binaries** (already in S3):
  - hudi-spark3.5-bundle_2.12-0.15.0.jar
  - hudi-spark3.5-bundle_2.12-0.16.0-SNAPSHOT.jar
  - hudi-utilities-slim-spark3.5-bundle_2.12-0.15.0.jar
  - hudi-utilities-slim-spark3.5-bundle_2.12-0.16.0-SNAPSHOT.jar
- **Schema**: full_schema.avsc (already in S3)
- **IAM Role**: S3 read/write permissions

## Running Individual Tests

```bash
cd /home/hadoop/benchmark/orchestration

# Example: Config A, COPY_ON_WRITE, Baseline
./run_single_test.sh config_a COPY_ON_WRITE baseline false

# Example: Config A, MERGE_ON_READ with compaction, Experimental
./run_single_test.sh config_a MERGE_ON_READ experimental true
```

## Monitoring

- **Spark UI**: `http://<master-public-dns>:8088`
- **Metrics**: `tail -f /tmp/benchmark_metrics.csv`
- **YARN Logs**: `yarn logs -applicationId <app_id>`

## Results Analysis

The analysis script generates:

1. **Summary Statistics**: Total latency by config, table type, scenario
2. **Baseline vs Experimental Comparison**: Performance deltas
3. **Hudi Version Comparison**: Performance by version
4. **Config A vs Config B Analysis**: Overhead from logical types
5. **Detailed CSV Report**: Full comparison with percentages

### Example Output

```
OVERHEAD ANALYSIS: Config A vs Config B (Experimental)
Average overhead from logical types: 2.15%
✅ Overhead is minimal (<5%), logical type fix has no significant performance impact
```

## Success Criteria

1. ✅ All 12 test scenarios complete successfully
2. ✅ Metrics CSV contains 60 entries (12 tests × 5 operations)
3. ✅ Config B (no logical types) has <5% performance difference
4. ✅ Config A performance acceptable (no regressions > 10%)
5. ✅ Results reproducible

## Troubleshooting

### Out of Memory
- Increase executor memory: `--executor-memory 32g`
- Reduce parallelism: `--num-executors 3`

### S3 Access Denied
- Verify EMR IAM role permissions
- Check bucket policies

### Metrics Not Logged
- Initialize manually: `echo "config_type,table_type,scenario,hudi_version,operation,latency_seconds" > /tmp/benchmark_metrics.csv`

## Cost Optimization

- Use SPOT instances (already configured)
- Terminate cluster after completion
- Run subset first (Config A + COW only)
- **Estimated cost**: ~$15-20 for full run

## S3 Path Structure

```
s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/
├── JARs (hudi-spark3.5-bundle, hudi-utilities-slim)
├── spark-3.5.0-bin-hadoop3.tgz
├── full_schema.avsc
├── scripts/                          # All benchmark scripts
├── data/                              # Test datasets (8 total)
│   ├── raw_config_a_initial/
│   ├── raw_config_a_write2/
│   ├── raw_config_a_update1/
│   ├── raw_config_a_update2/
│   └── raw_config_b_*/
├── tables/                            # Hudi tables (12 total)
│   ├── config_a_COPY_ON_WRITE_baseline/
│   ├── config_a_COPY_ON_WRITE_experimental/
│   └── ...
└── results/                           # Benchmark results
    └── metrics_<timestamp>.csv
```

## References

- **Branch**: `branch-0.x-with-logic_types_fix`
- **Fix Location**: `hudi-common/src/avro/java/org/apache/parquet/schema/AvroSchemaRepair.java`
- **Spark**: 3.5.0
- **EMR**: 7.11.0

---

For detailed configuration options and advanced usage, see inline comments in the scripts.
