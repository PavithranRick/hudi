# Manual Benchmark Guide — COW Table with Logical Timestamp Columns

Measures write and read latency for a Hudi COPY_ON_WRITE table that contains
logical-type timestamp columns (`ts_millis_*` / `ts_micros_*`), comparing
binary 0.15.0 (no fix) vs 0.16.0-SNAPSHOT (with the logical-ts fix).

---

## S3 Paths (reference)

| Resource | S3 Path |
|----------|---------|
| JARs directory | `s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/` |
| Parquet source | `s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts/` |
| Avro schema | `s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc` |
| Hudi COW table (target) | `s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical/` |

Spark home on EMR: `/home/hadoop/spark-3.5.0-bin-hadoop3`

---

## One-time Setup

SSH into the EMR master node and download the JARs locally:

```bash
ssh -i your-key.pem hadoop@<emr-master-dns>

mkdir -p /home/hadoop/hudi-jars
cd /home/hadoop/hudi-jars

aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/hudi-spark3.5-bundle_2.12-0.15.0.jar .
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/hudi-utilities-slim-spark3.5-bundle_2.12-0.15.0.jar .
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/hudi-spark3.5-bundle_2.12-0.16.0-SNAPSHOT.jar .
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/hudi-utilities-slim-spark3.5-bundle_2.12-0.16.0-SNAPSHOT.jar .
```

Copy the benchmark scripts to the master node:

```bash
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/initial_batch.scala \
    /home/hadoop/
aws s3 cp s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/incremental_batch_1.py \
    /home/hadoop/
```

> `initial_batch.scala` was already run and the parquet source exists in S3.
> You do **not** need to run it again unless the source data is missing.

---

## Write Benchmark

The write sequence simulates a real upgrade: 2 commits with the old binary,
then upgrading to the new binary and doing 2 more small commits.

```
Write 1 → 0.15.0 JAR   Full initial load from parquet source (10 K file groups)
Write 2 → 0.15.0 JAR   Append 100 records to source → 100 FG updates
          [upgrade binary → 0.16.0-SNAPSHOT]
Write 3 → 0.16.0-SNAP  Append 100 records to source → 100 FG updates (with fix)
Write 4 → 0.16.0-SNAP  Append 100 records to source → 100 FG updates (with fix)
```

All four commands below share the same target Hudi table. HoodieStreamer stores
a checkpoint inside `.hoodie/` and automatically picks up only the new parquet
files added between runs.

---

### Write 1 — Full initial load with 0.15.0

This reads all 10 K partitions from the parquet source and creates the Hudi table.
Record the wall-clock time.

```bash
time /home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.executor.cores=3 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --jars /home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar \
  /home/hadoop/hudi-jars/hudi-utilities-slim-spark3.5-bundle_2.12-0.15.0.jar \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --target-base-path s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical \
  --target-table hudi_cow_logical \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --hoodie-conf hoodie.deltastreamer.source.dfs.root=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc \
  --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
  --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
  --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col \
  --source-ordering-field col_1
```

**Record:** `real` time from the `time` command output.

---

### Write 2 — Stage 100 records, then write with 0.15.0

First, append 100 records to the parquet source so HoodieStreamer sees new files:

```bash
/home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  /home/hadoop/incremental_batch_1.py
```

> `incremental_batch_1.py` filters the 100 records where
> `col_1 IN ('value_1_1', 'value_2_1', … 'value_100_1')` and appends them
> to the same S3 parquet source directory as new files. Each of those 100 records
> lives in a unique partition → 100 file group updates.

Then run HoodieStreamer (same command as Write 1, still using 0.15.0 JAR):

```bash
time /home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.executor.cores=3 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --jars /home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar \
  /home/hadoop/hudi-jars/hudi-utilities-slim-spark3.5-bundle_2.12-0.15.0.jar \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --target-base-path s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical \
  --target-table hudi_cow_logical \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --hoodie-conf hoodie.deltastreamer.source.dfs.root=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc \
  --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
  --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
  --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col \
  --source-ordering-field col_1
```

**Record:** `real` time.

---

### Write 3 — Upgrade binary, stage 100 records, write with 0.16.0-SNAPSHOT

Stage another 100 records (creates new parquet files in the source directory):

```bash
/home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  /home/hadoop/incremental_batch_1.py
```

Run HoodieStreamer with the **0.16.0-SNAPSHOT** JARs (only the `--jars` and the
application JAR path change):

```bash
time /home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.executor.cores=3 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --jars /home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-0.16.0-SNAPSHOT.jar \
  /home/hadoop/hudi-jars/hudi-utilities-slim-spark3.5-bundle_2.12-0.16.0-SNAPSHOT.jar \
  --table-type COPY_ON_WRITE \
  --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
  --target-base-path s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical \
  --target-table hudi_cow_logical \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --hoodie-conf hoodie.deltastreamer.source.dfs.root=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.source.schema.file=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc \
  --hoodie-conf hoodie.deltastreamer.schemaprovider.target.schema.file=s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/full_schema.avsc \
  --hoodie-conf hoodie.datasource.write.recordkey.field=col_1 \
  --hoodie-conf hoodie.datasource.write.precombine.field=col_1 \
  --hoodie-conf hoodie.datasource.write.partitionpath.field=partition_col \
  --source-ordering-field col_1
```

**Record:** `real` time.

---

### Write 4 — Stage 100 records, write with 0.16.0-SNAPSHOT (repeat)

```bash
/home/hadoop/spark-3.5.0-bin-hadoop3/bin/spark-submit \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  /home/hadoop/incremental_batch_1.py
```

Then run the same 0.16.0-SNAPSHOT HoodieStreamer command from Write 3 above.

**Record:** `real` time.

---

## Read Benchmark

Read the Hudi table as a snapshot with **metadata disabled and data-skipping disabled**
so every file group is read. Run this twice — once per JAR version — to compare latency.

Open `pyspark` (or `spark-shell`) with the relevant JAR, then paste the read code.

### Read A — 0.15.0 JAR (baseline, no fix)

```bash
/home/hadoop/spark-3.5.0-bin-hadoop3/bin/pyspark \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.executor.cores=3 \
  --conf spark.dynamicAllocation.enabled=true \
  --jars /home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-0.15.0.jar
```

Paste in the shell:

```python
import time

hudi_path = "s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical"

spark.sql("SET hoodie.metadata.enable=false")
spark.conf.set("hoodie.enable.data.skipping", "false")

start_time = time.time()

df = spark.read.format("hudi").load(hudi_path)
df.distinct().count()

end_time = time.time()
print(f"Total execution time: {end_time - start_time:.2f} seconds")
```

**Record:** printed elapsed time.

---

### Read B — 0.16.0-SNAPSHOT JAR (experimental, with fix)

```bash
/home/hadoop/spark-3.5.0-bin-hadoop3/bin/pyspark \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=6g \
  --conf spark.executor.cores=3 \
  --conf spark.dynamicAllocation.enabled=true \
  --jars /home/hadoop/hudi-jars/hudi-spark3.5-bundle_2.12-0.16.0-SNAPSHOT.jar
```

Paste the same read code block above.

**Record:** printed elapsed time.

---

## Results Table

Fill in after each step:

| Step | Binary | Operation | Elapsed (s) |
|------|--------|-----------|-------------|
| Write 1 | 0.15.0 | Full load (10 K FGs) | |
| Write 2 | 0.15.0 | 100 FG update | |
| Write 3 | 0.16.0-SNAPSHOT | 100 FG update (after upgrade) | |
| Write 4 | 0.16.0-SNAPSHOT | 100 FG update | |
| Read A | 0.15.0 | Full snapshot scan | |
| Read B | 0.16.0-SNAPSHOT | Full snapshot scan | |

---

## Verify Data After Writes

Check commit history (should show 4 commits):

```bash
aws s3 ls s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical/.hoodie/ \
  | grep ".commit" | sort
```

Check that the incremental parquet files were staged correctly:

```bash
aws s3 ls s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/wide_500cols_10000parts/ \
  --recursive --human-readable --summarize | tail -5
```

The file count should increase by 1 small file each time `incremental_batch_1.py` is run.

---

## Troubleshooting

**HoodieStreamer exits with "no new data"**
The checkpoint already processed all files in the source directory. Re-run
`incremental_batch_1.py` first to add new parquet files, then re-run HoodieStreamer.

**Schema mismatch on Write 3 (after upgrading JAR)**
Expected — the 0.16.0-SNAPSHOT binary migrates `ts-micros` columns to `ts-millis`
internally. The write should succeed; check the commit metadata if it fails.

**Read returns 0 rows**
Verify at least Write 1 has completed and the table path is correct:
```bash
aws s3 ls s3://performance-benchmark-datasets-us-west-2/hudi-bench/pavijars/data/hudi_cow_logical/
```

**Out of memory**
Increase `spark.executor.memory` to `10g` or `spark.driver.memory` to `12g`.
