# Run Hudi Version Migration Test Locally

Step-by-step guide to run `run_hudi_version_migration_test.sh` on your machine.

---

## 1. Prerequisites

- **Java 8 or 11** (match your Hudi/Spark build)
- **Spark 3.4** (for the default test configs: `3.4 0.14.1 0.14.2-SNAPSHOT`)
- **Python** with PySpark (for the read-table step), or ensure `spark-submit` can run PySpark
- **Bash** (script uses `bash`)

---

## 2. Directory layout

Create a local workspace (e.g. under your home or project). The script expects:

```
<DATA_BASE_PATH>/
├── raw1/                    # Parquet files for Step 1 (initial load)
├── raw2/                    # Parquet files for Step 2 (incremental)
└── timestamp/
    ├── schema.avsc          # Avro schema for source/target
    └── timestampdfs-source.properties   # DFS source properties (optional overrides)

<JAR_BASE_PATH>/
├── hudi-spark3.4-bundle_2.12-0.14.1.jar
├── hudi-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar
├── hudi-utilities-slim-spark3.4-bundle_2.12-0.14.1.jar
└── hudi-utilities-slim-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar

<TEST_BASE_PATH>/             # Empty dir; script creates tables here
```

**Script also needs Spark:** it looks for  
`<SPARK_LIBRARIES_PATH>/spark-3.4*-bin-hadoop3`  
(e.g. `/Users/linliu/libraries/spark-3.4*-bin-hadoop3` in the original script).

---

## 3. Set paths in the script

Edit `run_hudi_version_migration_test.sh` and set these for **your** machine:

| Variable | Original | Set to |
|----------|----------|--------|
| `JAR_BASE_PATH` | `/Users/linliu/local_testing/jars` | e.g. `$HOME/hudi_migration_test/jars` |
| `DATA_BASE_PATH` | `/Users/linliu/local_testing/data` | e.g. `$HOME/hudi_migration_test/data` |
| `TEST_BASE_PATH` | `/Users/linliu/local_testing/tests/timestamp_test` | e.g. `$HOME/hudi_migration_test/tests/timestamp_test` |
| Spark discovery | `ls -d /Users/linliu/libraries/spark-${SPARK_VERSION}*-bin-hadoop3` | Path where you have Spark, e.g. `$HOME/spark` or `/opt/spark` |

**Option: use environment variables.** At the top of the script you can do:

```bash
JAR_BASE_PATH="${JAR_BASE_PATH:-$HOME/hudi_migration_test/jars}"
DATA_BASE_PATH="${DATA_BASE_PATH:-$HOME/hudi_migration_test/data}"
TEST_BASE_PATH="${TEST_BASE_PATH:-$HOME/hudi_migration_test/tests/timestamp_test}"
```

Then set `JAR_BASE_PATH`, `DATA_BASE_PATH`, `TEST_BASE_PATH` (and Spark path) in your shell before running.

---

## 4. Create schema and properties

The script passes these Hudi configs:

- `hoodie.datasource.write.recordkey.field=id`
- `hoodie.datasource.write.precombine.field=event_name`
- `hoodie.datasource.write.partitionpath.field=partition`

So your **source Parquet and Avro schema** must include: `id`, `event_name`, `partition`.

### 4.1 `timestamp/schema.avsc`

Create `$DATA_BASE_PATH/timestamp/schema.avsc` (or `data/timestamp/schema.avsc` under your workspace):

```json
{
  "type": "record",
  "name": "TimestampRecord",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "event_name", "type": "string" },
    { "name": "partition", "type": "string" },
    { "name": "payload", "type": "string", "default": "" }
  ]
}
```

### 4.2 `timestamp/timestampdfs-source.properties`

Create `$DATA_BASE_PATH/timestamp/timestampdfs-source.properties`:

```properties
include=base.properties
hoodie.embed.timeline.server=false
```

The script overrides schema paths and DFS root via `--hoodie-conf`, so this file can be minimal.

---

## 5. Generate source Parquet data (raw1 and raw2)

You need Parquet under `raw1/` and `raw2/` with columns `id`, `event_name`, `partition`.

### Option A: PySpark one-liner

From a directory where Spark/PySpark is available:

```bash
export SPARK_HOME=/path/to/spark-3.4.x-bin-hadoop3

$SPARK_HOME/bin/pyspark --master "local[1]" << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("event_name", StringType()),
    StructField("partition", StringType()),
    StructField("payload", StringType())
])

# raw1
df1 = spark.createDataFrame([
    ("1", "evt_a", "p1", "a"),
    ("2", "evt_b", "p1", "b"),
    ("3", "evt_c", "p2", "c"),
])
df1.write.mode("overwrite").parquet("/path/to/data/raw1")

# raw2
df2 = spark.createDataFrame([
    ("4", "evt_d", "p2", "d"),
    ("5", "evt_e", "p2", "e"),
])
df2.write.mode("overwrite").parquet("/path/to/data/raw2")

spark.stop()
EOF
```

Replace `/path/to/data` with your actual `DATA_BASE_PATH` (e.g. `$HOME/hudi_migration_test/data`).

### Option B: Python script (no Spark install for generation)

If you prefer generating Parquet with Pandas + PyArrow (then run the migration script with Spark):

```bash
pip install pyarrow pandas
```

```python
# generate_parquet_data.py
import pyarrow as pa
import pyarrow.parquet as pq
import os

data_root = os.environ.get("DATA_BASE_PATH", "./data")
os.makedirs(f"{data_root}/raw1", exist_ok=True)
os.makedirs(f"{data_root}/raw2", exist_ok=True)

schema = pa.schema([
    ("id", pa.string()),
    ("event_name", pa.string()),
    ("partition", pa.string()),
    ("payload", pa.string()),
])
raw1 = pa.table({
    "id": ["1", "2", "3"],
    "event_name": ["evt_a", "evt_b", "evt_c"],
    "partition": ["p1", "p1", "p2"],
    "payload": ["a", "b", "c"],
}, schema=schema)
raw2 = pa.table({
    "id": ["4", "5"],
    "event_name": ["evt_d", "evt_e"],
    "partition": ["p2", "p2"],
    "payload": ["d", "e"],
}, schema=schema)
pq.write_table(raw1, f"{data_root}/raw1/part-0.parquet")
pq.write_table(raw2, f"{data_root}/raw2/part-0.parquet")
print("Wrote raw1 and raw2")
```

Run: `DATA_BASE_PATH=/path/to/data python generate_parquet_data.py`

---

## 6. Get Hudi JARs

### Option A: Build from Hudi source (for 0.14.2-SNAPSHOT)

From your Hudi repo (e.g. `sandbox/hudi`):

```bash
# Spark 3.4, Scala 2.12
mvn clean package -DskipTests -Dspark3.4 -pl packaging/hudi-spark-bundle,packaging/hudi-utilities-slim-bundle -am
```

Then copy from `packaging/`:

- `hudi-spark-bundle/target/hudi-spark3.4-bundle_2.12-*.jar` → `JAR_BASE_PATH/hudi-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar`
- `hudi-utilities-slim-bundle/target/hudi-utilities-slim-spark3.4-bundle_2.12-*.jar` → `JAR_BASE_PATH/hudi-utilities-slim-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar`

For **0.14.1** you need a 0.14.1 tag/branch build or a released artifact.

### Option B: Download released 0.14.1

From [Apache Hudi releases](https://hudi.apache.org/releases/), get:

- `hudi-spark3.4-bundle_2.12-0.14.1.jar`
- `hudi-utilities-slim-spark3.4-bundle_2.12-0.14.1.jar`

Put them in `JAR_BASE_PATH` with the exact names the script uses (see section 2).

---

## 7. Fix Spark path in the script

The script discovers Spark with:

```bash
SPARK_HOME_DIR=$(ls -d /Users/linliu/libraries/spark-${SPARK_VERSION}*-bin-hadoop3 2>/dev/null | head -1)
```

Change that line to your Spark location, e.g.:

```bash
# Option 1: fixed path
SPARK_HOME_DIR="/opt/spark"

# Option 2: env var (so run_migration_test_local.sh can set SPARK_LIBRARIES_PATH)
SPARK_LIBRARIES_PATH="${SPARK_LIBRARIES_PATH:-$HOME/libraries}"
SPARK_HOME_DIR=$(ls -d "${SPARK_LIBRARIES_PATH}"/spark-${SPARK_VERSION}*-bin-hadoop3 2>/dev/null | head -1)

# Option 3: use SPARK_HOME directly if you already set it
SPARK_HOME_DIR="${SPARK_HOME}"
```

Ensure `$SPARK_HOME_DIR/bin/spark-submit` exists. If you use **Option 2**, you can run with:

```bash
export SPARK_LIBRARIES_PATH=$HOME/libraries   # or /opt/spark, etc.
./run_migration_test_local.sh
```

---

## 8. Run the script

```bash
cd /path/to/where/you/put/the/script
chmod +x run_hudi_version_migration_test.sh

# Optional: override paths without editing script
export JAR_BASE_PATH=$HOME/hudi_migration_test/jars
export DATA_BASE_PATH=$HOME/hudi_migration_test/data
export TEST_BASE_PATH=$HOME/hudi_migration_test/tests/timestamp_test
export LOG_DIR=./logs

./run_hudi_version_migration_test.sh
```

Logs go to `./logs` (or `$LOG_DIR`) with names like:

- `spark3_4_0_14_1_to_0_14_2_SNAPSHOT_COW.log`
- `spark3_4_0_14_1_to_0_14_2_SNAPSHOT_MOR_compaction_true.log`
- `spark3_4_0_14_1_to_0_14_2_SNAPSHOT_MOR_compaction_false.log`

---

## 9. Run a single test (fewer configs)

To run only one combination, edit `TEST_CONFIGS` and comment out the rest, e.g. only COW:

```bash
declare -a TEST_CONFIGS=(
  "3.4 0.14.1 0.14.2-SNAPSHOT COPY_ON_WRITE false"
  # "3.4 0.14.1 0.14.2-SNAPSHOT MERGE_ON_READ true"
  # "3.4 0.14.1 0.14.2-SNAPSHOT MERGE_ON_READ false"
)
```

---

## 10. Troubleshooting

| Issue | What to check |
|-------|-------------------------------|
| `JAR not found` | JAR names must match exactly: `hudi-spark3.4-bundle_2.12-<VERSION>.jar`, same for utilities-slim. Versions in script: `0.14.1`, `0.14.2-SNAPSHOT`. |
| `Could not find Spark 3.4 installation` | Set or fix the `SPARK_HOME_DIR` logic (see section 7). |
| `Partition field not found` | Source Parquet must have column `partition` (and `id`, `event_name`). Regenerate raw1/raw2 with correct schema. |
| PySpark / Python errors in read step | Use same Spark as script (`SPARK_HOME`), and ensure `spark-submit` can run the inline Python script (Python 3 + PySpark on classpath). |
| Checkpoint / no new data | Script uses unique checkpoint keys per step; if you changed it, ensure each step still gets its own `hoodie.deltastreamer.checkpoint.key` and that source folders (raw1 vs raw2) are correct. |

---

## Quick checklist

- [ ] Java 8/11 installed
- [ ] Spark 3.4 extracted and path set in script
- [ ] `JAR_BASE_PATH` with 4 JARs (2 versions × spark-bundle + utilities-slim)
- [ ] `DATA_BASE_PATH` with `raw1/`, `raw2/`, `timestamp/schema.avsc`, `timestamp/timestampdfs-source.properties`
- [ ] Parquet in `raw1` and `raw2` with columns `id`, `event_name`, `partition`
- [ ] `TEST_BASE_PATH` exists (can be empty)
- [ ] Run: `./run_hudi_version_migration_test.sh`
