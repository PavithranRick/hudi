# Step-by-step: Run migration test locally

Follow these steps in order to run `run_hudi_version_migration_test.sh` on your machine. Paths use `$HOME` so no script edits are required.

---

## Step 1: Prerequisites

- **Java 8 or 11** (for Spark/Hudi)
- **Python 3** (for Parquet generation and Spark read step)
- **Bash**

Check:

```bash
java -version
python3 --version
```

---

## Step 2: Create directory layout

The script expects these paths (already set to `$HOME/local_testing/...` and `$HOME/libraries/` in the script):

```bash
mkdir -p "$HOME/local_testing/jars"
mkdir -p "$HOME/local_testing/data/raw1" "$HOME/local_testing/data/raw2" "$HOME/local_testing/data/timestamp"
mkdir -p "$HOME/local_testing/tests/timestamp_test"
mkdir -p "$HOME/libraries"
```

---

## Step 3: Install Spark 3.4 and Spark 3.5 under `$HOME/libraries`

The script discovers Spark by version (e.g. `spark-3.4*-bin-hadoop3`, `spark-3.5*-bin-hadoop3`). Install both so you can run tests for either version.

**Spark 3.4:**

```bash
cd "$HOME/libraries"
curl -sL "https://archive.apache.org/dist/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz" -o spark-3.4.3-bin-hadoop3.tgz
tar xzf spark-3.4.3-bin-hadoop3.tgz
rm spark-3.4.3-bin-hadoop3.tgz
```

**Spark 3.5:**

```bash
cd "$HOME/libraries"
curl -sL "https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz" -o spark-3.5.0-bin-hadoop3.tgz
tar xzf spark-3.5.0-bin-hadoop3.tgz
rm spark-3.5.0-bin-hadoop3.tgz
```

Verify:

```bash
ls "$HOME/libraries/spark-3.4.3-bin-hadoop3/bin/spark-submit"
ls "$HOME/libraries/spark-3.5.0-bin-hadoop3/bin/spark-submit"
```

---

## Step 4: Create schema and properties files

**4.1 Avro schema** — create `$HOME/local_testing/data/timestamp/schema.avsc`:

```bash
cat > "$HOME/local_testing/data/timestamp/schema.avsc" << 'EOF'
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
EOF
```

**4.2 DFS source properties** — create `$HOME/local_testing/data/timestamp/timestampdfs-source.properties`:

```bash
cat > "$HOME/local_testing/data/timestamp/timestampdfs-source.properties" << 'EOF'
include=base.properties
hoodie.embed.timeline.server=false
EOF
```

---

## Step 5: Generate Parquet data (raw1 and raw2)

Install PyArrow, then run the generator (from the **repo root**):

```bash
cd /Users/pavithran/sandbox/hudi-dup
pip install pyarrow
DATA_BASE_PATH=$HOME/local_testing/data python3 scripts/generate_parquet_data.py
```

You should see: `Wrote raw1 and raw2 under /Users/pavithran/local_testing/data` (or your `$HOME/local_testing/data`).

Verify:

```bash
ls "$HOME/local_testing/data/raw1" "$HOME/local_testing/data/raw2"
```

---

## Step 6: Get Hudi JARs

You need **four** JARs in `$HOME/local_testing/jars/` with **exact** names:

| JAR name |
|----------|
| `hudi-spark3.4-bundle_2.12-0.14.1.jar` |
| `hudi-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar` |
| `hudi-utilities-slim-spark3.4-bundle_2.12-0.14.1.jar` |
| `hudi-utilities-slim-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar` |

**0.14.1 (released):**

- Download from [Apache Hudi releases](https://hudi.apache.org/releases/):
  - `hudi-spark3.4-bundle_2.12-0.14.1.jar`
  - `hudi-utilities-slim-spark3.4-bundle_2.12-0.14.1.jar`
- Copy into `$HOME/local_testing/jars/`.

**0.14.2-SNAPSHOT (build from this repo):**

From the **hudi-dup** repo root:

```bash
cd /Users/pavithran/sandbox/hudi-dup
mvn clean package -DskipTests -Dspark3.4 -pl packaging/hudi-spark-bundle,packaging/hudi-utilities-slim-bundle -am
```

Then copy and rename:

```bash
cp packaging/hudi-spark-bundle/target/hudi-spark3.4-bundle_2.12-*.jar \
   "$HOME/local_testing/jars/hudi-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar"
cp packaging/hudi-utilities-slim-bundle/target/hudi-utilities-slim-spark3.4-bundle_2.12-*.jar \
   "$HOME/local_testing/jars/hudi-utilities-slim-spark3.4-bundle_2.12-0.14.2-SNAPSHOT.jar"
```

Verify:

```bash
ls -la "$HOME/local_testing/jars/"
# Should list all 4 JARs.
```

---

## Step 7: Run the migration test script

From the repo root:

```bash
cd /Users/pavithran/sandbox/hudi-dup
chmod +x run_hudi_version_migration_test.sh
./run_hudi_version_migration_test.sh
```

Logs go to `./logs/` with names like:

- `spark3_4_0_14_1_to_0_14_2_SNAPSHOT_COW.log`
- `spark3_4_0_14_1_to_0_14_2_SNAPSHOT_MOR_compaction_true.log`
- `spark3_4_0_14_1_to_0_14_2_SNAPSHOT_MOR_compaction_false.log`

---

## Optional: Run only one test (e.g. COW)

Edit `run_hudi_version_migration_test.sh` and in `TEST_CONFIGS` comment out all but one line:

```bash
declare -a TEST_CONFIGS=(
  "3.4 0.14.1 0.14.2-SNAPSHOT COPY_ON_WRITE false"
  # "3.4 0.14.1 0.14.2-SNAPSHOT MERGE_ON_READ true"
  # "3.4 0.14.1 0.14.2-SNAPSHOT MERGE_ON_READ false"
)
```

---

## Troubleshooting

| Issue | What to do |
|-------|------------|
| `Could not find Spark 3.x` | Ensure the needed Spark version exists under `$HOME/libraries/` (e.g. `spark-3.4.3-bin-hadoop3`, `spark-3.5.0-bin-hadoop3`). Or set `SPARK_HOME` for that run. |
| `JAR not found` | Ensure all 4 JARs are in `$HOME/local_testing/jars/` with the exact names above (Step 6). |
| `Partition field not found` | Regenerate Parquet (Step 5); columns must be `id`, `event_name`, `partition`, `payload`. |
| PySpark / Python errors | Use the same Python that has access to Spark’s PySpark (script uses Spark’s `spark-submit` for the read step). |

---

## Checklist before running

- [ ] Java 8 or 11 installed
- [ ] Spark install(s) under `$HOME/libraries/` (e.g. `spark-3.4.3-bin-hadoop3`, `spark-3.5.0-bin-hadoop3`) with `bin/spark-submit`
- [ ] `$HOME/local_testing/data/timestamp/schema.avsc` and `timestampdfs-source.properties` created
- [ ] `$HOME/local_testing/data/raw1` and `raw2` contain Parquet files (from `generate_parquet_data.py`)
- [ ] All 4 Hudi JARs in `$HOME/local_testing/jars/`
- [ ] `$HOME/local_testing/tests/timestamp_test` exists (can be empty)
- [ ] Run: `./run_hudi_version_migration_test.sh` from repo root
