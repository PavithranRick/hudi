#!/usr/bin/env python3
"""
Generate Parquet data for run_hudi_version_migration_test.sh.
Requires: pip install pyarrow

Usage:
  DATA_BASE_PATH=$HOME/local_testing/data python scripts/generate_parquet_data.py
  # or from repo root with default:
  python scripts/generate_parquet_data.py
"""
import os
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("Install pyarrow: pip install pyarrow", file=sys.stderr)
    sys.exit(1)

data_root = os.environ.get("DATA_BASE_PATH", os.path.expanduser("~/local_testing/data"))
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
print(f"Wrote raw1 and raw2 under {data_root}")
