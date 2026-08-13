"""Cleaning code for world_aiddata_gcdf (AidData GCDF 3.0).

One project/activity-level table (``projects``), 20,985 records, 126 columns.
Reads AidData's analysis-ready workbook (sheet ``GCDF_3.0``), maps its 126
columns BY POSITION to the architecture's target names, casts each column to the
architecture type (STRING / INT64 / FLOAT64 / DATE by arithmetic meaning), and
writes snappy Parquet hive-partitioned by year:

    output/projects/year=<YYYY>/data.parquet

(the partition column ``year`` is encoded in the path, not stored in the file).

Column names, types, units, and order are the single source of truth in
``architecture/gen_architecture.py`` and are imported here — nothing is
duplicated. Scratch data lives OUTSIDE the repo (never under Dropbox), default
``~/Downloads/world_aiddata_gcdf_data`` (override with WORLD_AIDDATA_GCDF_DATA).
"""

import importlib.util
import math
import os
import shutil
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# --- locations -------------------------------------------------------------
DATA_DIR = Path(
    os.environ.get(
        "WORLD_AIDDATA_GCDF_DATA",
        str(Path.home() / "Downloads" / "world_aiddata_gcdf_data"),
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
TABLE_SLUG = "projects"
SHEET = "GCDF_3.0"
EXPECTED_ROWS = 20_985

# --- import the architecture spec (single source of truth) -----------------
_HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "gcdf_arch", _HERE / "architecture" / "gen_architecture.py"
)
arch = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(arch)

COLS = arch.COLS  # source order (workbook col i <-> COLS[i])
OUTPUT_ORDER = arch.OUTPUT_ORDER
TYPE_OF = {c["t"]: c["ty"] for c in COLS}

_ARROW = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}


def find_workbook() -> Path:
    hits = list(INPUT.rglob("*.xlsx"))
    if not hits:
        raise FileNotFoundError(f"No .xlsx under {INPUT}")
    # prefer the main dataset workbook
    hits.sort(
        key=lambda p: (0 if "GlobalChinese" in p.name else 1, len(p.name))
    )
    return hits[0]


def _to_string(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if isinstance(x, float) and x.is_integer():
        x = int(x)  # 740.0 -> "740", never "740.0"
    s = str(x).strip()
    return s or None


def _to_date(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    ts = pd.to_datetime(x, errors="coerce")
    return None if pd.isna(ts) else ts.date()


def build_table() -> pa.Table:
    wb = find_workbook()
    print(f"reading {wb.name} [{SHEET}] ...", flush=True)
    df = pd.read_excel(wb, sheet_name=SHEET, dtype=object)
    # Keep the first 126 columns by position; trailing sheet columns are empty.
    df = df.iloc[:, : len(COLS)]
    assert df.shape[1] == len(COLS), (
        f"expected {len(COLS)} cols, got {df.shape[1]}"
    )
    df.columns = [c["t"] for c in COLS]  # rename by position
    assert len(df) == EXPECTED_ROWS, (
        f"expected {EXPECTED_ROWS} rows, got {len(df)}"
    )

    arrays = {}
    for name in OUTPUT_ORDER:
        ty = TYPE_OF[name]
        s = df[name]
        if ty == "STRING":
            arrays[name] = [_to_string(v) for v in s]
        elif ty == "INT64":
            num = pd.to_numeric(s, errors="coerce").round()
            arrays[name] = [None if pd.isna(v) else int(v) for v in num]
        elif ty == "FLOAT64":
            num = pd.to_numeric(s, errors="coerce")
            arrays[name] = [None if pd.isna(v) else float(v) for v in num]
        elif ty == "DATE":
            arrays[name] = [_to_date(v) for v in s]
        else:
            raise ValueError(f"unknown type {ty} for {name}")

    schema = pa.schema(
        [(name, _ARROW[TYPE_OF[name]]) for name in OUTPUT_ORDER]
    )
    table = pa.table(
        {
            name: pa.array(arrays[name], type=_ARROW[TYPE_OF[name]])
            for name in OUTPUT_ORDER
        },
        schema=schema,
    )
    return table


def write_partitions(table: pa.Table) -> None:
    file_schema = pa.schema([f for f in table.schema if f.name != "year"])
    file_cols = [f.name for f in file_schema]
    target = OUTPUT / TABLE_SLUG
    if target.exists():
        shutil.rmtree(target)
    years = pc.unique(table.column("year")).to_pylist()
    years = sorted(y for y in years if y is not None)
    total = 0
    for y in years:
        mask = pc.equal(table.column("year"), y)
        part = table.filter(mask).select(file_cols)
        part_dir = OUTPUT / TABLE_SLUG / f"year={int(y)}"
        part_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(part, part_dir / "data.parquet", compression="snappy")
        total += part.num_rows
    print(
        f"years {years[0]}-{years[-1]} | wrote {total:,} rows across "
        f"{len(years)} partitions",
        flush=True,
    )
    assert total == EXPECTED_ROWS, f"row total {total} != {EXPECTED_ROWS}"


def main() -> None:
    table = build_table()
    # quick sanity report
    n_iso = pc.count(table.column("country_iso3_code")).as_py()
    print(
        f"rows: {table.num_rows:,} | cols: {table.num_columns} | "
        f"non-null country_iso3_code: {n_iso:,}",
        flush=True,
    )
    write_partitions(table)
    print("done.", flush=True)


if __name__ == "__main__":
    main()
