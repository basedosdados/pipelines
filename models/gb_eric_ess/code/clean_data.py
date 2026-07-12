#!/usr/bin/env python3
"""Clean the ESS integrated Stata files into partitioned Parquet for gb_eric_ess.

One wide table per round (design B). The architecture CSV for each round is the
source of truth for output column order, names, and BigQuery types; this script
maps each raw ESS mnemonic (`original_name`) to its English column name and casts
to the target type.

Storage strategy (per project decision): keep the raw numeric CODES for coded
categorical variables (STRING columns, decoded later via the `dictionary` table),
not the value labels. Stata defined-missing values (.a/.b/.c = No answer /
Refusal / Don't know, and system missing) are read as null. Reading with
`apply_value_formats=False` returns the underlying codes.

Output: output/round_<NN>/year=<YYYY>/data.parquet (snappy, explicit schema).
`year` is the hive partition key and is NOT stored in the parquet payload.

Usage:
    uv run python models/gb_eric_ess/code/clean_data.py            # all rounds present in input/
    uv run python models/gb_eric_ess/code/clean_data.py 1 2 7      # subset
"""

import csv
import glob
import logging
import math
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"
ARCH_DIR = Path(__file__).resolve().parent / "architecture"

YEAR = {
    1: 2002,
    2: 2004,
    3: 2006,
    4: 2008,
    5: 2010,
    6: 2012,
    7: 2014,
    8: 2016,
    9: 2018,
    10: 2020,
    11: 2023,
}

# derived key columns: architecture `name` -> how to source it from the raw file
DERIVED_SOURCE = {"year", "round", "country_id", "respondent_id"}

PA_TYPE = {"INT64": pa.int64(), "FLOAT64": pa.float64(), "STRING": pa.string()}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gb_eric_ess")


def read_architecture(round_n):
    path = ARCH_DIR / f"round_{round_n:02d}.csv"
    with open(path, newline="", encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if r["name"].strip()]


def to_str_val(v):
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v):
            return None
        return str(int(v)) if v.is_integer() else repr(v)
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    return s if s != "" else None


def cast_string(s):
    """Keep raw codes as strings without a spurious '.0'; preserve free text."""
    n_nonnull = s.notna().sum()
    if n_nonnull == 0:
        return pd.array([None] * len(s), dtype="string")
    num = pd.to_numeric(s, errors="coerce")
    fully_numeric = num.notna().sum() == n_nonnull
    if fully_numeric and bool(((num.dropna() % 1) == 0).all()):
        return num.astype("Int64").astype("string")  # "1" not "1.0", <NA> null
    return s.map(to_str_val).astype("string")


def cast_column(series, btype):
    if btype == "INT64":
        # round() matches BigQuery safe_cast(float as int64) for the few INT64
        # measures that carry fractional values (e.g. height in half-cm).
        return pd.to_numeric(series, errors="coerce").round().astype("Int64")
    if btype == "FLOAT64":
        return pd.to_numeric(series, errors="coerce").astype("float64")
    return cast_string(series)


def clean_round(round_n):
    matches = sorted(glob.glob(str(INPUT_DIR / f"ESS{round_n}e*.dta")))
    if not matches:
        log.warning(
            f"round {round_n:02d}: no input file (ESS{round_n}e*.dta) — skipping"
        )
        return None
    dta = matches[0]
    year = YEAR[round_n]
    arch = read_architecture(round_n)
    log.info(
        f"round {round_n:02d}: reading {Path(dta).name} ({len(arch)} arch cols)"
    )

    raw, _ = pyreadstat.read_dta(dta, apply_value_formats=False)
    raw.columns = [c.lower() for c in raw.columns]

    data = {}
    schema_fields = []
    missing_src = []
    for row in arch:
        name = row["name"].strip()
        btype = row["bigquery_type"].strip().upper()
        orig = row["original_name"].strip()
        if name == "year":
            continue  # hive partition key, not stored
        if name == "round":
            col = pd.Series([round_n] * len(raw))
        elif name == "country_id":
            col = raw["cntry"]
        elif name == "respondent_id":
            col = raw["idno"]
        elif orig in raw.columns:
            col = raw[orig]
        else:
            col = pd.Series([None] * len(raw))
            missing_src.append(orig)
        data[name] = cast_column(col, btype)
        schema_fields.append(pa.field(name, PA_TYPE[btype]))

    out = pd.DataFrame(data)
    schema = pa.schema(schema_fields)
    table = pa.Table.from_pandas(out, schema=schema, preserve_index=False)
    table = table.replace_schema_metadata(None)  # drop pandas metadata

    part_dir = OUTPUT_DIR / f"round_{round_n:02d}" / f"year={year}"
    part_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, part_dir / "data.parquet", compression="snappy")

    if missing_src:
        log.warning(
            f"round {round_n:02d}: {len(missing_src)} arch cols absent in raw "
            f"(filled null): {missing_src[:8]}"
        )
    log.info(
        f"round {round_n:02d}: wrote {len(out):,} rows x {out.shape[1]} cols "
        f"-> {part_dir.relative_to(ROOT)}"
    )
    return {"round": round_n, "rows": len(out), "cols": out.shape[1]}


def main():
    rounds = [int(a) for a in sys.argv[1:]] or list(range(1, 12))
    stats = []
    for r in rounds:
        s = clean_round(r)
        if s:
            stats.append(s)
    log.info("=" * 50)
    for s in stats:
        log.info(
            f"  round_{s['round']:02d}: {s['rows']:>7,} rows | {s['cols']} cols"
        )
    log.info(f"  total rows: {sum(s['rows'] for s in stats):,}")


if __name__ == "__main__":
    main()
