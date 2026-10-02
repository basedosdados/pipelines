"""Clean the CE PUMD quarter files into the eight microdata tables.

One table per file family (FMLI, MEMI, MTBI, ITBI, FMLD, MEMD, EXPD, DTBD),
hive-partitioned by collection year. File discovery and the one-file-per-quarter
rule live in ``pumd_files.py``.

Values are kept exactly as BLS writes them, apart from three things: surrounding
whitespace is stripped, empty fields and a bare ``.`` become NULL, and NEWID is
split into ``consumer_unit_id`` (all but the last digit) and the interview number
or diary week (the last digit). Column names follow the architecture CSVs:
``original_name`` says which BLS column feeds each output column. A column the
architecture lists but a given file lacks is NULL for that file.

Output is all-STRING parquet (see ``.claude/rules/bigquery-conventions.md``).
Per table and year the script records how many cells were empty or ``.``, which
numeric columns hold unparseable values, and any file column the architecture
does not know; these land in ``<DATA_DIR>/logs/pumd_<table>.json``.

Usage:
    python models/us_bls_cex/code/clean_pumd.py [--tables fmli ...] [--years 2024 ...]
"""

import argparse
import json
import logging
import sys
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_bls_cex.pumd_files import (
    DATA_DIR,
    FAMILIES,
    OUTPUT_DIR,
    QuarterFile,
    selected_quarter_files,
)
from pipelines.datasets.us_bls_cex.utils import (
    normalize_code_array,
    normalized_columns,
    read_arch,
    reset_dir,
    string_schema,
)

LOG_DIR = DATA_DIR / "logs"
NULL = pa.scalar(None, pa.string())
DERIVED = {
    "year",
    "quarter",
    "consumer_unit_id",
    "interview_number",
    "diary_week",
}
# BLS spells the reference-period columns both ways across files.
ALIASES = {
    "ref_yr": "reference_year",
    "refyr": "reference_year",
    "ref_mo": "reference_month",
    "refmo": "reference_month",
}

log = logging.getLogger("clean_pumd")


def source_map(table: str) -> dict[str, str]:
    """BLS column (lowercase) -> architecture column, for non-derived columns."""
    m = {}
    for a in read_arch(table):
        if a["name"] in DERIVED:
            continue
        m[a["original_name"]] = a["name"]
    for src, dst in ALIASES.items():
        if dst in m.values():
            m.setdefault(src, dst)
    return m


def read_quarter(qf: QuarterFile) -> pa.Table:
    """Read one quarter CSV with every column as a string, names lowercased."""
    with zipfile.ZipFile(qf.zip_path) as zf:
        raw = zf.read(qf.member)
    first = raw.split(b"\n", 1)[0].decode("latin-1").strip()
    names = [h.strip().strip('"').strip().lower() for h in first.split(",")]
    if len(set(names)) != len(names):
        raise ValueError(f"{qf.member}: duplicate header names")
    return pacsv.read_csv(
        pa.py_buffer(raw),
        read_options=pacsv.ReadOptions(
            column_names=names, skip_rows=1, encoding="latin-1"
        ),
        convert_options=pacsv.ConvertOptions(
            column_types={n: pa.string() for n in names},
            strings_can_be_null=False,
            quoted_strings_can_be_null=False,
        ),
    )


def clean_quarter(
    qf: QuarterFile, table: str, smap: dict, stats: dict, newid_map: dict
) -> pa.Table:
    """Map one quarter file onto the table's architecture columns."""
    raw = read_quarter(qf)
    n = raw.num_rows
    unknown = [c for c in raw.column_names if c not in smap]
    for c in unknown:
        stats["unknown_columns"][c].append(f"{qf.year}Q{qf.quarter}")
    seen = defaultdict(list)
    for c in raw.column_names:
        if c in smap:
            seen[smap[c]].append(c)
    clash = {k: v for k, v in seen.items() if len(v) > 1}
    if clash:
        raise ValueError(f"{qf.member}: several columns map to one {clash}")

    cleaned = {}
    for c in raw.column_names:
        if c not in smap:
            continue
        col = pc.utf8_trim_whitespace(raw.column(c))  # pyrefly: ignore
        is_empty = pc.equal(col, "")  # pyrefly: ignore
        is_dot = pc.equal(col, ".")  # pyrefly: ignore
        stats["empty_cells"] += pc.sum(is_empty).as_py() or 0  # pyrefly: ignore
        n_dot = pc.sum(is_dot).as_py() or 0  # pyrefly: ignore
        if n_dot:
            stats["dot_cells"][smap[c]] += n_dot
            stats["dot_cells_by_year"][str(qf.year)] += n_dot
        cleaned[smap[c]] = pc.if_else(pc.or_(is_empty, is_dot), NULL, col)  # pyrefly: ignore

    # Unpad all-digit codes so "01" and "1" are one code across years. NEWID
    # gets the same treatment so a consumer unit keeps one id across the
    # padding change; the split below uses the unpadded NEWID.
    for c in normalized_columns(table):
        if c in cleaned:
            cleaned[c] = normalize_code_array(cleaned[c])
    raw_newid = cleaned["newid"]
    if raw_newid.null_count:
        raise ValueError(f"{qf.member}: {raw_newid.null_count} NULL NEWID")
    lengths = pc.utf8_length(raw_newid)  # pyrefly: ignore
    for k in pc.unique(lengths).to_pylist():  # pyrefly: ignore
        stats["newid_lengths"][str(k)] += 1
    newid = normalize_code_array(raw_newid)
    cleaned["newid"] = newid
    if pc.min(pc.utf8_length(newid)).as_py() < 2:  # pyrefly: ignore
        raise ValueError(f"{qf.member}: NEWID shorter than 2 after unpadding")
    pairs = (
        pa.table({"raw": raw_newid, "norm": newid})
        .group_by(["norm", "raw"])
        .aggregate([])
    )
    for norm, raw in zip(
        pairs.column("norm").to_pylist(),
        pairs.column("raw").to_pylist(),
        strict=True,
    ):
        newid_map.setdefault(norm, set()).add(raw)
    cleaned["consumer_unit_id"] = pc.utf8_slice_codeunits(newid, 0, -1)  # pyrefly: ignore
    last = pc.utf8_slice_codeunits(newid, -1)  # pyrefly: ignore
    seq = "interview_number" if table.startswith("interview") else "diary_week"
    cleaned[seq] = last
    cleaned["year"] = pa.array([str(qf.year)] * n, pa.string())
    cleaned["quarter"] = pa.array([str(qf.quarter)] * n, pa.string())

    schema = string_schema(table)
    cols = [cleaned.get(f.name, pa.nulls(n, pa.string())) for f in schema]
    return pa.Table.from_arrays(
        [
            c.combine_chunks() if isinstance(c, pa.ChunkedArray) else c
            for c in cols
        ],
        schema=schema,
    )


def numeric_problems(at: pa.Table, arch: list[dict], stats: dict, year: int):
    """Record non-null values in INT64/FLOAT64 columns that do not parse."""
    for a in arch:
        if a["bigquery_type"] not in ("INT64", "FLOAT64"):
            continue
        col = at.column(a["name"])
        if col.null_count == len(col):
            continue
        s = col.to_pandas()
        bad = s[s.notna() & pd.to_numeric(s, errors="coerce").isna()]
        if len(bad):
            e = stats["numeric_problems"].setdefault(
                a["name"], {"n": 0, "years": [], "examples": []}
            )
            e["n"] += len(bad)
            e["years"].append(year)
            for v in bad.unique()[:5]:
                if len(e["examples"]) < 10 and v not in e["examples"]:
                    e["examples"].append(v)


def clean_family(family: str, years: list[int] | None) -> dict:
    """Build one table from all its selected quarter files, year by year."""
    t0 = time.time()
    table = FAMILIES[family][1]
    arch = read_arch(table)
    smap = source_map(table)
    files = [q for q in selected_quarter_files() if q.family == family]
    if years:
        files = [q for q in files if q.year in years]
    by_year = defaultdict(list)
    for q in files:
        by_year[q.year].append(q)

    stats: dict[str, Any] = {
        "table": table,
        "empty_cells": 0,
        "dot_cells": defaultdict(int),
        "dot_cells_by_year": defaultdict(int),
        "unknown_columns": defaultdict(list),
        "newid_lengths": defaultdict(int),
        "numeric_problems": {},
        "rows": {},
        "files": {},
    }
    newid_map: dict[str, set] = {}
    tdir = OUTPUT_DIR / table
    reset_dir(tdir)
    for year in sorted(by_year):
        parts = []
        for qf in sorted(by_year[year], key=lambda q: q.quarter):
            parts.append(clean_quarter(qf, table, smap, stats, newid_map))
            stats["files"][f"{year}Q{qf.quarter}"] = (
                f"{qf.zip_path.name}:{qf.member}"
            )
        at = pa.concat_tables(parts)
        numeric_problems(at, arch, stats, year)
        pdir = tdir / f"year={year}"
        pdir.mkdir()
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
        stats["rows"][year] = at.num_rows
        log.info(
            f"{table} {year}: {at.num_rows:,} rows ({len(parts)} quarters)"
        )
    collisions = {k: sorted(v) for k, v in newid_map.items() if len(v) > 1}
    stats["newid_distinct"] = len(newid_map)
    stats["newid_collisions"] = len(collisions)
    stats["newid_collision_examples"] = dict(list(collisions.items())[:10])
    stats["seconds"] = round(time.time() - t0, 1)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_DIR / f"pumd_{table}.json", "w") as fh:
        json.dump(stats, fh, indent=1, default=dict)
    return {
        "table": table,
        "rows": sum(stats["rows"].values()),
        "s": stats["seconds"],
    }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", nargs="*", choices=sorted(FAMILIES))
    ap.add_argument("--years", nargs="*", type=int)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()
    families = args.tables or list(FAMILIES)
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(clean_family, f, args.years) for f in families]
        for f in futs:
            log.info(f.result())


if __name__ == "__main__":
    main()
