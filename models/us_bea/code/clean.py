"""Download + clean all six us_bea tables into partitioned Parquet (bootstrap).

One-shot onboarding bootstrap. The download + row-building transform lives in
``pipelines.datasets.us_bea.utils`` and is imported here rather than duplicated,
so this bootstrap and the recurring flow cannot diverge (DRY). This module keeps
only what is bootstrap-specific: the TYPED parquet writer (the one-shot upload
accepts typed parquet, unlike the pipeline path), and per-BEA-table resume so a
multi-hour county pull can restart where it left off.

Pull strategy (verified against the live API), all in utils:
  - NIPA: one GetData per TableName, Frequency='A,Q,M', Year='ALL'.
  - GDPbyIndustry: per TableID, Frequency A then Q, Industry='ALL', Year='ALL'.
  - Regional: LineCode='ALL' is UNSUPPORTED -> loop line codes, wildcard geo.

Output: <outdir>/<db_table>/year=<YYYY>/<part>.parquet  (typed staging schema).
Resume: a table is skipped if <outdir>/.done/<db_table> exists.

Run:  python -m models.us_bea.code.clean [table ...]   (default: all)
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import sys
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from pipelines.datasets.us_bea.constants import constants
from pipelines.datasets.us_bea.utils import (
    STAGING_SCHEMAS,
    build_dicionario_rows,
    fetch_gi_table,
    fetch_nipa_table,
    fetch_regional_table,
    gi_tables,
    nipa_table_names,
    regional_table_names,
)

OUTDIR = os.environ.get(
    "US_BEA_OUT", os.path.expanduser("~/Downloads/us_bea_data/output")
)

# Staging schema is the single source of truth (shared with the pipeline).
SCHEMAS = STAGING_SCHEMAS


# ----------------------------------------------------------------- writers ---
FLUSH_ROWS = constants.FLUSH_ROWS.value


class Writer:
    """Buffers rows for one db-table, flushing TYPED parquet in chunks to bound
    memory and keep the part-file count reasonable. ``tag`` (the source BEA
    table) is embedded in each part-file name so a partial BEA table can be
    purged and re-run on resume."""

    def __init__(self, table: str):
        self.table = table
        self.buf: list[dict] = []
        self.total = 0
        self.tag = "x"

    def add(self, rows):
        self.buf.extend(rows)
        if len(self.buf) >= FLUSH_ROWS:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        tbl = pa.Table.from_pylist(self.buf, schema=SCHEMAS[self.table])
        ds.write_dataset(
            tbl,
            os.path.join(OUTDIR, self.table),
            format="parquet",
            partitioning=["year"],
            partitioning_flavor="hive",
            existing_data_behavior="overwrite_or_ignore",
            basename_template=f"part-{self.tag}-{int(time.time() * 1000) % 10**9}-{{i}}.parquet",
        )
        self.total += len(self.buf)
        self.buf = []


def _purge_tag(table: str, tag: str):
    for f in glob.glob(
        os.path.join(OUTDIR, table, "**", f"part-{tag}-*.parquet"),
        recursive=True,
    ):
        os.remove(f)


def _progress_path(table):
    return os.path.join(OUTDIR, ".progress", f"{table}.json")


def _progress_load(table):
    p = _progress_path(table)
    return set(json.load(open(p))) if os.path.exists(p) else set()


def _progress_add(table, tag):
    os.makedirs(os.path.dirname(_progress_path(table)), exist_ok=True)
    done = _progress_load(table)
    done.add(tag)
    with open(_progress_path(table), "w") as fh:
        json.dump(sorted(done), fh)


def _reset(table: str):
    shutil.rmtree(os.path.join(OUTDIR, table), ignore_errors=True)
    for p in (os.path.join(OUTDIR, ".done", table), _progress_path(table)):
        if os.path.exists(p):
            os.remove(p)


def _mark_done(table: str, n: int):
    d = os.path.join(OUTDIR, ".done")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, table), "w") as f:
        f.write(str(n))


def _is_done(table: str) -> bool:
    return os.path.exists(os.path.join(OUTDIR, ".done", table))


# ------------------------------------------------------------- table runners --
def run_nipa():
    tabs = nipa_table_names()
    done = _progress_load("nipa")
    print(f"[nipa] {len(tabs)} tables ({len(done)} already done)")
    w = Writer("nipa")
    for i, t in enumerate(tabs, 1):
        if t in done:
            continue
        _purge_tag("nipa", t)
        w.tag = t
        w.add(fetch_nipa_table(t))
        w.flush()
        _progress_add("nipa", t)
        if i % 25 == 0:
            print(f"  [nipa] {i}/{len(tabs)} tables, {w.total:,} rows")
    w.flush()
    _mark_done("nipa", w.total)
    print(f"[nipa] done: {w.total:,} rows")


def run_gdp_by_industry():
    tids = gi_tables()
    done = _progress_load("gdp_by_industry")
    print(f"[gdp_by_industry] {len(tids)} tables")
    w = Writer("gdp_by_industry")
    for x in tids:
        tid = x["key"]
        if tid in done:
            continue
        _purge_tag("gdp_by_industry", tid)
        w.tag = tid
        w.add(fetch_gi_table(tid, (x["desc"] or "").strip()))
        w.flush()
        _progress_add("gdp_by_industry", tid)
    w.flush()
    _mark_done("gdp_by_industry", w.total)
    print(f"[gdp_by_industry] done: {w.total:,} rows")


def _run_regional(table_key, prefixes, geofips, level):
    tabs = regional_table_names(prefixes)
    done = _progress_load(table_key)
    print(
        f"[{table_key}] {len(tabs)} BEA tables ({','.join(prefixes)}) @ GeoFips={geofips} "
        f"({len(done)} already done)"
    )
    w = Writer(table_key)
    for ti, t in enumerate(tabs, 1):
        if t in done:
            print(f"  [{table_key}] {ti}/{len(tabs)} {t}: skip (done)")
            continue
        _purge_tag(table_key, t)
        w.tag = t
        before = w.total + len(w.buf)
        for batch in fetch_regional_table(t, geofips, level):
            w.add(batch)
        got = w.total + len(w.buf) - before
        w.flush()
        _progress_add(table_key, t)
        print(f"  [{table_key}] {ti}/{len(tabs)} {t}: {got:,} rows")
    w.flush()
    _mark_done(table_key, w.total)
    print(f"[{table_key}] done: {w.total:,} rows")


def run_regional_state():
    fam = constants.REGIONAL_FAMILIES.value["regional_state"]
    _run_regional(
        "regional_state", fam["prefixes"], fam["geofips"], fam["level"]
    )


def run_regional_county():
    fam = constants.REGIONAL_FAMILIES.value["regional_county"]
    _run_regional(
        "regional_county", fam["prefixes"], fam["geofips"], fam["level"]
    )


def run_regional_metro():
    fam = constants.REGIONAL_FAMILIES.value["regional_metro"]
    _run_regional(
        "regional_metro", fam["prefixes"], fam["geofips"], fam["level"]
    )


def run_dicionario():
    """Code->label maps for the dictionary-covered columns across all tables."""
    rows = build_dicionario_rows()
    outdir = os.path.join(OUTDIR, "dicionario")
    os.makedirs(outdir, exist_ok=True)
    tbl = pa.Table.from_pylist(rows, schema=SCHEMAS["dicionario"])
    pq.write_table(tbl, os.path.join(outdir, "dicionario.parquet"))
    _mark_done("dicionario", len(rows))
    print(f"[dicionario] done: {len(rows):,} rows")


RUNNERS = {
    "nipa": run_nipa,
    "gdp_by_industry": run_gdp_by_industry,
    "regional_state": run_regional_state,
    "regional_county": run_regional_county,
    "regional_metro": run_regional_metro,
    "dicionario": run_dicionario,
}


def main(argv):
    tables = argv or list(RUNNERS.keys())
    for t in tables:
        if t not in RUNNERS:
            print(f"unknown table {t}; choices: {list(RUNNERS)}")
            continue
        if _is_done(t):
            print(f"[{t}] already done (skip); use RESET=1 to redo")
            continue
        # no reset here: the runner resumes from .progress (per-BEA-table checkpoints).
        RUNNERS[t]()


if __name__ == "__main__":
    if os.environ.get("RESET"):
        for t in sys.argv[1:] or list(RUNNERS):
            _reset(t)
    main(sys.argv[1:])
