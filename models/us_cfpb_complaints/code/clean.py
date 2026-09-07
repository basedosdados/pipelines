"""Clean the CFPB Consumer Complaint Database bulk export into staging parquet.

Reads the 9.3 GB single-file CSV export with Python's ``csv`` module — the narratives
contain embedded newlines inside quoted fields, which DuckDB's parallel reader
desynchronises on — and streams it out as Snappy parquet hive-partitioned by year.

Every column is written as STRING. Staging is all-STRING by Data Basis convention:
``pipelines.utils.gcs.dump_header`` stringifies the header file BigQuery infers the
staging schema from, so typed parquet is rejected once the recurring pipeline writes
to the same staging dataset. The dbt model ``safe_cast``s each column to its
architecture type. Values are cast to string by writing them as text, never by
``astype(str)``, which would turn a NULL into the literal ``"nan"``.

The transform is deliberately narrow: it renames columns to the architecture, derives
``year`` and ``state_id``, trims whitespace and maps empty strings to NULL. It does
not reclassify product/issue values across the 2017 and 2023 taxonomy revisions.
"""

import argparse
import csv
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from common import COMPLAINT, CSV_NAME, INPUT, OUTPUT, load_cols, state_id_for

csv.field_size_limit(sys.maxsize)

# Rows buffered per year before a row group is flushed. Bounds peak RAM at roughly
# (n_years x FLUSH_ROWS) rows held as Python strings.
FLUSH_ROWS = 50_000


def clean_complaint(
    csv_path: Path, output_dir: Path, limit: int | None = None
) -> dict:
    """Stream the bulk CSV into ``<output_dir>/complaint/year=<YYYY>/data.parquet``.

    Args:
        csv_path: The unzipped ``complaints.csv``.
        output_dir: Root output directory.
        limit: Stop after this many source records (for smoke tests).

    Returns:
        Counters describing the run: rows per year and data-quality tallies.
    """
    cols = load_cols(COMPLAINT)
    order = [c.name for c in cols]
    schema = pa.schema([pa.field(n, pa.string()) for n in order])
    # architecture column -> source header, for the columns read straight through
    src_of = {c.name: c.original for c in cols}

    tdir = output_dir / COMPLAINT
    tdir.mkdir(parents=True, exist_ok=True)

    writers: dict[str, pq.ParquetWriter] = {}
    buf: dict[str, list[list]] = defaultdict(list)
    per_year = Counter()
    stats = Counter()
    t0 = time.time()

    def flush(year: str) -> None:
        rows = buf[year]
        if not rows:
            return
        arrays = [
            pa.array([r[i] for r in rows], type=pa.string())
            for i in range(len(order))
        ]
        table = pa.Table.from_arrays(arrays, schema=schema)
        w = writers.get(year)
        if w is None:
            pdir = tdir / f"year={year}"
            pdir.mkdir(parents=True, exist_ok=True)
            w = pq.ParquetWriter(
                pdir / "data.parquet", schema, compression="snappy"
            )
            writers[year] = w
        w.write_table(table)
        buf[year] = []

    n = 0
    with open(csv_path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = [
            c.original
            for c in cols
            if c.original and c.original not in (reader.fieldnames or [])
        ]
        if missing:
            raise SystemExit(
                f"source CSV is missing expected columns: {missing}\n"
                f"header seen: {reader.fieldnames}"
            )
        extra = [
            f
            for f in (reader.fieldnames or [])
            if f not in set(src_of.values())
        ]
        if extra:
            # Not fatal, but never silent: a new source column must be a decision.
            print(f"WARNING: source columns not in the architecture: {extra}")

        for rec in reader:
            n += 1
            date_received = (rec["Date received"] or "").strip()
            year = date_received[:4]
            if len(date_received) != 10 or not year.isdigit():
                stats["bad_date_received"] += 1
                continue

            published_state = (rec["State"] or "").strip()
            sid = state_id_for(published_state)
            if published_state and sid is None:
                stats["state_without_fips"] += 1

            out = []
            for c in cols:
                if c.name == "year":
                    out.append(year)
                elif c.name == "state_id":
                    out.append(sid)
                else:
                    v = rec[c.original]
                    v = v.strip() if v is not None else ""
                    out.append(v if v != "" else None)
            buf[year].append(out)
            per_year[year] += 1
            if len(buf[year]) >= FLUSH_ROWS:
                flush(year)
            if n % 2_000_000 == 0:
                print(
                    f"  ...{n:,} rows  ({time.time() - t0:.0f}s)", flush=True
                )
            if limit and n >= limit:
                break

    for year in list(buf):
        flush(year)
    for w in writers.values():
        w.close()

    stats["source_rows"] = n
    stats["written_rows"] = sum(per_year.values())
    return {"per_year": dict(per_year), "stats": dict(stats)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", type=Path, default=INPUT / CSV_NAME)
    ap.add_argument("--output", type=Path, default=OUTPUT)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    res = clean_complaint(args.csv, args.output, args.limit)
    print("\n=== ROWS PER YEAR ===")
    for y in sorted(res["per_year"]):
        print(f"  {y}  {res['per_year'][y]:>10,}")
    print("\n=== STATS ===")
    for k, v in sorted(res["stats"].items()):
        print(f"  {k:24s} {v:>12,}")


if __name__ == "__main__":
    main()
