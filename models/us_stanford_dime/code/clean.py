"""Clean the DIME v4.0 source files into staging Parquet.

The transform is deliberately thin: rename source columns to the architecture
names, normalise the source's several spellings of "missing" to real NULLs, and
emit every column as a STRING.

Staging is all-STRING by house convention (.claude/rules/bigquery-conventions.md)
and the dbt models ``safe_cast`` each column to its architecture type. Two
details are load-bearing and easy to get wrong:

* Cast through the column's real type *first*, then to VARCHAR. Casting the raw
  text straight across would leave ``1982.0`` where the year belongs and
  ``safe_cast(... as int64)`` would return NULL.
* Never use a pandas ``astype(str)``-style cast, which renders NULL as the
  literal string ``nan`` that ``safe_cast`` will not turn back into NULL. DuckDB's
  ``CAST(... AS VARCHAR)`` keeps NULL as NULL.

The contribution files are large — 202.9M rows for the 2024 cycle alone — so
each cycle is written as numbered part files that ``upload.py`` can push to GCS
and delete one at a time, keeping peak local disk to a few GB.
"""

from __future__ import annotations

import argparse
import gzip
import os
import shlex
import subprocess
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import architecture as arch

SCRATCH = Path(
    os.environ.get(
        "DIME_DATA_DIR", Path.home() / "Downloads" / "us_stanford_dime_data"
    )
)
INPUT = SCRATCH / "input"
OUTPUT = SCRATCH / "output"

# Election cycles published as separate contribution files.
CYCLES = list(range(1980, 2025, 2))

# The only missing-value spelling the source uses besides the empty string.
# Verified by scanning whole CSV fields across cycles: "NA", "NULL", "." and
# "-" never appear on their own, so treating them as NULL would silently
# destroy real values rather than clean them.
NULL_TOKENS = ("\\N",)

# Target size of one output part file. DuckDB splits the COPY in a single
# pass, so this never costs an extra scan of the source.
PART_SIZE = "900mb"


def _connect(
    memory_limit: str | None = None, threads: int | None = None
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection configured for out-of-core work.

    ``preserve_insertion_order=false`` is required: without it a COPY over a
    multi-GB CSV buffers the whole scan and RAM climbs into the tens of GB.

    The limits default to 6 GB and 4 threads and are overridable through
    ``DIME_DUCKDB_MEMORY`` and ``DIME_DUCKDB_THREADS``, so a second job can be
    run alongside the backfill on a machine that cannot afford two full-size
    DuckDB processes.
    """
    memory_limit = memory_limit or os.environ.get("DIME_DUCKDB_MEMORY", "6GB")
    threads = threads or int(os.environ.get("DIME_DUCKDB_THREADS", "4"))
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET threads={threads}")
    con.execute(f"SET temp_directory='{SCRATCH / 'duckdb_tmp'}'")
    return con


def _select_expr(table: str) -> str:
    """Build the SELECT list mapping source columns to architecture columns.

    Every column comes out as VARCHAR, having first been routed through its
    architecture type so numbers and dates serialise in their canonical form.
    """
    tokens = ", ".join(f"'{t}'" for t in NULL_TOKENS)
    parts = []
    for col in arch.TABLES[table]:
        name, bq_type, original = col[0], col[1], col[9]
        if not original or "<" in original:
            continue
        src = f'"{original}"'
        clean = (
            f"case when trim({src}) in ({tokens}) then null "
            f"else nullif(trim({src}), '') end"
        )
        if bq_type == "INT64":
            expr = f"cast(try_cast(try_cast({clean} as double) as bigint) as varchar)"
        elif bq_type == "FLOAT64":
            expr = f"cast(try_cast({clean} as double) as varchar)"
        elif bq_type == "DATE":
            expr = f"cast(try_cast({clean} as date) as varchar)"
        else:
            expr = clean
        parts.append(f"{expr} as {name}")
    return ",\n    ".join(parts)


def _read_csv(path: Path, columns: list[str], parallel: bool = True) -> str:
    """Return a DuckDB ``read_csv`` call that types every column as VARCHAR.

    ``ignore_errors`` is deliberately left off. It would silently drop malformed
    rows, and on 861M records a quiet loss is worse than a loud failure — row
    counts are checked against the codebook instead. ``null_padding`` is also
    off: the recipient file contains quoted newlines, which the parallel scanner
    refuses to combine with padding, and the fixed ``columns`` mapping already
    pins the schema.

    ``parallel`` must be off for sources containing quoted newlines inside a
    field; DuckDB's parallel scanner cannot split those safely and says so
    rather than mis-parsing.
    """
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in columns)
    return (
        f"read_csv('{path}', header=true, columns={{{cols}}}, "
        "quote='\"', escape='\"', strict_mode=false"
        + ("" if parallel else ", parallel=false")
        + ")"
    )


def sanitize_source(path: Path) -> int:
    """Strip invalid UTF-8 byte sequences from a gzipped source file, in place.

    A handful of records in the DIME contribution files carry stray bytes that
    are not valid UTF-8 — one line in the 1990 cycle, for instance, has 0xE3 0x9D
    inside a city name. DuckDB refuses the whole file over it, and its latin-1
    mode rejects the same bytes, so the file has to be repaired before it can be
    read.

    ``iconv -c`` drops only the invalid sequences and leaves valid multi-byte
    characters intact, which matters because the files are otherwise almost
    pure ASCII with a few genuine accented characters. Returns the number of
    lines that were not valid UTF-8, counted on the way through.

    The rewritten file is recompressed at gzip -1 for speed, so it is typically
    larger than the original; that is expected and says nothing about how much
    was repaired.
    """
    repaired = path.with_suffix(".gz.repaired")
    cmd = f"gzip -dc {shlex.quote(str(path))} | iconv -f UTF-8 -t UTF-8 -c | gzip -1"
    with repaired.open("wb") as out:
        proc = subprocess.run(cmd, shell=True, stdout=out)
    if proc.returncode != 0:
        repaired.unlink(missing_ok=True)
        raise RuntimeError(f"failed to sanitize {path}")
    bad = 0
    with gzip.open(path, "rb") as fh:
        for line in fh:
            try:
                line.decode("utf-8")
            except UnicodeDecodeError:
                bad += 1
    repaired.replace(path)
    return bad


def _source_header(path: Path) -> list[str]:
    """Read the CSV header out of a gzipped source file."""
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
        import csv as _csv

        return next(_csv.reader(fh))


def _copy_split(
    con: duckdb.DuckDBPyConnection, out_dir: Path, prefix: str
) -> list[Path]:
    """COPY the ``clean`` view into ``out_dir`` as size-capped part files."""
    con.execute(
        f"copy (select * from clean) to '{out_dir}' "
        f"(format parquet, compression snappy, file_size_bytes '{PART_SIZE}')"
    )
    written = sorted(out_dir.glob("*.parquet"))
    renamed = []
    for i, f in enumerate(written):
        dest = out_dir / f"{prefix}_{i:03d}.parquet"
        if f != dest:
            f.rename(dest)
        renamed.append(dest)
    return renamed


def _prepare(
    con: duckdb.DuckDBPyConnection,
    table: str,
    src: Path,
    parallel: bool = True,
) -> None:
    """Register the ``src`` and ``clean`` views for one source file."""
    header = _source_header(src)
    con.execute(
        "create or replace view src as select * from "
        + _read_csv(src, header, parallel=parallel)
    )
    con.execute(
        f"create or replace view clean as select\n    {_select_expr(table)}\nfrom src"
    )


def clean_contribution(cycle: int) -> tuple[int, list[Path]]:
    """Clean one election cycle of contribution records."""
    src = INPUT / f"contribDB_{cycle}.csv.gz"
    if not src.exists():
        raise FileNotFoundError(f"missing source file {src}")
    out_dir = OUTPUT / "contribution" / str(cycle)
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob("*.parquet"):
        stale.unlink()

    con = _connect()
    _prepare(con, "contribution", src)
    files = _copy_split(con, out_dir, f"contribution_{cycle}")
    total = con.execute(
        f"select count(*) from read_parquet('{out_dir}/*.parquet')"
    ).fetchone()[0]
    con.close()
    return total, files


def clean_simple(
    table: str, src_name: str, parallel: bool = True
) -> tuple[int, list[Path]]:
    """Clean a single-file table (recipient, contributor)."""
    src = INPUT / src_name
    if not src.exists():
        raise FileNotFoundError(f"missing source file {src}")
    out_dir = OUTPUT / table
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob("*.parquet"):
        stale.unlink()

    con = _connect()
    _prepare(con, table, src, parallel=parallel)
    files = _copy_split(con, out_dir, table)
    total = con.execute(
        f"select count(*) from read_parquet('{out_dir}/*.parquet')"
    ).fetchone()[0]
    con.close()
    return total, files


def clean_contributor_cycle() -> tuple[int, list[Path]]:
    """Reshape the wide ``amount.<cycle>`` columns into one row per donor-cycle.

    The source contributor file carries 23 ``amount.YYYY`` columns, one per
    election cycle, and most are zero. Only cycles in which the donor actually
    gave are kept, which is what makes the long form smaller than the wide one
    it replaces.

    The reshape is a DuckDB ``UNPIVOT``, deliberately. The obvious alternative —
    a 23-branch ``UNION ALL``, one per amount column — reads the 3 GB gzipped
    source once per branch, and writing one output file per cycle re-evaluates
    every branch each time: about 550 full scans for a job that needs one.
    ``UNPIVOT`` also drops NULLs on its own, so only the explicit zero filter is
    left to write.
    """
    src = INPUT / "dime_contributors_1979_2024.csv.gz"
    if not src.exists():
        raise FileNotFoundError(f"missing source file {src}")
    out_dir = OUTPUT / "contributor_cycle"
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob("*.parquet"):
        stale.unlink()

    con = _connect()
    header = _source_header(src)
    amount_cols = [c for c in header if c.startswith("amount.")]
    if not amount_cols:
        raise RuntimeError(
            "no amount.<cycle> columns found in the contributor file"
        )
    cycles = [c.split(".")[-1] for c in amount_cols]

    con.execute(
        "create or replace view src as select * from "
        + _read_csv(src, header, parallel=True)
    )
    wide = ",\n        ".join(
        f'try_cast(nullif(trim("{col}"), \'\') as double) as "{cycle}"'
        for col, cycle in zip(amount_cols, cycles, strict=True)
    )
    quoted = ", ".join(f'"{c}"' for c in cycles)
    con.execute(f"""
        create or replace view clean as
        select
            cast(cycle as varchar) as cycle,
            contributor_id,
            cast(amount as varchar) as amount
        from (
            select
                nullif(trim("bonica.cid"), '') as contributor_id,
                {wide}
            from src
        ) unpivot (amount for cycle in ({quoted}))
        where amount != 0
    """)

    files = _copy_split(con, out_dir, "contributor_cycle")
    total = con.execute(
        f"select count(*) from read_parquet('{out_dir}/*.parquet')"
    ).fetchone()[0]
    con.close()
    return total, files


def _with_repair(fn, src: Path, label: str):
    """Run a clean step, repairing the source once if it is not valid UTF-8.

    The same stray-byte problem that affects the contribution cycles affects the
    contributor file — one line carries a bare 0x3F-adjacent sequence inside a
    street address. Every entry point needs the repair, not just the backfill.
    """
    try:
        return fn()
    except duckdb.InvalidInputException as exc:
        if "utf-8" not in str(exc).lower():
            raise
        bad = sanitize_source(src)
        print(
            f"  {label}: source had {bad} invalid-UTF-8 line(s), repaired",
            flush=True,
        )
        return fn()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "table",
        choices=[
            "contribution",
            "recipient",
            "contributor",
            "contributor_cycle",
        ],
    )
    p.add_argument(
        "--cycle", type=int, help="single cycle for the contribution table"
    )
    p.add_argument(
        "--drop-input",
        action="store_true",
        help="delete the source file once cleaned (disk is tight)",
    )
    p.add_argument(
        "--copy-only",
        metavar="OUT_DIR",
        help=(
            "write the cycle's part files into OUT_DIR and exit. upload.py runs "
            "this as a separate process rather than a thread: the concurrent "
            "uploader is pure Python, and sharing an interpreter with it starves "
            "the conversion of the GIL."
        ),
    )
    args = p.parse_args()

    if args.copy_only:
        if args.table != "contribution" or not args.cycle:
            raise SystemExit("--copy-only requires contribution and --cycle")
        out_dir = Path(args.copy_only)
        out_dir.mkdir(parents=True, exist_ok=True)
        con = _connect()
        _prepare(con, "contribution", INPUT / f"contribDB_{args.cycle}.csv.gz")
        con.execute(
            f"copy (select * from clean) to '{out_dir}' "
            f"(format parquet, compression snappy, file_size_bytes '{PART_SIZE}')"
        )
        con.close()
        return

    if args.table == "contribution":
        cycles = [args.cycle] if args.cycle else CYCLES
        for cycle in cycles:
            n, files = clean_contribution(cycle)
            size = sum(f.stat().st_size for f in files)
            print(
                f"contribution {cycle}: {n:,} rows -> {len(files)} file(s), {size / 1e9:.2f} GB"
            )
            if args.drop_input:
                (INPUT / f"contribDB_{cycle}.csv.gz").unlink(missing_ok=True)
    elif args.table == "recipient":
        name = "dime_recipients_all_1979_2024.csv.gz"
        n, files = _with_repair(
            lambda: clean_simple("recipient", name, parallel=False),
            INPUT / name,
            "recipient",
        )
        print(f"recipient: {n:,} rows -> {len(files)} file(s)")
    elif args.table == "contributor":
        name = "dime_contributors_1979_2024.csv.gz"
        n, files = _with_repair(
            lambda: clean_simple("contributor", name, parallel=True),
            INPUT / name,
            "contributor",
        )
        print(f"contributor: {n:,} rows -> {len(files)} file(s)")
    else:
        n, files = _with_repair(
            clean_contributor_cycle,
            INPUT / "dime_contributors_1979_2024.csv.gz",
            "contributor_cycle",
        )
        print(f"contributor_cycle: {n:,} rows -> {len(files)} file(s)")


if __name__ == "__main__":
    main()
