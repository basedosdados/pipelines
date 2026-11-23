"""Clean the Stanford Open Policing CSVs into all-STRING partitioned Parquet.

One source file per jurisdiction. Each shares the project's standardized
schema but fills a different subset of it, so every file is projected onto
the full ``stop`` architecture with absent columns as NULL. Columns prefixed
``raw_`` are location-specific pass-throughs of the agency's raw data and are
dropped: they cannot share one table.

Output (under ``constants.OUTPUT_DIR``):
    stop/year=YYYY/state_abbreviation=XX/<jurisdiction>_<n>.parquet
    jurisdiction/data.parquet
    _manifest/<jurisdiction>.json   rows, date range, duplicate keys

Every column is written as STRING; the dbt model casts. ``NA`` is the source's
null marker (R's readr).

Usage:
    python models/us_stanford_open_policing/code/clean.py [slug ...] [--force]
"""

import argparse
import json
import shutil
import sys
import zipfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from models.us_stanford_open_policing.code import architecture as arch
    from models.us_stanford_open_policing.code import constants
except ModuleNotFoundError:  # run as a script
    import architecture as arch  # type: ignore[no-redef]
    import constants  # type: ignore[no-redef]

STOP_DIR = constants.OUTPUT_DIR / "stop"
MANIFEST_DIR = constants.OUTPUT_DIR / "_manifest"
TMP_DIR = constants.DATA_DIR / "tmp"
PARTITION = ["year", "state_abbreviation"]
ADDED = {"year", "state_abbreviation", "state_id", "jurisdiction_id"}
# The ASCII whitespace Python's str.strip removes, as a DuckDB expression
WHITESPACE = " || ".join(f"chr({c})" for c in (32, 9, 10, 11, 12, 13))


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("set memory_limit = '8GB'")
    con.execute("set preserve_insertion_order = false")
    con.execute(f"set temp_directory = '{TMP_DIR / 'duckdb'}'")
    return con


def _extract(slug: str):
    archive = constants.source_file(slug)
    with zipfile.ZipFile(archive) as z:
        (member,) = [n for n in z.namelist() if n.endswith(".csv")]
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        target = TMP_DIR / f"{slug}.csv"
        with z.open(member) as src, target.open("wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 24)
    return target


def clean_one(slug: str, con: duckdb.DuckDBPyConnection) -> dict:
    state = slug[:2].upper()
    csv_path = _extract(slug)
    src = (
        f"read_csv('{csv_path}', all_varchar = true, header = true, "
        "nullstr = 'NA', quote = '\"', escape = '\"', sample_size = -1)"
    )
    header = [
        r[0] for r in con.execute(f"describe select * from {src}").fetchall()
    ]
    standard = [c for c in header if not c.startswith("raw_")]
    unknown = sorted(set(standard) - set(arch.column_names("stop")))
    if unknown:
        raise ValueError(
            f"{slug}: columns missing from the architecture: {unknown}"
        )

    con.execute(f"create or replace temp view src as select * from {src}")
    exprs = []
    for name in arch.column_names("stop"):
        if name == "year":
            expr = 'substr("date", 1, 4)'
        elif name == "state_abbreviation":
            expr = f"'{state}'"
        elif name == "state_id":
            expr = f"'{constants.STATE_FIPS[state]}'"
        elif name == "jurisdiction_id":
            expr = f"'{slug}'"
        elif name in header:
            # trim() alone strips only spaces; tn_nashville has notes of "\n\n"
            expr = f"nullif(trim(\"{name}\", {WHITESPACE}), '')"
        else:
            expr = "null"
        exprs.append(f'cast({expr} as varchar) as "{name}"')
    con.execute(
        f"create or replace temp table cleaned as select {', '.join(exprs)} from src"
    )

    stats = con.execute(
        """
        select count(*), min("date"), max("date"),
               count(*) filter (where "date" is null),
               count(*) - count(distinct raw_row_number),
               count(*) filter (where raw_row_number is null)
        from cleaned
        """
    ).fetchone()
    assert stats is not None
    rows, start, end, null_date, dup_raw, null_raw = stats

    STOP_DIR.mkdir(parents=True, exist_ok=True)
    for old in STOP_DIR.glob(
        f"year=*/state_abbreviation={state}/{slug}_*.parquet"
    ):
        old.unlink()
    con.execute(
        f"""
        copy cleaned to '{STOP_DIR}' (
            format parquet, compression snappy,
            partition_by ({", ".join(PARTITION)}),
            filename_pattern '{slug}_{{i}}',
            overwrite_or_ignore true
        )
        """
    )
    csv_path.unlink()

    used = [c for c in standard if c in arch.column_names("stop")]
    manifest = {
        "slug": slug,
        "rows": rows,
        "start_date": start,
        "end_date": end,
        "null_date": null_date,
        "duplicate_raw_row_number": dup_raw,
        "null_raw_row_number": null_raw,
        "columns_present": used,
        "raw_columns_dropped": [c for c in header if c.startswith("raw_")],
    }
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    (MANIFEST_DIR / f"{slug}.json").write_text(json.dumps(manifest, indent=1))
    return manifest


def build_jurisdiction() -> int:
    """Write the jurisdiction table from the manifests of every cleaned file."""
    rows = []
    for slug, (name, release) in sorted(constants.JURISDICTIONS.items()):
        m = json.loads((MANIFEST_DIR / f"{slug}.json").read_text())
        state = slug[:2].upper()
        rows.append(
            {
                "jurisdiction_id": slug,
                "state_abbreviation": state,
                "state_id": constants.STATE_FIPS[state],
                "jurisdiction_name": name,
                "jurisdiction_type": "statewide"
                if name == "Statewide"
                else "city",
                "release_date": release,
                "start_date": m["start_date"],
                "end_date": m["end_date"],
                "number_stops": str(m["rows"]),
                "source_url": constants.source_url(slug),
            }
        )
    names = arch.column_names("jurisdiction")
    schema = pa.schema([(n, pa.string()) for n in names])
    table = pa.Table.from_pylist(rows, schema=schema)
    out = constants.OUTPUT_DIR / "jurisdiction"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out / "data.parquet", compression="snappy")
    return table.num_rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("slugs", nargs="*")
    p.add_argument(
        "--force", action="store_true", help="re-clean files already done"
    )
    p.add_argument(
        "--jurisdiction",
        action="store_true",
        help="only build the jurisdiction table",
    )
    args = p.parse_args()
    if args.jurisdiction:
        print(f"jurisdiction: {build_jurisdiction()} rows")
        return
    slugs = args.slugs or sorted(
        constants.JURISDICTIONS,
        key=lambda s: (
            constants.source_file(s).stat().st_size
            if constants.source_file(s).exists()
            else 0
        ),
    )
    con = _connect()
    failed = []
    for slug in slugs:
        if not constants.source_file(slug).exists():
            print(f"{slug}: not downloaded yet, skipped", flush=True)
            continue
        if (MANIFEST_DIR / f"{slug}.json").exists() and not args.force:
            print(f"{slug}: done already", flush=True)
            continue
        try:
            m = clean_one(slug, con)
        except Exception as exc:
            failed.append(slug)
            print(f"{slug}: FAILED {type(exc).__name__}: {exc}", flush=True)
            continue
        print(
            f"{slug}: {m['rows']:,} rows {m['start_date']}..{m['end_date']} "
            f"dup_raw={m['duplicate_raw_row_number']} cols={len(m['columns_present'])}",
            flush=True,
        )
    if failed:
        sys.exit(f"failed: {failed}")


if __name__ == "__main__":
    main()
