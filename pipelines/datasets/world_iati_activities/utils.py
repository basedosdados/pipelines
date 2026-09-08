"""Pure functions for world_iati_activities: download, clean, write.

No Prefect imports here. ``tasks.py`` wraps these; the one-shot bootstrap under
``models/world_iati_activities/code/`` imports them directly, so the transform
exists in exactly one place.

The architecture CSVs under ``models/world_iati_activities/code/architecture/``
are the schema authority: they carry the clean column name, its BigQuery type,
and the raw IATI Tables column it comes from. Nothing here hardcodes a column.

Staging parquet is written **all-STRING** with a stable column order, per
.claude/rules/bigquery-conventions.md — the dbt model does every ``safe_cast``.
Three source encodings would not survive a naive pass-through and are normalised
here, in the parquet, precisely because ``safe_cast`` would turn each into a
silent NULL rather than an error:

1. Booleans are PostgreSQL's ``t``/``f``. ``SAFE_CAST('t' AS BOOL)`` is NULL, so
   they become ``true``/``false``.
2. Timestamps carry mixed UTC offsets (``+00:00``, ``Z``, ``+01:00``, ``-05:00``,
   and some with none at all). ``SAFE_CAST`` of an offset-bearing string to
   DATETIME is NULL, so they are converted to UTC and rendered
   ``YYYY-MM-DD HH:MM:SS``.
3. Empty strings are written as NULL, never as the literal ``nan`` a pandas
   ``astype(str)`` would produce.
"""

from __future__ import annotations

import csv
import json
import os
import shutil
from collections import namedtuple
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import requests

from pipelines.datasets.world_iati_activities.constants import constants

Col = namedtuple("Col", "name bq_type original_name")

# A row whose partition date is missing or unparseable goes to year 0 rather
# than being dropped or given an invented date. Hive-partitioned NULLs would
# land in __HIVE_DEFAULT_PARTITION__, which BigQuery cannot read as INT64.
UNKNOWN_YEAR = 0

# IATI accepts any date a publisher types. Anything outside this window is a
# data-entry error, not a real transaction date, and is treated as unknown so
# it cannot create thousands of empty partitions.
MIN_YEAR = 1900
MAX_YEAR = 2100


# --- Schema ---------------------------------------------------------------


def load_cols(table: str) -> list[Col]:
    """Read one architecture CSV. Column order in the file is the column order
    in BigQuery."""
    path = constants.ARCHITECTURE_DIR.value / f"sheet_{table}.csv"
    with path.open(encoding="utf-8") as fh:
        return [
            Col(r["name"], r["bigquery_type"], r["original_name"])
            for r in csv.DictReader(fh)
        ]


def assert_all_string(path: Path) -> None:
    """Fail loudly if any parquet column is not STRING.

    Staging is all-STRING by house convention, and both upload paths must agree:
    a typed onboarding external table collides with the pipeline's later
    all-STRING overwrite (.claude/rules/prefect-pipeline-conventions.md).
    """
    files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
    assert files, f"no parquet written under {path}"
    for f in files:
        schema = pq.read_schema(f)
        bad = [
            n
            for n, t in zip(schema.names, schema.types, strict=True)
            if str(t) not in ("string", "large_string")
        ]
        assert not bad, f"{f}: non-string columns {bad}"


# --- Download -------------------------------------------------------------


def download(url: str, dest: Path, chunk: int = 1 << 20) -> Path:
    """Stream a URL to disk. ``decode_content`` is not used because the response
    is consumed through ``iter_content``, which already decodes transfer
    encodings (see reference_requests_raw_skips_content_encoding)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(
        url, headers=constants.HEADERS.value, stream=True, timeout=(30, 600)
    ) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for block in r.iter_content(chunk_size=chunk):
                fh.write(block)
    return dest


def source_max_date(stats_path: Path) -> str:
    """The Bulk Data Service snapshot IATI Tables last built from, as
    ``YYYY-MM-DD``. This is the source's own coverage marker and is what the
    poll compares against."""
    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    return stats["data_dump_updated_at"][:10]


# --- Transform ------------------------------------------------------------


def _select_expr(col: Col, table: str) -> str:
    """The DuckDB expression producing one clean column, always as VARCHAR."""
    raw = col.original_name

    if col.name == "year":
        return f"{_year_expr(table)} AS year"
    if col.name == "licence_id":
        return "nullif(trim(lic.licence_id), '') AS licence_id"
    if raw == "transaction.dataset":
        return (
            "nullif(trim(txn.registry_dataset_id), '') AS registry_dataset_id"
        )

    src = f"nullif(trim(s.\"{raw}\"), '')"

    if col.bq_type == "BOOLEAN":
        # PostgreSQL's t/f. Anything else (including a value a future export
        # might use) is passed through unchanged rather than silently dropped.
        return (
            f"CASE {src} WHEN 't' THEN 'true' WHEN 'f' THEN 'false' "
            f"ELSE {src} END AS {col.name}"
        )
    if col.bq_type == "DATETIME":
        # Mixed offsets, and some values with none. try_cast to TIMESTAMPTZ
        # resolves the offset; a naive value is read in the session zone, which
        # is forced to UTC by the connection setup.
        return (
            f"coalesce("
            f"strftime(try_cast({src} AS TIMESTAMPTZ) AT TIME ZONE 'UTC',"
            f" '%Y-%m-%d %H:%M:%S'), {src}) AS {col.name}"
        )
    return f"{src} AS {col.name}"


def _year_expr(table: str) -> str:
    date_col = constants.PARTITION_SOURCE.value[table]
    cols = {c.name: c for c in load_cols(table)}
    raw = cols[date_col].original_name
    return (
        f"CAST(coalesce("
        f"  nullif(CASE WHEN year(try_cast(nullif(trim(s.\"{raw}\"), '') AS DATE))"
        f"    BETWEEN {MIN_YEAR} AND {MAX_YEAR}"
        f"    THEN year(try_cast(nullif(trim(s.\"{raw}\"), '') AS DATE)) END, NULL),"
        f"  {UNKNOWN_YEAR}) AS VARCHAR)"
    )


def build_registry_dataset(datasets_minimal: Path, out_dir: Path) -> Path:
    """The registry dataset dimension: one row per IATI Registry dataset, with
    the licence its publisher declared. This is also the licence lookup every
    other table joins to."""
    cols = load_cols("registry_dataset")
    raw = json.loads(datasets_minimal.read_text(encoding="utf-8"))["datasets"]
    rows = []
    for d in raw:
        good = d.get("last_known_good_dataset") or {}
        cached = good.get("cached_dataset_url_xml")
        licence = (d.get("licence_id") or "").strip() or None
        rows.append(
            {
                "registry_dataset_id": d["short_name"],
                "publisher_id": d.get("reporting_org_short_name"),
                "licence_id": licence,
                "source_url": d.get("source_url") or None,
                "cached_xml_url": cached or None,
                "is_downloaded": "true" if cached else "false",
                "last_update_check": (d.get("last_update_check") or "")[:19]
                or None,
            }
        )
    assert [c.name for c in cols] == list(rows[0]), "architecture/order drift"

    dest = out_dir / "registry_dataset"
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    con = _connect()
    con.register("rows", _arrow_all_string(rows, [c.name for c in cols]))
    con.execute(
        f"COPY (SELECT * FROM rows) TO '{dest / 'data.parquet'}' "
        "(FORMAT PARQUET, COMPRESSION snappy)"
    )
    con.close()
    assert_all_string(dest)
    return dest


def _arrow_all_string(rows, names):
    import pyarrow as pa

    return pa.table(
        {n: pa.array([r[n] for r in rows], type=pa.string()) for n in names}
    )


def _connect(temp_dir: Path | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Naive timestamps are read as UTC rather than as the machine's local zone,
    # so the same input produces the same output on a laptop and on the worker.
    con.execute("SET TimeZone='UTC'")
    # transaction_breakdown is 24.6M rows and partitions on write; cap the
    # memory so it spills instead of being OOM-killed on the Prefect pod, and
    # spill next to the data rather than into the system temp.
    con.execute(
        f"SET memory_limit='{os.environ.get('IATI_DUCKDB_MEMORY', '6GB')}'"
    )
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{temp_dir}'")
    return con


def _scalar(con: duckdb.DuckDBPyConnection, sql: str):
    """``fetchone`` is typed Optional; an aggregate always returns a row, and a
    missing one is a bug worth failing on rather than a None to propagate."""
    row = con.execute(sql).fetchone()
    assert row is not None, f"no row returned by: {sql}"
    return row[0]


def clean_table(
    table: str,
    csv_dir: Path,
    out_dir: Path,
    registry_parquet: Path,
    txn_lookup: Path | None = None,
) -> dict:
    """Convert one IATI Tables CSV into all-STRING parquet.

    Joins the publisher's licence, drops the non-commercial datasets, and drops
    rows whose registry dataset is no longer in the Bulk Data Service index —
    those have no licence we can assert. Returns the row counts so the caller
    can report what the filter removed rather than hiding it.
    """
    member = constants.SOURCE_TABLES.value[table]
    src_csv = csv_dir / f"{member}.csv"
    cols = load_cols(table)
    partitioned = table in constants.PARTITION_SOURCE.value

    dest = out_dir / table
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    con = _connect(temp_dir=out_dir / "_duckdb_tmp")
    con.execute(
        f"CREATE VIEW lic AS SELECT registry_dataset_id, licence_id "
        f"FROM read_parquet('{registry_parquet / 'data.parquet'}')"
    )
    join_key = (
        "txn.registry_dataset_id"
        if table == "transaction_breakdown"
        else 's."dataset"'
    )
    joins = [f"LEFT JOIN lic ON lic.registry_dataset_id = {join_key}"]
    if table == "transaction_breakdown":
        assert txn_lookup is not None, "breakdown needs the transaction lookup"
        joins.insert(
            0,
            f"LEFT JOIN read_parquet('{txn_lookup}') AS txn "
            f'ON txn.transaction_id = s."_link_transaction"',
        )

    select = ",\n       ".join(_select_expr(c, table) for c in cols)
    nc = ", ".join(
        f"'{x}'" for x in sorted(constants.NON_COMMERCIAL_LICENCES.value)
    )
    body = f"""
        SELECT {select}
        FROM read_csv('{src_csv}', all_varchar=true, header=true,
                      sample_size=-1) AS s
        {" ".join(joins)}
        WHERE lic.registry_dataset_id IS NOT NULL
          AND coalesce(lic.licence_id, '') NOT IN ({nc})
    """

    total = _scalar(
        con,
        f"SELECT count(*) FROM read_csv('{src_csv}', all_varchar=true, "
        "header=true, sample_size=-1)",
    )

    copy_opts = "FORMAT PARQUET, COMPRESSION snappy"
    if partitioned:
        copy_opts += ", PARTITION_BY (year), OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'data'"
        con.execute(f"COPY ({body}) TO '{dest}' ({copy_opts})")
    else:
        con.execute(
            f"COPY ({body}) TO '{dest / 'data.parquet'}' ({copy_opts})"
        )

    kept = _scalar(
        con, f"SELECT count(*) FROM read_parquet('{dest}/**/*.parquet')"
    )
    con.close()

    _rename_partition_files(dest)
    assert_all_string(dest)
    return {"table": table, "source_rows": total, "kept_rows": kept}


def _rename_partition_files(dest: Path) -> None:
    """DuckDB writes ``data0.parquet`` per partition; the house convention is
    ``data.parquet``."""
    for f in dest.rglob("*.parquet"):
        if f.name != "data.parquet":
            f.rename(f.with_name("data.parquet"))


def write_transaction_lookup(out_dir: Path, dest: Path) -> Path:
    """transaction_breakdown carries no ``dataset`` column, so its registry
    dataset — and therefore its licence — is only reachable through the
    transaction it decomposes."""
    con = _connect(temp_dir=out_dir / "_duckdb_tmp")
    con.execute(
        f"COPY (SELECT transaction_id, registry_dataset_id "
        f"FROM read_parquet('{out_dir / 'transaction'}/**/*.parquet')) "
        f"TO '{dest}' (FORMAT PARQUET, COMPRESSION snappy)"
    )
    con.close()
    return dest


def clean_all(
    csv_dir: Path, out_dir: Path, datasets_minimal: Path
) -> list[dict]:
    """Every table, in an order that satisfies the one intra-dataset dependency
    (transaction before transaction_breakdown)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    registry = build_registry_dataset(datasets_minimal, out_dir)
    report = []
    lookup = None
    for table in constants.ALL_TABLES.value:
        if table == "registry_dataset":
            continue
        if table == "transaction_breakdown":
            lookup = write_transaction_lookup(
                out_dir, out_dir / "_transaction_lookup.parquet"
            )
        report.append(
            clean_table(table, csv_dir, out_dir, registry, txn_lookup=lookup)
        )
    if lookup and lookup.exists():
        lookup.unlink()
    return report
