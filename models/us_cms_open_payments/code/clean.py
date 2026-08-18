"""Clean one program year of Open Payments into all-STRING parquet.

    uv run --with duckdb python clean.py detail 2024
    uv run --with duckdb python clean.py profile
    uv run --with duckdb python clean.py summary

Staging is all-STRING by house convention and the dbt models ``safe_cast``
every column, so the only value normalisation here is the part ``safe_cast``
cannot do for itself: CMS writes dates as MM/DD/YYYY, which casts to NULL, so
they are rewritten to YYYY-MM-DD. Blank and whitespace-only fields become
NULL rather than empty strings.

Research payments are read once and written twice: the payment-level columns
to ``research``/``research_legacy``, and the five repeated principal
investigator blocks to rows of ``research_principal_investigator``.
"""

import sys

import constants as c
import duckdb
import layout
import naming
import schema

MEMORY_LIMIT = "8GB"


def _expr(source_column: str | None, bd_column: str, table: str) -> str:
    """SQL for one output column, always VARCHAR."""
    if source_column is None:
        # A bare NULL is typed INTEGER by duckdb, which would break the
        # all-STRING staging contract for any column absent from this year.
        return f'CAST(NULL AS VARCHAR) AS "{bd_column}"'
    trimmed = f"nullif(trim(\"{source_column}\"), '')"
    if schema.bigquery_type(table, bd_column) == "DATE":
        # CMS publishes MM/DD/YYYY; safe_cast in dbt needs an ISO date.
        return f"strftime(try_strptime({trimmed}, '%m/%d/%Y'), '%Y-%m-%d') AS \"{bd_column}\""
    return f'{trimmed} AS "{bd_column}"'


def _detail_select(table: str, kind: str, year: int) -> str:
    header = layout.HEADERS["detail"][f"{kind}_{year}"]
    available = {}
    for source in header:
        if naming.split_principal_investigator(source):
            continue
        available[naming.rename(source)] = source
    return ",\n  ".join(
        _expr(available.get(col), col, table) for col in layout.LAYOUT[table]
    )


def _investigator_select(kind_year: int) -> str:
    """UNION ALL over the five investigator slots, empty slots dropped."""
    header = layout.HEADERS["detail"][f"research_{kind_year}"]
    by_slot: dict[int, dict[str, str]] = {i: {} for i in range(1, 6)}
    for source in header:
        split = naming.split_principal_investigator(source)
        if split:
            by_slot[split[0]][split[1]] = source

    table = "research_principal_investigator"
    fields = [
        col
        for col in layout.LAYOUT[table]
        if col not in {"year", "record_id", "principal_investigator_number"}
    ]
    blocks = []
    for slot, mapping in by_slot.items():
        if not mapping:
            continue
        cols = ",\n    ".join(_expr(mapping.get(f), f, table) for f in fields)
        present = " OR ".join(
            f"nullif(trim(\"{src}\"), '') IS NOT NULL"
            for src in mapping.values()
        )
        blocks.append(
            f"""SELECT
    nullif(trim("Program_Year"), '') AS "year",
    nullif(trim("Record_ID"), '') AS "record_id",
    '{slot}' AS "principal_investigator_number",
    {cols}
  FROM src
  WHERE {present}"""
        )
    return "\n  UNION ALL\n  ".join(blocks)


def _copy(con, query: str, table: str, year: int | None) -> int:
    out = c.OUTPUT_DIR / table
    if year is not None:
        out = out / f"year={year}"
    out.mkdir(parents=True, exist_ok=True)
    target = out / "data.parquet"
    con.execute(
        f"COPY ({query}) TO '{target}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 200000)"
    )
    rows = con.execute(
        f"SELECT count(*) FROM read_parquet('{target}')"
    ).fetchone()[0]
    print(f"  {table:38s} year={year} -> {rows:>12,} rows")
    return rows


def _reader(path) -> str:
    # Quoting is stated rather than sniffed, and strict_mode is left at its
    # default: setting it false forces the single-threaded reader, which
    # rejects these files outright ("Parallel CSV Reader does not support a
    # full read on this file").
    return (
        f"read_csv('{path}', all_varchar=true, header=true, "
        "delim=',', quote='\"', escape='\"')"
    )


def clean_detail(year: int) -> dict[str, int]:
    legacy = year in c.LEGACY_YEARS
    con = duckdb.connect(
        config={
            "memory_limit": MEMORY_LIMIT,
            "preserve_insertion_order": "false",
        }
    )
    con.execute(f"SET temp_directory='{c.DATA_ROOT / 'duckdb_tmp'}'")
    counts = {}

    for kind, table in (
        ("general", "general_legacy" if legacy else "general"),
        ("research", "research_legacy" if legacy else "research"),
        ("ownership", "ownership"),
    ):
        path = c.INPUT_DIR / f"{kind}_{year}.csv"
        con.execute(
            f"CREATE OR REPLACE VIEW src AS SELECT * FROM {_reader(path)}"
        )
        counts[table] = _copy(
            con,
            f"SELECT\n  {_detail_select(table, kind, year)}\nFROM src",
            table,
            year,
        )
        if kind == "research":
            counts["research_principal_investigator"] = _copy(
                con,
                _investigator_select(year),
                "research_principal_investigator",
                year,
            )
    con.close()
    return counts


def clean_profiles() -> dict[str, int]:
    con = duckdb.connect(
        config={
            "memory_limit": MEMORY_LIMIT,
            "preserve_insertion_order": "false",
        }
    )
    counts = {}
    for table in layout.HEADERS["profile"]:
        path = c.INPUT_DIR / f"{table}.csv"
        con.execute(
            f"CREATE OR REPLACE VIEW src AS SELECT * FROM {_reader(path)}"
        )
        available = {
            naming.rename_profile(table, s): s
            for s in layout.HEADERS["profile"][table]
        }
        cols = ",\n  ".join(
            _expr(available.get(col), col, table)
            for col in layout.LAYOUT[table]
        )
        counts[table] = _copy(con, f"SELECT\n  {cols}\nFROM src", table, None)
    con.close()
    return counts


def _summary_select(table: str, year: int | None) -> str:
    """Column list for one summary report.

    Reports in SUMMARY_WITH_YEAR carry Program_Year already; the rest get the
    program year from the file name. The all-year files also carry a rolled-up
    row with Program_Year 'ALL' (spelled 'All' in some files) -- dropped here,
    since it is the sum of the per-year rows and cannot sit in an INT64
    partition.
    """
    available = {
        naming.rename_summary(s_): s_
        for s_ in layout.HEADERS["summary"][table]
    }
    parts = []
    for col in layout.LAYOUT[table]:
        if col == "year" and year is not None:
            parts.append(f"'{year}' AS \"year\"")
        else:
            parts.append(_expr(available.get(col), col, table))
    query = "SELECT\n  " + ",\n  ".join(parts) + "\nFROM src"
    if year is None:
        query += "\nWHERE upper(trim(\"Program_Year\")) <> 'ALL'"
    return query


def _dashboard_select(years: list[int]) -> str:
    """Unpivot the dashboard, which CMS publishes one column per program year."""
    blocks = [
        f"""SELECT
    '{year}' AS "year",
    nullif(trim("Dashboard_Row_Number"), '') AS "dashboard_row_number",
    nullif(trim("Data_Metrics"), '') AS "metric",
    nullif(trim("PY_{year}"), '') AS "value"
  FROM src"""
        for year in years
    ]
    return "\n  UNION ALL\n  ".join(blocks)


def clean_summaries() -> dict[str, int]:
    con = duckdb.connect(
        config={
            "memory_limit": MEMORY_LIMIT,
            "preserve_insertion_order": "false",
        }
    )
    con.execute(f"SET temp_directory='{c.DATA_ROOT / 'duckdb_tmp'}'")
    counts = {}

    for table in c.SUMMARY_PER_YEAR:
        for year in c.SUMMARY_YEARS:
            path = c.INPUT_DIR / f"{table}_{year}.csv"
            con.execute(
                f"CREATE OR REPLACE VIEW src AS SELECT * FROM {_reader(path)}"
            )
            counts[f"{table}/{year}"] = _copy(
                con, _summary_select(table, year), table, year
            )

    for table in c.SUMMARY_ALL_YEARS:
        path = c.INPUT_DIR / f"{table}.csv"
        con.execute(
            f"CREATE OR REPLACE VIEW src AS SELECT * FROM {_reader(path)}"
        )
        if table == "summary_dashboard":
            query = _dashboard_select(c.SUMMARY_YEARS)
        else:
            query = _summary_select(table, None)
        # One file spans every program year, so it is split into partitions.
        for year in c.SUMMARY_YEARS:
            counts[f"{table}/{year}"] = _copy(
                con,
                f"SELECT * FROM ({query}) WHERE \"year\" = '{year}'",
                table,
                year,
            )
    con.close()
    return counts


if __name__ == "__main__":
    what = sys.argv[1]
    if what == "detail":
        clean_detail(int(sys.argv[2]))
    elif what == "profile":
        clean_profiles()
    elif what == "summary":
        clean_summaries()
    else:
        raise SystemExit(f"unknown target: {what}")
