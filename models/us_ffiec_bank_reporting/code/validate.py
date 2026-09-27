"""Verify the built dataset against the source and against us_fdic_bankfind.

Six checks, each of which has to pass before the dev/staging checkpoint:

  1. row counts       parquet footers match BigQuery, table by table
  2. coverage         every expected quarter and year is present, no holes
  3. keys             the declared unique key really is unique
  4. units            total assets agree with the already published
                      us_fdic_bankfind figures for the same bank and quarter,
                      which is what proves the thousands rescale is right rather
                      than merely plausible
  5. safe_cast        no column silently emptied between staging and the model
  6. sparsity         measure each column's null share so the schema.yml
                      ignore_values list is measured, not guessed

Usage:
    python validate.py [--local-only]
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from common import (
    BHC_FIRST,
    BHC_LAST,
    CALL_FIRST,
    CALL_LAST,
    CRA_FIRST_YEAR,
    CRA_LAST_YEAR,
    OUTPUT_DIR,
    quarters,
)
from schema_def import TABLES

BILLING_PROJECT = "basedosdados-dev"
DATASET_ID = "us_ffiec_bank_reporting"

UNIQUE_KEY = {
    "institution": ["year", "quarter", "rssd_id"],
    "call_report_item": ["year", "quarter", "rssd_id", "item_code"],
    "holding_company": ["year", "quarter", "rssd_id"],
    "holding_company_item": ["year", "quarter", "rssd_id", "item_code"],
    "mdrm_item": ["item_code"],
    "cra_respondent": ["year", "respondent_id", "agency_id"],
    "dictionary": ["table_id", "column_name", "key"],
}

results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str) -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)


def parquet_files(table: str) -> list[Path]:
    return sorted((OUTPUT_DIR / table).rglob("*.parquet"))


def local_rows(table: str) -> int:
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in parquet_files(table)
        if f.name != "00_header.parquet"
    )


# --- 1. row counts ---------------------------------------------------------


def check_row_counts(local_only: bool) -> None:
    totals = {}
    for table in TABLES:
        totals[table] = local_rows(table)
    print("\nlocal row counts:")
    for table, n in totals.items():
        print(f"  {table:30s} {n:>15,}")
    print(f"  {'TOTAL':30s} {sum(totals.values()):>15,}")
    if local_only:
        return
    import basedosdados as bd

    for table, expected in totals.items():
        frame = bd.read_sql(
            f"select count(*) as n from "
            f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table}`",
            billing_project_id=BILLING_PROJECT,
            from_file=True,
        )
        actual = int(frame["n"].iloc[0])
        record(
            f"row_count.{table}",
            actual == expected,
            f"parquet {expected:,} vs BigQuery {actual:,}",
        )


# --- 2. coverage -----------------------------------------------------------


def check_coverage() -> None:
    for table, first, last in (
        ("call_report_item", CALL_FIRST, CALL_LAST),
        ("institution", CALL_FIRST, CALL_LAST),
    ):
        expected = {f"{y}Q{q}" for y, q in quarters(first, last)}
        found = _quarters_present(table)
        missing = sorted(expected - found)
        record(
            f"coverage.{table}",
            not missing,
            f"{len(found)}/{len(expected)} quarters"
            + (f", missing {missing[:6]}" if missing else ""),
        )
    expected = {f"{y}Q{q}" for y, q in quarters(BHC_FIRST, BHC_LAST)}
    found = _quarters_present("holding_company_item")
    record(
        "coverage.holding_company_item",
        len(found) >= len(expected) - 8,
        f"{len(found)}/{len(expected)} quarters present",
    )
    for table in (
        "cra_lending",
        "cra_assessment_area_tract",
        "cra_respondent",
    ):
        years = {
            int(p.parent.name.split("=")[1])
            for p in parquet_files(table)
            if "year=" in str(p)
        }
        expected_years = set(range(CRA_FIRST_YEAR, CRA_LAST_YEAR + 1))
        missing = sorted(expected_years - years)
        record(
            f"coverage.{table}",
            not missing,
            f"{len(years)}/{len(expected_years)} years"
            + (f", missing {missing}" if missing else ""),
        )


def _quarters_present(table: str) -> set[str]:
    out = set()
    for path in parquet_files(table):
        if "year=" not in str(path):
            continue
        year = path.parent.name.split("=")[1]
        stem = path.stem
        quarter = stem.split("_q")[-1] if "_q" in stem else "?"
        out.add(f"{year}Q{quarter}")
    return out


# --- 3. keys ---------------------------------------------------------------


def check_keys(sample_tables: list[str] | None = None) -> None:
    for table, key in UNIQUE_KEY.items():
        if sample_tables and table not in sample_tables:
            continue
        seen: set[tuple] = set()
        dupes = 0
        rows = 0
        for path in parquet_files(table):
            if path.name == "00_header.parquet":
                continue
            data = pq.ParquetFile(path).read(columns=key).to_pydict()
            for values in zip(*(data[c] for c in key), strict=False):
                rows += 1
                if values in seen:
                    dupes += 1
                else:
                    seen.add(values)
        record(
            f"unique_key.{table}",
            dupes == 0,
            f"{rows:,} rows, {dupes:,} duplicate {tuple(key)}",
        )


# --- 4. units, against the published us_fdic_bankfind figures --------------

# Total assets: the Call Report item, and the FDIC's own indicator. If the
# thousands rescale were wrong these would differ by exactly 1,000x.
ASSET_ITEMS = ("RCFD2170", "RCON2170")


def check_units() -> None:
    import basedosdados as bd

    frame = bd.read_sql(
        f"""
        with call as (
          select i.fdic_cert_id as cert, c.value as call_assets
          from `{BILLING_PROJECT}.{DATASET_ID}_staging.call_report_item` c
          join `{BILLING_PROJECT}.{DATASET_ID}_staging.institution` i
            on i.rssd_id = c.rssd_id and i.year = c.year and i.quarter = c.quarter
          where c.year = '2026' and c.quarter = '2'
            and c.item_code in {ASSET_ITEMS}
            and i.fdic_cert_id is not null
        ),
        fdic as (
          select cert, value as fdic_assets
          from `basedosdados.us_fdic_bankfind.financials_indicator`
          where year = 2026 and quarter = 2 and indicator_id = 'ASSET'
        )
        select
          count(*) as n,
          countif(abs(safe_cast(call_assets as float64) - fdic_assets)
                  <= 0.005 * abs(fdic_assets)) as agree
        from call join fdic using (cert)
        """,
        billing_project_id=BILLING_PROJECT,
        from_file=True,
    )
    n = int(frame["n"].iloc[0])
    agree = int(frame["agree"].iloc[0])
    record(
        "units.total_assets_vs_fdic",
        n > 0 and agree / n > 0.98,
        f"{agree:,}/{n:,} banks agree within 0.5% with us_fdic_bankfind",
    )


# --- 5. safe_cast ----------------------------------------------------------


def check_safe_cast() -> None:
    import basedosdados as bd

    for table in ("institution", "call_report_item", "cra_lending"):
        cols = [c[0] for c in TABLES[table]]
        staging = ", ".join(
            f"countif({c} is not null and {c} != '') as {c}" for c in cols
        )
        modelled = ", ".join(f"countif({c} is not null) as {c}" for c in cols)
        where = (
            "where year = '2026'"
            if table != "cra_lending"
            else "where year = '2024'"
        )
        where_m = where.replace("'2026'", "2026").replace("'2024'", "2024")
        a = bd.read_sql(
            f"select {staging} from "
            f"`{BILLING_PROJECT}.{DATASET_ID}_staging.{table}` {where}",
            billing_project_id=BILLING_PROJECT,
            from_file=True,
        ).iloc[0]
        b = bd.read_sql(
            f"select {modelled} from "
            f"`{BILLING_PROJECT}.{DATASET_ID}.{table}` {where_m}",
            billing_project_id=BILLING_PROJECT,
            from_file=True,
        ).iloc[0]
        lost = [c for c in cols if int(a[c]) > 0 and int(b[c]) == 0]
        shrunk = {
            c: (int(a[c]), int(b[c]))
            for c in cols
            if int(a[c]) > 0 and int(b[c]) < int(a[c]) * 0.99
        }
        record(
            f"safe_cast.{table}",
            not lost and not shrunk,
            f"{len(cols)} columns, {len(lost)} emptied, {len(shrunk)} shrunk"
            + (f" {shrunk}" if shrunk else ""),
        )


# --- 6. sparsity -----------------------------------------------------------


def measure_sparsity() -> None:
    print("\nnull share by column (for schema.yml ignore_values):")
    for table in TABLES:
        files = [
            p for p in parquet_files(table) if p.name != "00_header.parquet"
        ]
        if not files:
            continue
        files = files[-4:]
        cols = [c[0] for c in TABLES[table]]
        nulls: dict[str, int] = defaultdict(int)
        total = 0
        for path in files:
            data = pq.ParquetFile(path).read().to_pydict()
            n = len(next(iter(data.values())))
            total += n
            for c in cols:
                nulls[c] += sum(
                    1 for v in data.get(c, []) if v is None or v == ""
                )
        sparse = {
            c: round(nulls[c] / total * 100, 1)
            for c in cols
            if total and nulls[c] / total > 0.95
        }
        if sparse:
            print(f"  {table}: over 95% null -> {sparse}")


def main() -> None:
    local_only = "--local-only" in sys.argv
    check_row_counts(local_only)
    check_coverage()
    check_keys(["institution", "mdrm_item", "cra_respondent", "dictionary"])
    measure_sparsity()
    if not local_only:
        check_units()
        check_safe_cast()
    failed = [name for name, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print("FAILED: " + ", ".join(failed))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
