"""Validate the materialized us_fdic_bankfind tables against the local parquet.

    uv run python models/us_fdic_bankfind/code/validate.py

Four checks, each of which has caught a real problem on some dataset in this
repo:

1. **Row counts** — BigQuery against the parquet footers, per table.  Reads the
   footers rather than the data, so it costs nothing locally, and uses
   `__TABLES__` rather than `count(*)` so it costs nothing in BigQuery either.
2. **Coverage** — the min and max period actually present, so the registered
   DateTimeRange can be checked against the data instead of against intent.
3. **safe_cast column loss** — `safe_cast` returns NULL instead of raising, so a
   column whose staging text does not parse arrives silently empty with every
   test still green.  Compares each column's non-null count in the model against
   the same column in staging.
4. **Wide against long** — the same figure reached two ways, for a sample of
   institution-quarters.  The wide table's `total_assets` must equal the long
   table's `ASSET`, which is the end-to-end check that the melt, the unit
   scaling and the column mapping all agree.
"""

from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery

PROJECT = "basedosdados-dev"
DATASET = "us_fdic_bankfind"
OUT = (
    Path(
        os.environ.get(
            "FDIC_DATA_DIR", Path.home() / "Downloads/us_fdic_bankfind_data"
        )
    )
    / "output"
)
TABLES = ["institution", "indicator", "financials", "financials_indicator"]


def client() -> bigquery.Client:
    config = tomllib.loads(
        (Path.home() / ".basedosdados/config.toml").read_text()
    )
    key = config["gcloud-projects"]["staging"]["credentials_path"]
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", key)
    return bigquery.Client(project=PROJECT)


def local_rows(table: str) -> int:
    return sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in (OUT / table).rglob("*.parquet")
    )


def bq_rows(bq: bigquery.Client) -> dict[str, int]:
    # __TABLES__ is metadata: no bytes billed, unlike count(*)
    query = f"select table_id, row_count from `{PROJECT}.{DATASET}.__TABLES__`"
    return {r["table_id"]: r["row_count"] for r in bq.query(query).result()}


def check_row_counts(bq: bigquery.Client) -> list[str]:
    print("1. row counts")
    remote = bq_rows(bq)
    problems = []
    for table in TABLES:
        want, got = local_rows(table), remote.get(table, 0)
        ok = "OK " if want == got else "BAD"
        if want != got:
            problems.append(f"{table}: parquet {want:,} vs BigQuery {got:,}")
        print(f"   {ok} {table:<22} parquet {want:>14,}  BigQuery {got:>14,}")
    return problems


def check_coverage(bq: bigquery.Client) -> list[str]:
    print("\n2. coverage actually present")
    query = f"""
    select 'financials' as t, min(year) mn, max(year) mx,
           min(concat(cast(year as string), 'Q', cast(quarter as string))) mnq,
           max(concat(cast(year as string), 'Q', cast(quarter as string))) mxq,
           count(distinct cert) certs
    from `{PROJECT}.{DATASET}.financials`
    """
    for r in bq.query(query).result():
        print(
            f"   financials  {r['mn']}..{r['mx']}  "
            f"first={r['mnq']} last={r['mxq']}  distinct certs={r['certs']:,}"
        )
    return []


def check_safe_cast(bq: bigquery.Client) -> list[str]:
    """Compare non-null counts, model against staging, on the wide table.

    safe_cast NULLs instead of raising, so a mis-typed column arrives empty and
    every dbt test still passes.  Only the most recent year is scanned: a
    whole-table pass over 290 columns is a large amount of billed bytes for a
    check that is just as conclusive on one partition.
    """
    print("\n3. safe_cast column loss (most recent year)")
    model = bq.get_table(f"{PROJECT}.{DATASET}.financials")
    names = [f.name for f in model.schema]
    year = next(
        iter(
            bq.query(
                f"select max(year) y from `{PROJECT}.{DATASET}.financials`"
            ).result()
        )
    )["y"]

    def nonnull(table: str, cast: bool) -> dict[str, int]:
        parts = []
        for n in names:
            col = f"safe_cast({n} as string)" if cast else n
            parts.append(f"countif({col} is not null) as {n}")
        source = (
            f"`{PROJECT}.{DATASET}_staging.financials` where safe_cast(year as int64) = {year}"
            if cast
            else f"`{PROJECT}.{DATASET}.financials` where year = {year}"
        )
        rows = bq.query(f"select {', '.join(parts)} from {source}").result()
        return dict(next(iter(rows)))

    modelled, staged = (
        nonnull("financials", False),
        nonnull("financials", True),
    )
    problems = []
    for n in names:
        if staged[n] > 0 and modelled[n] == 0:
            problems.append(
                f"financials.{n}: {staged[n]:,} non-null in staging, 0 in model"
            )
    print(f"   checked {len(names)} columns in year {year}")
    print(
        f"   {'OK  no column lost to safe_cast' if not problems else 'BAD ' + str(len(problems)) + ' column(s) emptied'}"
    )
    for p in problems[:10]:
        print(f"      {p}")
    return problems


def check_wide_against_long(bq: bigquery.Client) -> list[str]:
    """The same figure reached two ways must agree."""
    print("\n4. wide against long")
    query = f"""
    with w as (
      select year, quarter, cert, total_assets, total_deposits, net_income
      from `{PROJECT}.{DATASET}.financials`
      where year = 2026 and quarter = 2
    ),
    l as (
      select year, quarter, cert,
             max(if(indicator_id = 'ASSET', value, null)) asset,
             max(if(indicator_id = 'DEP', value, null)) dep,
             max(if(indicator_id = 'NETINC', value, null)) netinc
      from `{PROJECT}.{DATASET}.financials_indicator`
      where year = 2026 and quarter = 2
      group by 1, 2, 3
    )
    select count(*) n,
           countif(w.total_assets != l.asset) bad_assets,
           countif(w.total_deposits != l.dep) bad_deposits,
           countif(w.net_income != l.netinc) bad_income
    from w join l using (year, quarter, cert)
    """
    row = next(iter(bq.query(query).result()))
    bad = row["bad_assets"] + row["bad_deposits"] + row["bad_income"]
    status = "OK " if bad == 0 else "BAD"
    print(
        f"   {status} {row['n']:,} institution-quarters compared; "
        f"mismatches: assets={row['bad_assets']} deposits={row['bad_deposits']} "
        f"income={row['bad_income']}"
    )
    return [] if bad == 0 else [f"wide/long disagree on {bad} value(s)"]


def main() -> None:
    bq = client()
    problems: list[str] = []
    problems += check_row_counts(bq)
    problems += check_coverage(bq)
    problems += check_safe_cast(bq)
    problems += check_wide_against_long(bq)

    print()
    if problems:
        print(f"{len(problems)} PROBLEM(S):")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print("all checks passed")


if __name__ == "__main__":
    main()
