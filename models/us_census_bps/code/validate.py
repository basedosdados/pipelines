"""Validate the cleaned us_census_bps Parquet against the survey's own totals.

Runs entirely on the local Parquet with DuckDB, before anything is uploaded.
Checks key uniqueness, the aggregation identities the survey publishes, and
the share of place records that reach the US place directory.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

DATA_DIR = Path(
    os.environ.get(
        "BPS_DATA_DIR", Path.home() / "Downloads/us_census_bps_data"
    )
)
OUT = DATA_DIR / "output"


def src(table: str) -> str:
    """Return a DuckDB scan expression for a table's hive-partitioned Parquet."""
    return (
        f"read_parquet('{OUT}/{table}/**/*.parquet', "
        f"hive_partitioning=false, union_by_name=true)"
    )


KEYS = {
    "permit_place_monthly": ["year", "month", "state_id", "permit_office_id"],
    "permit_place_annual": ["year", "state_id", "permit_office_id"],
    # The county files carry one "Balance of State" record per state, with no
    # FIPS county code, so the state is part of the key.
    "permit_county_monthly": ["year", "month", "state_id", "county_id"],
    "permit_county_annual": ["year", "state_id", "county_id"],
    "permit_cbsa_monthly": ["year", "month", "cbsa_id"],
    "permit_cbsa_annual": ["year", "cbsa_id"],
    "permit_msa_monthly": ["year", "month", "msa_cmsa_id", "pmsa_id"],
    "permit_msa_annual": ["year", "msa_cmsa_id", "pmsa_id"],
    "permit_state_monthly": ["year", "month", "geography_id"],
    "permit_state_annual": ["year", "geography_id"],
}


def main() -> int:
    con = duckdb.connect()
    failures: list[str] = []

    print("=== row counts and key uniqueness ===")
    for table, key in KEYS.items():
        cols = ", ".join([*key, "structure_type"])
        counts = con.execute(
            f"select count(*), count(distinct ({cols})) from {src(table)}"
        ).fetchone()
        assert counts is not None
        n, k = counts
        flag = "OK" if n == k else "DUPLICATE KEYS"
        if n != k:
            failures.append(f"{table}: {n - k:,} duplicate key rows")
        print(f"  {table:24s} {n:>12,} rows  key={flag}")

    print("\n=== survey aggregation identities (units, latest 5 years) ===")
    # Regions and divisions must sum to the published national total.
    rows = con.execute(f"""
        with a as (
          select year, geography_level, sum(cast(units as bigint)) u
          from {src("permit_state_annual")} group by 1, 2
        )
        select year,
               max(u) filter (where geography_level='nation')   as nation,
               max(u) filter (where geography_level='region')   as regions,
               max(u) filter (where geography_level='division') as divisions,
               max(u) filter (where geography_level='state')    as states
        from (select year, geography_level, sum(u) u from a group by 1, 2)
        group by 1 order by year desc limit 5
    """).fetchall()
    for year, nation, regions, divisions, states in rows:
        ok = nation == regions == divisions
        if not ok:
            failures.append(f"state_annual {year}: region/division != nation")
        print(
            f"  {year}  nation={nation:>9,}  regions={regions:>9,}  "
            f"divisions={divisions:>9,}  states={states:>9,}  "
            f"{'OK' if ok else 'MISMATCH'}"
        )

    print("\n=== place records reaching the FIPS place directory ===")
    rows = con.execute(f"""
        select year,
               count(distinct permit_office_id || state_id) offices,
               count(distinct case when place_id is not null
                     then permit_office_id || state_id end) with_place
        from {src("permit_place_annual")}
        where cast(year as int) in (2008, 2015, 2025) group by 1 order by 1
    """).fetchall()
    for year, offices, with_place in rows:
        print(
            f"  {year}: {with_place:,}/{offices:,} offices carry a FIPS place "
            f"code ({with_place / offices * 100:.1f}%)"
        )

    print("\n=== valuation sanity (annual, USD per housing unit) ===")
    rows = con.execute(f"""
        select 'state' lvl, year,
               sum(cast(valuation as bigint)) / nullif(sum(cast(units as bigint)), 0) v
        from {src("permit_state_annual")} where geography_level='nation' and cast(year as int)>=2020
        group by 1,2
        union all
        select 'county', year,
               sum(cast(valuation as bigint)) / nullif(sum(cast(units as bigint)), 0)
        from {src("permit_county_annual")} where cast(year as int)>=2020 group by 1,2
        order by 2, 1
    """).fetchall()
    for lvl, year, value in rows:
        print(f"  {lvl:7s} {year}  ${value:,.0f} per unit")

    print("\n=== cross-level agreement, annual units ===")
    rows = con.execute(f"""
        with p as (
          select year, county_id, sum(cast(units as bigint)) u
          from {src("permit_place_annual")} where county_id is not null
          group by 1, 2
        ),
        c as (
          select year, county_id, sum(cast(units as bigint)) u
          from {src("permit_county_annual")} where county_id is not null
          group by 1, 2
        ),
        j as (select p.year, p.u pu, c.u cu from p join c using (year, county_id))
        select year, count(*) counties, sum(pu) place_units, sum(cu) county_units,
               count(*) filter (where pu = cu) exact
        from j where cast(year as int) in (2000, 2010, 2020, 2025)
        group by 1 order by 1
    """).fetchall()
    for year, counties, place_units, county_units, exact in rows:
        gap = (place_units - county_units) / county_units * 100
        print(
            f"  {year}: {counties:,} counties  place={place_units:,}  "
            f"county={county_units:,}  gap={gap:+.3f}%  "
            f"exact match on {exact:,} ({exact / counties * 100:.1f}%)"
        )

    print("\n=== structure types present ===")
    print(
        "  ",
        con.execute(
            f"select distinct structure_type from {src('permit_state_annual')} "
            "order by 1"
        ).fetchall(),
    )

    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        for line in failures:
            print("  ", line)
        return 1
    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
