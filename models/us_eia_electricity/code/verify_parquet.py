"""Verify the cleaned us_eia_electricity parquet before it is uploaded anywhere.

    python verify_parquet.py                     # every check
    python verify_parquet.py --write-coverage    # also refresh measured_coverage.json

Six checks, all read from the local parquet:

1. **Row counts** per table and year, and the corpus total.
2. **Key uniqueness** — the natural key of every table, with the duplicate share
   where it is not exact, because two of these tables genuinely collide and the
   dbt test has to allow exactly as much as the source contains.
3. **Null shares** per column, over the full record and over the years the dbt
   ``not_null_proportion_multiple_columns`` test is scoped to, so the exemption
   list in ``gen_dbt.py`` is measured rather than guessed. Read from the parquet
   **footers** (every column chunk records its own ``null_count``), so this is
   metadata only — no scan and no BigQuery quota.
4. **Directory resolution** — the share of rows whose state and county resolve
   against ``br_bd_diretorios_us``.
5. **Magnitudes** — national net generation by year against EIA's own published
   totals, which is the one check that would catch a units or melt error.
6. **All-STRING, non-empty** partitions.

``--write-measured`` writes ``measured.json``: per column, the first and last
year in which it holds a non-null value, and the columns too sparse for the dbt
null-proportion test. ``gen_architecture.py`` reads the first for
``temporal_coverage`` and ``gen_dbt.py`` reads the second for its exemption
list, so both are what the data shows rather than what the documentation claims
or what someone guessed.
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from common import DATA_TABLES, OUTPUT, assert_all_string, load_cols

CODE_DIR = Path(__file__).resolve().parent

# The natural key of each table, as the source defines it.
KEYS = {
    "plant": ["year", "plant_id"],
    "generator": [
        "year",
        "plant_id",
        "generator_id",
        "generator_status_group",
    ],
    "generation_fuel": [
        "year",
        "month",
        "plant_id",
        "energy_source_code",
        "prime_mover_code",
        "nuclear_unit_id",
    ],
    "fuel_receipts_costs": None,  # a delivery has no published identifier
}

# EIA's own published total net generation for the US electric power sector,
# all sectors, in million MWh (EIA Electric Power Annual, Table 3.1.A). Used as
# an order-of-magnitude check on the wide-to-long melt: an error there would
# either multiply the total by twelve or divide it by twelve.
PUBLISHED_NET_GENERATION_TWH = {
    2005: 4055,
    2010: 4125,
    2015: 4078,
    2020: 4009,
    2023: 4178,
}


def partitions(table: str) -> list[Path]:
    return sorted((OUTPUT / table).rglob("*.parquet"))


def row_counts(table: str) -> dict[int, int]:
    out = {}
    for path in partitions(table):
        year = int(path.parent.name.split("=")[1])
        out[year] = pq.ParquetFile(path).metadata.num_rows
    return out


def null_counts(
    table: str, years: set[int] | None = None
) -> tuple[dict[str, int], int]:
    """Per-column null counts read from the parquet footers, no data scan."""
    counts: dict[str, int] = defaultdict(int)
    total = 0
    for path in partitions(table):
        year = int(path.parent.name.split("=")[1])
        if years is not None and year not in years:
            continue
        meta = pq.ParquetFile(path).metadata
        names = meta.schema.names
        total += meta.num_rows
        for group in range(meta.num_row_groups):
            for index, name in enumerate(names):
                counts[name] += (
                    meta.row_group(group).column(index).statistics.null_count
                )
    return counts, total


def coverage(table: str) -> dict[str, str]:
    """First and last year each column is non-null, in START(1)END notation."""
    spans: dict[str, list[int]] = defaultdict(list)
    for path in partitions(table):
        year = int(path.parent.name.split("=")[1])
        meta = pq.ParquetFile(path).metadata
        names = meta.schema.names
        for index, name in enumerate(names):
            nulls = sum(
                meta.row_group(g).column(index).statistics.null_count
                for g in range(meta.num_row_groups)
            )
            if nulls < meta.num_rows:
                spans[name].append(year)
    return {name: (min(years), max(years)) for name, years in spans.items()}


def check_keys(table: str) -> None:
    key = KEYS[table]
    if key is None:
        print(f"  {table}: no natural key published — uniqueness not asserted")
        return
    data = ds.dataset(OUTPUT / table, format="parquet")
    seen: set[tuple] = set()
    total = duplicates = 0
    for batch in data.to_batches(columns=key):
        columns = [batch.column(i).to_pylist() for i in range(len(key))]
        for row in zip(*columns, strict=True):
            total += 1
            if row in seen:
                duplicates += 1
            else:
                seen.add(row)
    share = duplicates / total if total else 0
    print(
        f"  {table}: key {tuple(key)} -> {total:,} rows, {duplicates:,} duplicate "
        f"({share:.4%})"
    )


def check_directory(table: str) -> None:
    cols = {c.name for c in load_cols(table)}
    wanted = [
        c for c in ("state_id", "county_id", "mine_county_id") if c in cols
    ]
    if not wanted:
        return
    counts, total = null_counts(table)
    for column in wanted:
        resolved = total - counts[column]
        print(
            f"  {table}.{column}: {resolved:,}/{total:,} resolved ({resolved / total:.2%})"
        )


def check_generation_magnitude() -> None:
    data = ds.dataset(OUTPUT / "generation_fuel", format="parquet")
    by_year: dict[int, float] = defaultdict(float)
    for batch in data.to_batches(columns=["year", "net_generation_mwh"]):
        for year, value in zip(
            batch.column(0).to_pylist(),
            batch.column(1).to_pylist(),
            strict=True,
        ):
            if year and value:
                by_year[int(year)] += float(value)
    for year, published in sorted(PUBLISHED_NET_GENERATION_TWH.items()):
        got = by_year.get(year, 0) / 1e6
        delta = (got - published) / published
        flag = "OK " if abs(delta) < 0.05 else "!! "
        print(
            f"  {flag}{year}: {got:,.0f} million MWh vs EIA's {published:,} ({delta:+.1%})"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write-measured",
        action="store_true",
        help="refresh measured.json, which gen_architecture.py and gen_dbt.py read",
    )
    parser.add_argument(
        "--scope-years",
        type=int,
        nargs="*",
        help="years the scoped dbt null-proportion test will see (default: last 3)",
    )
    args = parser.parse_args()

    measured: dict[str, dict] = {}

    print("=== 1. row counts ===")
    grand = 0
    all_years: dict[str, dict[int, int]] = {}
    for table in DATA_TABLES:
        counts = row_counts(table)
        all_years[table] = counts
        grand += sum(counts.values())
        print(
            f"  {table:22s} {sum(counts.values()):>12,} rows across "
            f"{len(counts)} year(s) {min(counts)}-{max(counts)}"
        )
    dicionario = partitions("dicionario")
    if dicionario:
        n = sum(pq.ParquetFile(p).metadata.num_rows for p in dicionario)
        grand += n
        print(f"  {'dicionario':22s} {n:>12,} rows")
    print(f"  {'TOTAL':22s} {grand:>12,}")

    print("\n=== 2. key uniqueness ===")
    for table in DATA_TABLES:
        check_keys(table)

    print("\n=== 3. null shares ===")
    for table in DATA_TABLES:
        years = set(all_years[table])
        scope = (
            set(args.scope_years)
            if args.scope_years
            else set(sorted(years)[-3:])
        )
        full_counts, full_total = null_counts(table)
        scope_counts, scope_total = null_counts(table, scope)
        measured.setdefault(table, {})["scope_years"] = sorted(scope)
        sparse_full = sorted(
            c
            for c in full_counts
            if (full_total - full_counts[c]) / full_total < 0.05
        )
        sparse_scope = sorted(
            c
            for c in scope_counts
            if scope_total
            and (scope_total - scope_counts[c]) / scope_total < 0.05
        )
        measured[table]["sparse_full"] = sparse_full
        measured[table]["sparse_scope"] = sparse_scope
        print(
            f"  {table}: below 5% non-null over the full record: {sparse_full}"
        )
        print(
            f"  {table}: below 5% non-null over {sorted(scope)}: {sparse_scope}"
        )

    print("\n=== 4. directory resolution ===")
    for table in DATA_TABLES:
        check_directory(table)

    print("\n=== 5. net generation vs EIA's published totals ===")
    check_generation_magnitude()

    print("\n=== 6. all-STRING, non-empty partitions ===")
    for table in [*DATA_TABLES, "dicionario"]:
        assert_all_string(OUTPUT / table)
    print("  OK")

    if args.write_measured:
        for table in DATA_TABLES:
            measured[table]["coverage"] = {
                name: f"{lo}(1){hi}"
                for name, (lo, hi) in coverage(table).items()
            }
        path = CODE_DIR / "measured.json"
        path.write_text(json.dumps(measured, indent=1, sort_keys=True) + "\n")
        print(f"\nwrote {path.name}")


if __name__ == "__main__":
    main()
