"""Validate the cleaned parquet before it is uploaded.

Checks the claims the dataset makes about itself, cheaply and locally, so a
defect surfaces here rather than after a 13 GB upload and a full BigQuery
materialisation:

* row counts and year coverage per table;
* uniqueness of each table's declared key;
* referential integrity of the child tables against their parents;
* that the coded columns only ever hold codes the dictionary defines;
* that the ORI on ucr_summary actually joins to the agency table, which is the
  one derived join in the dataset and the one most likely to be wrong.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fbi_cde.constants import constants  # noqa: E402
from pipelines.datasets.us_fbi_cde.spec import TABLES  # noqa: E402

OUTPUT = constants.DATA_ROOT.value / "output"
DICIONARIO = Path(__file__).resolve().parent / "dicionario.csv"

# child table -> (child column, parent table, parent column)
REFERENCES = [
    ("offense", "incident_id", "incident", "incident_id"),
    ("offender", "incident_id", "incident", "incident_id"),
    ("victim", "incident_id", "incident", "incident_id"),
    ("property", "incident_id", "incident", "incident_id"),
    ("victim_offense", "victim_id", "victim", "victim_id"),
    ("victim_offense", "offense_id", "offense", "offense_id"),
    ("victim_offender_relationship", "victim_id", "victim", "victim_id"),
    ("victim_offender_relationship", "offender_id", "offender", "offender_id"),
]


def source(table):
    return f"read_parquet('{OUTPUT}/{table}/**/*.parquet', hive_partitioning=true)"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-references", action="store_true")
    args = parser.parse_args()
    con = duckdb.connect()
    con.execute("pragma threads=4")
    failures = []

    print(f"{'table':32s} {'rows':>15s}  years")
    counts = {}
    for table in TABLES:
        path = OUTPUT / table
        if not path.exists():
            print(f"{table:32s} {'(absent)':>15s}")
            continue
        if table == "dicionario":
            rows = con.execute(f"select count(*) from {source(table)}").fetchone()[0]
            counts[table] = rows
            print(f"{table:32s} {rows:>15,}")
            continue
        rows, first, last = con.execute(
            f"select count(*), min(year), max(year) from {source(table)}"
        ).fetchone()
        counts[table] = rows
        print(f"{table:32s} {rows:>15,}  {first}-{last}")
        declared = TABLES[table]
        if first != declared["first_year"] or last != declared["last_year"]:
            failures.append(
                f"{table}: years {first}-{last}, spec declares "
                f"{declared['first_year']}-{declared['last_year']}"
            )

    print("\nkey uniqueness")
    for table, spec in TABLES.items():
        if table not in counts:
            continue
        key = ", ".join(spec["unique_key"])
        duplicates = con.execute(
            f"select count(*) from (select {key} from {source(table)} "
            f"group by {key} having count(*) > 1)"
        ).fetchone()[0]
        status = "ok" if duplicates == 0 else f"{duplicates:,} duplicate keys"
        print(f"  {table:32s} ({key}): {status}")
        if duplicates:
            failures.append(f"{table}: {duplicates:,} duplicate keys on ({key})")

    if not args.skip_references:
        print("\nreferential integrity")
        for child, child_col, parent, parent_col in REFERENCES:
            if child not in counts or parent not in counts:
                continue
            orphans = con.execute(
                f"select count(*) from {source(child)} c "
                f"anti join {source(parent)} p "
                f"on c.year = p.year and c.{child_col} = p.{parent_col} "
                f"where c.{child_col} is not null"
            ).fetchone()[0]
            share = orphans / max(counts[child], 1)
            print(
                f"  {child}.{child_col} -> {parent}.{parent_col}: "
                f"{orphans:,} orphans ({share:.4%})"
            )
            if share > 0.001:
                failures.append(
                    f"{child}.{child_col}: {share:.2%} of rows have no parent in {parent}"
                )

    print("\ndictionary coverage")
    con.execute(
        f"create table dic as select * from read_csv_auto('{DICIONARIO}', header=true)"
    )
    for table, spec in TABLES.items():
        if table not in counts or table == "dicionario":
            continue
        coded = [c["name"] for c in spec["columns"] if c["covered_by_dictionary"] == "yes"]
        for column in coded:
            uncovered = con.execute(
                f"select count(distinct t.{column}) from {source(table)} t "
                f"left join dic d on d.id_tabela = '{table}' "
                f"and d.nome_coluna = '{column}' and d.chave = t.{column} "
                f"where t.{column} is not null and d.chave is null"
            ).fetchone()[0]
            if uncovered:
                missing = con.execute(
                    f"select distinct t.{column} from {source(table)} t "
                    f"left join dic d on d.id_tabela = '{table}' "
                    f"and d.nome_coluna = '{column}' and d.chave = t.{column} "
                    f"where t.{column} is not null and d.chave is null limit 8"
                ).fetchall()
                values = ", ".join(repr(m[0]) for m in missing)
                print(f"  {table}.{column}: {uncovered} codes with no entry ({values})")
                failures.append(f"{table}.{column}: {uncovered} codes absent from the dictionary")
    print("  every other coded column is fully covered")

    if "ucr_summary" in counts and "agency" in counts:
        print("\nthe one derived join: ucr_summary.ori -> agency.ori")
        matched, total = con.execute(
            "select count(*) filter (where p.ori is not null), count(*) from "
            f"(select distinct year, ori from {source('ucr_summary')}) s "
            f"left join (select distinct year, ori from {source('agency')}) p "
            "on s.year = p.year and s.ori = p.ori"
        ).fetchone()
        print(f"  {matched:,} of {total:,} agency-years join ({matched / max(total, 1):.1%})")
        if matched / max(total, 1) < 0.9:
            failures.append(
                f"ucr_summary.ori joins to agency for only {matched / total:.1%} of agency-years"
            )

    print()
    if failures:
        print("FAILURES")
        for failure in failures:
            print("  -", failure)
        raise SystemExit(1)
    print("all checks passed")


if __name__ == "__main__":
    main()
