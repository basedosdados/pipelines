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

from pipelines.datasets.us_fbi_cde.constants import constants
from pipelines.datasets.us_fbi_cde.spec import TABLES

OUTPUT = constants.DATA_ROOT.value / "output"
DICIONARIO = Path(__file__).resolve().parent / "dicionario.csv"

# child table -> (child column, parent table, parent column). Every join also
# carries year and state_abbr: the FBI's ids are unique within a state-year, not
# globally, and joining on the id alone silently mixes states.
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


# Patterns BigQuery's SAFE_CAST actually accepts from a string. Anything else
# becomes NULL without an error, which is how a whole column goes missing
# quietly. duckdb's TRY_CAST is more permissive than BigQuery's, so the shapes
# are matched with a regex rather than delegated to a cast.
CAST_PATTERNS = {
    "INT64": r"^-?[0-9]+$",
    "FLOAT64": r"^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$",
    "DATE": r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
}


def check_cast_survival(con, counts, failures):
    """Report any typed column whose staging values would not survive safe_cast."""
    print(
        "\ncast survival (staging is all-STRING; the dbt model safe_casts it)"
    )
    problems = 0
    for table, spec in TABLES.items():
        if table not in counts or table == "dicionario":
            continue
        for column in spec["columns"]:
            pattern = CAST_PATTERNS.get(column["bigquery_type"])
            if pattern is None:
                continue
            name = column["name"]
            if name in spec["partitions"]:
                continue  # hive keys are typed by the reader, not the cast
            total, bad = con.execute(
                f"select count({name}), "
                f"count(*) filter (where {name} is not null "
                f"and not regexp_matches({name}, '{pattern}')) "
                f"from {source(table)}"
            ).fetchone()
            if not bad:
                continue
            problems += 1
            examples = con.execute(
                f"select distinct {name} from {source(table)} "
                f"where {name} is not null and not regexp_matches({name}, '{pattern}') "
                f"limit 5"
            ).fetchall()
            values = ", ".join(repr(e[0]) for e in examples)
            share = bad / max(total, 1)
            print(
                f"  {table}.{name} ({column['bigquery_type']}): "
                f"{bad:,} of {total:,} ({share:.2%}) would become NULL — {values}"
            )
            failures.append(
                f"{table}.{name}: {share:.2%} of non-null values fail "
                f"safe_cast to {column['bigquery_type']}"
            )
    if not problems:
        print("  every typed column survives the cast intact")


def check_null_sentinels(con, counts, failures):
    """Look for the literal string "NULL" left in any string column.

    The employee extract writes "NULL" where the NIBRS bundles leave the field
    empty. The reader treats both as missing, but a source file that starts
    using the sentinel somewhere new would otherwise publish it as a value.
    """
    print('\nliteral "NULL" sentinels')
    found = False
    for table, spec in TABLES.items():
        if table not in counts:
            continue
        for column in spec["columns"]:
            if column["bigquery_type"] != "STRING":
                continue
            name = column["name"]
            if name in spec["partitions"]:
                continue
            hits = con.execute(
                f"select count(*) from {source(table)} where {name} = 'NULL'"
            ).fetchone()[0]
            if hits:
                found = True
                print(
                    f"  {table}.{name}: {hits:,} rows hold the string 'NULL'"
                )
                failures.append(
                    f"{table}.{name}: {hits:,} literal 'NULL' strings"
                )
    if not found:
        print("  none")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-references", action="store_true")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-limit", default="6GB")
    args = parser.parse_args()
    con = duckdb.connect()
    # Bounded on purpose: the referential checks anti-join 200-million-row
    # tables, and duckdb will happily take the whole machine. It spills to disk
    # past the limit instead.
    con.execute(f"pragma threads={args.threads}")
    con.execute(f"pragma memory_limit='{args.memory_limit}'")
    con.execute(f"pragma temp_directory='{OUTPUT.parent / 'duckdb_tmp'}'")
    failures = []

    print(f"{'table':32s} {'rows':>15s}  years")
    counts = {}
    for table in TABLES:
        path = OUTPUT / table
        if not path.exists():
            print(f"{table:32s} {'(absent)':>15s}")
            continue
        if table == "dicionario":
            rows = con.execute(
                f"select count(*) from {source(table)}"
            ).fetchone()[0]
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
            failures.append(
                f"{table}: {duplicates:,} duplicate keys on ({key})"
            )

    if not args.skip_references:
        print("\nreferential integrity")
        for child, child_col, parent, parent_col in REFERENCES:
            if child not in counts or parent not in counts:
                continue
            orphans = con.execute(
                f"select count(*) from {source(child)} c "
                f"anti join {source(parent)} p "
                f"on c.year = p.year and c.state_abbr = p.state_abbr "
                f"and c.{child_col} = p.{parent_col} "
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
        coded = [
            c["name"]
            for c in spec["columns"]
            if c["covered_by_dictionary"] == "yes"
        ]
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
                print(
                    f"  {table}.{column}: {uncovered} codes with no entry ({values})"
                )
                failures.append(
                    f"{table}.{column}: {uncovered} codes absent from the dictionary"
                )
    print("  every other coded column is fully covered")

    check_cast_survival(con, counts, failures)
    check_null_sentinels(con, counts, failures)

    if "ucr_summary" in counts and "agency" in counts:
        print("\nthe one derived join: ucr_summary.ori -> agency.ori")
        matched, total = con.execute(
            "select count(*) filter (where p.ori is not null), count(*) from "
            f"(select distinct year, ori from {source('ucr_summary')}) s "
            f"left join (select distinct year, ori from {source('agency')}) p "
            "on s.year = p.year and s.ori = p.ori"
        ).fetchone()
        print(
            f"  {matched:,} of {total:,} agency-years join ({matched / max(total, 1):.1%})"
        )
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
