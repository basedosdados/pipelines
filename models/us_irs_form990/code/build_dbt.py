"""Generate the dbt models and ``schema.yml`` from the architecture CSVs.

Column order and types come from the architecture (the source of truth), so
the models cannot drift from it. ``ignore_values`` for the null-proportion test
is measured from the cleaned parquet (columns whose non-null share falls below
5%), never guessed — pass ``--output-dir`` after a full clean.

    python build_dbt.py --output-dir ~/Downloads/us_irs_form990_data/output

Run ``pre-commit run --files models/us_irs_form990/*`` afterwards.
"""

import argparse
import csv
import glob
import json
from pathlib import Path

import pyarrow.parquet as pq

HERE = Path(__file__).parent
ARCH = HERE / "architecture"
TABLES_JSON = HERE / "tables.json"
OUT = HERE.parent
DATASET = "us_irs_form990"
UPDATED = "2026-09-03"
AT_LEAST = 0.05

STATE_DIR = "ref('br_bd_diretorios_us__state')"
YEAR_DIR = "ref('br_bd_diretorios_data_tempo__ano')"


def cast(name: str, typ: str) -> str:
    return f"    safe_cast({name} as {typ.lower()}) {name},"


def read_arch(table: str) -> list[dict]:
    with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def model_sql(table: str, cols: list[dict]) -> str:
    lines = [cast(c["name"], c["bigquery_type"]) for c in cols]
    lines[-1] = lines[-1].rstrip(",")
    body = "\n".join(lines)
    src = f'{{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}'
    if table == "organization":
        return (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="incremental",\n'
            "        partition_by={\n"
            '            "field": "extraction_date",\n'
            '            "data_type": "date",\n'
            '            "granularity": "day",\n'
            "        },\n    )\n}}\n\n"
            f"-- Atualizado em {UPDATED}\n"
            "-- Each monthly BMF extract is a full snapshot; snapshots stack on\n"
            "-- extraction_date and the incremental build appends new ones only.\n"
            f"select\n{body}\nfrom {src} as t\n"
            "{% if is_incremental() %}\n"
            "    where\n"
            "        safe_cast(extraction_date as date)\n"
            "        > (select max(extraction_date) from {{ this }})\n"
            "{% endif %}\n"
        )
    if table == "return_financial":
        return (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            '            "range": {"start": 2010, "end": 2040, "interval": 1},\n'
            "        },\n    )\n}}\n\n"
            f"-- Atualizado em {UPDATED}\n"
            "-- The IRS releases amended returns and occasionally re-releases a\n"
            "-- filing in a later batch. One return is kept per (ein, year,\n"
            "-- form_type): the most recently filed, ties broken by object_id, so\n"
            "-- re-loading a batch into staging never duplicates a row.\n"
            f"with staged as (\nselect\n{body}\nfrom {src} as t\n)\n"
            "select *\nfrom staged\n"
            "qualify\n"
            "    row_number() over (\n"
            "        partition by ein, year, form_type\n"
            "        order by return_timestamp desc, object_id desc\n"
            "    )\n"
            "    = 1\n"
        )
    if table == "compensation":
        return (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            '            "range": {"start": 2010, "end": 2040, "interval": 1},\n'
            "        },\n    )\n}}\n\n"
            f"-- Atualizado em {UPDATED}\n"
            "-- Restricted to the filings kept in return_financial (one per ein,\n"
            "-- year and form_type), so amended or re-released returns do not\n"
            "-- list their officers twice.\n"
            f"select\n{body}\nfrom {src} as t\n"
            "where\n"
            "    safe_cast(object_id as string) in (\n"
            f"        select object_id from {{{{ ref('{DATASET}__return_financial') }}}}\n"
            "    )\n"
        )
    if table == "revocation":
        return (
            "{{\n    config(\n"
            f'        schema="{DATASET}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "revocation_date",\n'
            '            "data_type": "date",\n'
            '            "granularity": "month",\n'
            "        },\n    )\n}}\n\n"
            f"-- Atualizado em {UPDATED}\n"
            f"select\n{body}\nfrom {src} as t\n"
        )
    return (
        f'{{{{ config(alias="{table}", schema="{DATASET}", materialized="table") }}}}\n\n'
        f"-- Atualizado em {UPDATED}\n"
        f"select\n{body}\nfrom {src} as t\n"
    )


def block(text: str, indent: int) -> str:
    pad = " " * indent
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > 76:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    lines.append(cur)
    return ">\n" + "\n".join(pad + ln for ln in lines)


def measure_fill(output_dir: Path, table: str) -> dict[str, float]:
    files = [
        f
        for f in glob.glob(
            str(output_dir / table / "**" / "*.parquet"), recursive=True
        )
        if not f.endswith("00_header.parquet")
    ]
    if not files:
        return {}
    total, nulls, names = 0, {}, []
    for path in files:
        md = pq.ParquetFile(path).metadata
        if not names:
            names = [md.schema.column(i).name for i in range(md.num_columns)]
            nulls = {n: 0 for n in names}
        total += md.num_rows
        for rg in range(md.num_row_groups):
            g = md.row_group(rg)
            for i, n in enumerate(names):
                st = g.column(i).statistics
                nulls[n] += st.null_count if st is not None else 0
    return {n: 1 - nulls[n] / total for n in names} if total else {}


def schema_yml(
    fill: dict[str, dict[str, float]], dictionary_cols: dict[str, set]
) -> str:
    tables = json.loads(TABLES_JSON.read_text(encoding="utf-8"))
    out = ["---", "version: 2", "models:"]
    for table, meta in tables.items():
        cols = read_arch(table)
        out.append(f"  - name: {DATASET}__{table}")
        out.append(f"    description: {block(meta['description_pt'], 6)}")
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            f"          combination_of_columns: [{', '.join(meta['unique_key'])}]"
        )
        sparse = sorted(
            n for n, share in fill.get(table, {}).items() if share < AT_LEAST
        )
        out.append("      - not_null_proportion_multiple_columns:")
        out.append(f"          at_least: {AT_LEAST}")
        if sparse:
            out.append(
                "          # Measured from the cleaned data: legitimately sparse."
            )
            out.append("          ignore_values:")
            out.extend(f"            - {n}" for n in sparse)
        dict_cols = [
            c["name"]
            for c in cols
            if c["covered_by_dictionary"] == "yes"
            and c["name"] in dictionary_cols.get(table, set())
        ]
        if dict_cols:
            out.append("      - custom_dictionary_coverage:")
            out.append("          columns_covered_by_dictionary:")
            out.extend(f"            - {n}" for n in dict_cols)
            out.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
        out.append("    columns:")
        for c in cols:
            out.append(f"      - name: {c['name']}")
            out.append(f"        description: {block(c['description'], 10)}")
            rel = relationship(table, c)
            not_null = c["name"] in meta["not_null"]
            if not_null and not rel:
                out.append("        tests: [not_null]")
            elif rel:
                out.append("        tests:")
                if not_null:
                    out.append("          - not_null")
                out.extend(rel)
    return "\n".join(out) + "\n"


def relationship(table: str, c: dict) -> list[str] | None:
    d = c["directory_column"]
    if not d:
        return None
    if d.startswith("br_bd_diretorios_data_tempo.ano"):
        return [
            "          - relationships:",
            f"              to: {YEAR_DIR}",
            "              field: ano.ano",
        ]
    if d.startswith("br_bd_diretorios_us.state"):
        if table == "return_financial" and c["name"] == "state":
            # Foreign filers carry a province name; US addresses only.
            return [
                "          - relationships:",
                f"              to: {STATE_DIR}",
                "              field: abbreviation",
                "              config:",
                "                where: country is null",
            ]
        if table in ("organization", "revocation"):
            # International records (eo_xx / foreign revocations) carry
            # non-US codes; a small share is tolerated and documented.
            return [
                "          - custom_relationships:",
                f"              to: {STATE_DIR}",
                "              field: abbreviation",
                "              proportion_allowed_failures: 0.005",
            ]
        return [
            "          - relationships:",
            f"              to: {STATE_DIR}",
            "              field: abbreviation",
        ]
    raise SystemExit(f"unknown directory {d}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", type=Path, default=None)
    args = p.parse_args()
    for table in json.loads(TABLES_JSON.read_text(encoding="utf-8")):
        cols = read_arch(table)
        dest = OUT / f"{DATASET}__{table}.sql"
        dest.write_text(model_sql(table, cols), encoding="utf-8")
        print(f"{dest.name}: {len(cols)} columns")
    fill = {}
    if args.output_dir:
        for table in json.loads(TABLES_JSON.read_text(encoding="utf-8")):
            fill[table] = measure_fill(args.output_dir, table)
            low = {
                k: round(v, 3) for k, v in fill[table].items() if v < AT_LEAST
            }
            print(f"{table}: below {AT_LEAST} non-null -> {low}")
    # Which (table, column) pairs the dictionary actually holds.
    dictionary_cols: dict[str, set] = {}
    with open(HERE / "dicionario.csv", newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            dictionary_cols.setdefault(r["id_tabela"], set()).add(
                r["nome_coluna"]
            )
    # ntee_code values carry a 4th character the dictionary does not list.
    dictionary_cols["organization"].discard("ntee_code")
    (OUT / "schema.yml").write_text(
        schema_yml(fill, dictionary_cols), encoding="utf-8"
    )
    print("schema.yml written")


if __name__ == "__main__":
    main()
