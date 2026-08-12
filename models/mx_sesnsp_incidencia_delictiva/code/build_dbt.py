#!/usr/bin/env python3
"""Generate dbt SQL models + schema.yml for both datasets from the architecture CSVs.

  - br_bd_diretorios_mx            (estado, municipio) — unpartitioned directory tables
  - mx_sesnsp_incidencia_delictiva (7 tables)          — partitioned by ano

Follows .claude/rules/dbt-conventions.md: set_datalake_project for staging refs,
safe_cast every column, relationships tests for directory_column FKs, unique
combination + not_null_proportion on every model. Big municipal tables scope tests
with the __most_recent_year_month__ incremental keyword.

Usage:
    uv run python models/mx_sesnsp_incidencia_delictiva/code/build_dbt.py
"""

import csv
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

DATASETS = {
    "br_bd_diretorios_mx": {
        "partition": None,
        "tables": ["estado", "municipio"],
        "arch": REPO / "models/br_bd_diretorios_mx/code/architecture",
        "keys": {  # unique-combination key per table
            "estado": ["id_estado"],
            "municipio": ["id_municipio"],
        },
        "big": set(),
    },
    "mx_sesnsp_incidencia_delictiva": {
        "partition": {
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2015, "end": 2031, "interval": 1},
        },
        "tables": None,  # discover from architecture dir
        "arch": REPO
        / "models/mx_sesnsp_incidencia_delictiva/code/architecture",
        "keys": None,  # full non-cantidad grain
        "big": {
            "municipio_delitos_2015_2025",
            "municipio_victimas",
            "municipio_delitos",
        },
    },
}


def read_arch(arch_dir, table):
    with open(arch_dir / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def sql_model(ds, table, cols, partition):
    lines = [
        f"    safe_cast({c['name']} as {c['bigquery_type'].lower()}) {c['name']},"
        for c in cols
    ]
    lines[-1] = lines[-1].rstrip(",")
    cfg = [
        f'        schema="{ds}",',
        f'        alias="{table}",',
        '        materialized="table",',
    ]
    if partition:
        r = partition["range"]
        cfg.append(
            "        partition_by={\n"
            f'            "field": "{partition["field"]}",\n'
            f'            "data_type": "{partition["data_type"]}",\n'
            f'            "range": {{"start": {r["start"]}, "end": {r["end"]}, "interval": {r["interval"]}}},\n'
            "        },"
        )
    body = "{{\n    config(\n" + "\n".join(cfg) + "\n    )\n}}\n\n"
    body += "select\n" + "\n".join(lines) + "\n"
    body += f'from\n    {{{{ set_datalake_project("{ds}_staging.{table}") }}}}\n    as t\n'
    return body


def schema_yaml(ds, cfg):
    tables = cfg["tables"] or sorted(p.stem for p in cfg["arch"].glob("*.csv"))
    out = ["---", "version: 2", "models:"]
    for table in tables:
        cols = read_arch(cfg["arch"], table)
        names = [c["name"] for c in cols]
        # unique-combination key
        if cfg["keys"]:
            key = cfg["keys"][table]
        else:
            key = [n for n in names if n != "cantidad"]
        big = table in cfg["big"]
        out.append(f"  - name: {ds}__{table}")
        out.append("    description: >")
        desc = f"Tabla {table} del conjunto {ds}."
        if ds == "mx_sesnsp_incidencia_delictiva" and table.startswith("municipio"):
            desc += (
                " Los códigos de municipio agregados del SESNSP (sufijo 998/999, "
                "'No especificado' / 'Otros municipios') no son municipios reales del "
                "INEGI y se excluyen de la prueba de llave foránea de id_municipio "
                "contra br_bd_diretorios_mx.municipio."
            )
        out.append(f"      {desc}")
        out.append("    tests:")
        if big:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(f"          combination_of_columns: {key}")
            out.append("          config:")
            out.append("            where: __most_recent_year_month__")
        else:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(f"          combination_of_columns: {key}")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        out.append("    columns:")
        for c in cols:
            out.append(f"      - name: {c['name']}")
            out.append("        description: >")
            out.append(f"          {c['description']}")
            tests = []
            # not_null for partition + key id columns
            if c["name"] in ("ano", "mes") or (
                c["name"] in key and c["name"].startswith("id_")
            ):
                tests.append("not_null")
            dircol = c["directory_column"]
            # relationships only for intra-project MX directory FKs (refable models)
            if dircol.startswith("br_bd_diretorios_mx."):
                tref, tfield = dircol.split(":")
                _, dtable = tref.split(".")
                out.append("        tests:")
                if "not_null" in tests:
                    out.append("          - not_null")
                out.append("          - relationships:")
                out.append(
                    f"              to: ref('br_bd_diretorios_mx__{dtable}')"
                )
                out.append(f"              field: {tfield}")
                where_parts = []
                if big:
                    where_parts.append("__most_recent_year_month__")
                if dtable == "municipio":
                    # SESNSP aggregate codes (municipio 998/999, "No especificado"/
                    # "Otros municipios") are not real INEGI municipios — ignore in FK test
                    where_parts.append(
                        f"right({c['name']}, 3) not in ('998', '999')"
                    )
                if where_parts:
                    out.append("              config:")
                    out.append(
                        f"                where: {' and '.join(where_parts)}"
                    )
                continue
            if tests:
                out.append("        tests:")
                for t in tests:
                    out.append(f"          - {t}")
    return "\n".join(out) + "\n"


def main():
    for ds, cfg in DATASETS.items():
        ddir = REPO / "models" / ds
        ddir.mkdir(parents=True, exist_ok=True)
        tables = cfg["tables"] or sorted(
            p.stem for p in cfg["arch"].glob("*.csv")
        )
        for table in tables:
            cols = read_arch(cfg["arch"], table)
            (ddir / f"{ds}__{table}.sql").write_text(
                sql_model(ds, table, cols, cfg["partition"])
            )
        (ddir / "schema.yml").write_text(schema_yaml(ds, cfg))
        print(f"{ds}: {len(tables)} models + schema.yml")
        print(
            f"  dbt_project.yml entry:\n    {ds}:\n      +materialized: table\n      +schema: {ds}"
        )


if __name__ == "__main__":
    main()
