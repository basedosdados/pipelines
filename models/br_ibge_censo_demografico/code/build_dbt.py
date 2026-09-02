"""Generate dbt SQL models and append 2022 entries to schema.yml.

Usage:
    uv run python models/br_ibge_censo_demografico/code/build_dbt.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import constants as c

MODEL_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = MODEL_DIR / "schema.yml"
DATASET = c.DATASET_ID

CAST = {
    "INT64": "int64",
    "FLOAT64": "float64",
    "STRING": "string",
}

KEYS = {
    "microdados_domicilio_2022": ["ano", "sigla_uf", "controle"],
    "microdados_pessoa_2022": ["ano", "sigla_uf", "controle", "numero_ordem"],
    "microdados_familia_2022": ["ano", "sigla_uf", "controle", "numero_ordem"],
    "microdados_mortalidade_2022": [
        "ano",
        "sigla_uf",
        "controle",
        "numero_ordem",
    ],
}

DIR_FK = {
    "sigla_uf": ("br_bd_diretorios_brasil__uf", "sigla"),
    "ano": ("br_bd_diretorios_data_tempo__ano", "ano"),
}

MARKER_START = "# --- censo 2022 public microdata (generated) ---"
MARKER_END = "# --- end censo 2022 public microdata ---"


def read_architecture(slug: str) -> list[dict[str, str]]:
    with (c.ARCHITECTURE_DIR / f"{slug}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_sql(slug: str, columns: list[dict[str, str]]) -> None:
    casts = []
    for col in columns:
        bq = CAST[col["bigquery_type"]]
        casts.append(f"    safe_cast({col['name']} as {bq}) {col['name']}")
    body = ",\n".join(casts)
    sql = f"""{{{{
    config(
        alias="{slug}",
        schema="{DATASET}",
        materialized="table",
        cluster_by=["sigla_uf"],
    )
}}}}
select
{body}
from
    {{{{
        set_datalake_project(
            "{DATASET}_staging.{slug}"
        )
    }}}} as t
"""
    dest = MODEL_DIR / f"{DATASET}__{slug}.sql"
    dest.write_text(sql, encoding="utf-8")
    print(f"wrote {dest.name}")


def schema_fragment(slug: str, columns: list[dict[str, str]]) -> str:
    key = KEYS[slug]
    desc = c.TABLES[
        next(sheet for sheet, spec in c.TABLES.items() if spec["slug"] == slug)
    ]["description"]
    lines = [
        f"  - name: {DATASET}__{slug}",
        "    description: >",
        f"      {desc}",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        "          combination_of_columns:",
    ]
    for col in key:
        lines.append(f"            - {col}")
    # Wide sample-microdata tables are skip-pattern sparse; 2010 models
    # also omit not_null_proportion. Unique + key not_null stay.
    lines.append("    columns:")
    for col in columns:
        text = (col["description"] or "").replace("\n", " ").strip() or col[
            "name"
        ]
        lines.append(f"      - name: {col['name']}")
        lines.append("        description: >")
        lines.append(f"          {text}")
        tests: list[str] = []
        if col["name"] in key:
            tests.append("          - not_null")
        dest = DIR_FK.get(col["name"])
        if dest:
            model, field = dest
            tests.append("          - relationships:")
            tests.append(f"              to: ref('{model}')")
            tests.append(f"              field: {field}")
        if tests:
            lines.append("        tests:")
            lines.extend(tests)
    return "\n".join(lines) + "\n"


def write_schema(fragments: list[str]) -> None:
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    block = MARKER_START + "\n" + "".join(fragments) + MARKER_END + "\n"
    if MARKER_START in text and MARKER_END in text:
        start = text.index(MARKER_START)
        end = text.index(MARKER_END) + len(MARKER_END)
        # keep surrounding newlines tidy
        prefix = text[:start].rstrip("\n") + "\n"
        suffix = text[end:].lstrip("\n")
        text = prefix + block + suffix
    else:
        text = text.rstrip("\n") + "\n" + block
    SCHEMA_PATH.write_text(text, encoding="utf-8")
    print(f"updated {SCHEMA_PATH.name}")


def main() -> None:
    fragments = []
    for spec in c.TABLES.values():
        slug = spec["slug"]
        columns = read_architecture(slug)
        write_sql(slug, columns)
        fragments.append(schema_fragment(slug, columns))
    write_schema(fragments)


if __name__ == "__main__":
    main()
