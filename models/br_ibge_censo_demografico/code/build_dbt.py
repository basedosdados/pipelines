"""Generate dbt SQL models and append 2022 entries to schema.yml.

Usage:
    uv run python models/br_ibge_censo_demografico/code/build_dbt.py
"""

from __future__ import annotations

import csv
from pathlib import Path

from models.br_ibge_censo_demografico.code import constants

MODEL_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = MODEL_DIR / "schema.yml"
DATASET = constants.DATASET_ID

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

# Piso de preenchimento por tabela e as colunas dispensadas. O `at_least` é a
# proporção mínima de NÃO nulos: a macro reprova quando
# `nulos / total > 1 - at_least`. As dispensadas são variáveis condicionais do
# questionário — perguntadas só a quem se aplica (migrantes, indígenas e
# quilombolas, ocupados, quem estuda) — e vazias na maioria das linhas por
# desenho da coleta. Medidas na tabela inteira; ver o README do conjunto.
NOT_NULL_AT_LEAST = 0.95
IGNORE_VALUES = {
    "microdados_pessoa_2022": [
        "p0230",
        "p0240",
        "p0250",
        "p0260",
        "p0270",
        "p0280",
        "p0290",
        "p0320",
        "p0330",
        "p0340",
        "p0350",
        "p0360",
        "p0370",
        "p0380",
        "p0390",
        "p0400",
        "p0410",
        "p0490",
        "p0530",
        "p0540",
        "p0550",
        "p0560",
        "p0570",
        "p0600",
        "p0610",
        "p0640",
        "p0660",
        "p0670",
        "p0680",
        "p0690",
        "p0700",
        "p0710",
        "p0720",
        "p0730",
        "p0740",
        "p0800",
        "p0810",
        "p0840",
        "p0850",
        "p0860",
        "p0870",
        "p0880",
        "p0890",
        "p0900",
        "p0930",
        "p0940",
        "p0950",
        "p0960",
        "p0990",
        "p1000",
        "p1010",
        "p1020",
        "p1050",
        "p1060",
        "p1070",
        "p1080",
        "p1090",
        "p1100",
        "p1110",
        "p1120",
        "p1130",
        "p1160",
        "p1170",
        "p1180",
        "p1190",
        "p1220",
    ],
    "microdados_domicilio_2022": ["d0290"],
    # Bloco do responsável pela família única ou convivente principal: vazio
    # para as conviventes secundárias, que são 5,84% do total.
    "microdados_familia_2022": [
        "f0170",
        "f0180",
        "f0190",
        "f0200",
        "f0210",
        "f0230",
        "f0220",
        "f0270",
    ],
    "microdados_mortalidade_2022": [],
}

MARKER_START = "# --- censo 2022 public microdata (generated) ---"
MARKER_END = "# --- end censo 2022 public microdata ---"


def read_architecture(slug: str) -> list[dict[str, str]]:
    with (constants.ARCHITECTURE_DIR / f"{slug}.csv").open(
        encoding="utf-8"
    ) as handle:
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
    desc = constants.TABLES[
        next(
            sheet
            for sheet, spec in constants.TABLES.items()
            if spec["slug"] == slug
        )
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
    lines.append("      - not_null_proportion_multiple_columns:")
    lines.append(f"          at_least: {NOT_NULL_AT_LEAST}")
    dispensadas = IGNORE_VALUES.get(slug, [])
    if dispensadas:
        lines.append("          ignore_values:")
        lines.extend(f"            - {c}" for c in dispensadas)
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
    for spec in constants.TABLES.values():
        slug = spec["slug"]
        columns = read_architecture(slug)
        write_sql(slug, columns)
        fragments.append(schema_fragment(slug, columns))
    write_schema(fragments)


if __name__ == "__main__":
    main()
