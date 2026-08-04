"""Generate dbt models (SQL + schema.yml) for au_alexander_politicians.

Reads the architecture CSVs (source of truth) and emits, into the model dir:
  au_alexander_politicians__<table>.sql   (one per table)
  schema.yml

Conventions: unpartitioned reference/spell tables (small), safe_cast every
column, set_datalake_project for the staging ref, relationships tests for FKs.
"""

import csv
from pathlib import Path

CODE = Path(__file__).resolve().parent
ARCH = CODE / "architecture"
MODEL_DIR = CODE.parent  # models/au_alexander_politicians/
DATASET = "au_alexander_politicians"

TABLES = [
    "politician",
    "party_affiliation",
    "house_member",
    "senator",
    "ministry",
]

# Table-level descriptions (Portuguese; EN/ES generated at metadata step)
TABLE_DESC = {
    "politician": "Dados biográficos e políticos dos parlamentares federais australianos que serviram entre 1901 e 2021, com um registro por político.",
    "party_affiliation": "Filiações partidárias dos parlamentares federais australianos ao longo dos mandatos, com um registro por período de filiação.",
    "house_member": "Mandatos dos parlamentares federais australianos na Câmara dos Representantes, com um registro por período em uma divisão eleitoral.",
    "senator": "Mandatos dos parlamentares federais australianos no Senado, com um registro por período representando um estado ou território.",
    "ministry": "Cargos ministeriais ocupados por parlamentares federais australianos, com um registro por pasta ocupada em um ministério.",
}

# Uniqueness key per table. relaxed=True uses the custom test (allows a small
# proportion of duplicate keys that the source contains).
UNIQUE_KEY = {
    "politician": (["id_politician"], False),
    "party_affiliation": (
        ["id_politician", "party_abbreviation", "date_start"],
        True,
    ),
    "house_member": (["id_politician", "division", "date_start"], False),
    "senator": (["id_politician", "id_state", "date_start"], False),
    "ministry": (
        ["id_politician", "ministry_number", "ministry_title", "date_start"],
        True,
    ),
}

# not_null columns per table (dense keys only)
NOT_NULL = {
    "politician": ["id_politician"],
    "party_affiliation": ["id_politician"],
    "house_member": ["id_politician", "id_state"],
    "senator": ["id_politician", "id_state"],
    "ministry": [],  # id_politician has 2 legitimate source nulls
}

# columns < 5% non-null -> excluded from the proportion test
IGNORE_PROP = {
    "politician": [
        "earlier_or_later_names",
        "title",
        "birth_year",
        "url_adb",
        "comments",
    ],
    "party_affiliation": ["comments"],
    "house_member": ["comments"],
    "senator": ["comments"],
    "ministry": ["comments"],
}

CAST = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date",
    "BOOLEAN": "boolean",
}


def arch(table):
    with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def rel_target(col, table):
    """Return (ref_model, field) for a column that carries a FK, else None."""
    name = col["name"]
    directory = col["directory_column"].strip()
    if directory:  # e.g. br_bd_diretorios_au.state:id_state
        ds_tbl, field = directory.split(":")
        ds, tbl = ds_tbl.rsplit(".", 1)
        return f"{ds}__{tbl}", field
    # satellite id_politician -> master; the master itself is not self-linked
    if name == "id_politician" and table != "politician":
        return f"{DATASET}__politician", "id_politician"
    return None


def gen_sql(table):
    cols = arch(table)
    lines = [
        "{{",
        "    config(",
        f'        alias="{table}",',
        f'        schema="{DATASET}",',
        '        materialized="table",',
        "    )",
        "}}",
        "select",
    ]
    body = []
    for c in cols:
        cast = CAST[c["bigquery_type"]]
        body.append(f"    safe_cast({c['name']} as {cast}) {c['name']}")
    lines.append(",\n".join(body))
    lines.append(
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t'
    )
    (MODEL_DIR / f"{DATASET}__{table}.sql").write_text("\n".join(lines) + "\n")


def y(s):
    """Quote a description for YAML block scalar safety."""
    return s


def gen_schema():
    out = ["---", "version: 2", "models:"]
    for table in TABLES:
        cols = arch(table)
        key, relaxed = UNIQUE_KEY[table]
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >")
        out.append(f"      {TABLE_DESC[table]}")
        out.append("    tests:")
        if relaxed:
            out.append("      - custom_unique_combinations_of_columns:")
            out.append(f"          combination_of_columns: [{', '.join(key)}]")
            out.append("          proportion_allowed_failures: 0.05")
        else:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(f"          combination_of_columns: [{', '.join(key)}]")
        prop_ignore = IGNORE_PROP[table]
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if prop_ignore:
            out.append("          ignore_values:")
            for c in prop_ignore:
                out.append(f"            - {c}")
        out.append("    columns:")
        for c in cols:
            name = c["name"]
            out.append(f"      - name: {name}")
            out.append("        description: >")
            out.append(f"          {c['description']}")
            tests = []
            if name in NOT_NULL[table]:
                tests.append(("not_null", None))
            rel = rel_target(c, table)
            if rel:
                tests.append(("relationships", rel))
            if tests:
                out.append("        tests:")
                for kind, arg in tests:
                    if kind == "not_null":
                        out.append("          - not_null")
                    else:
                        ref_model, field = arg
                        out.append("          - relationships:")
                        out.append(f"              to: ref('{ref_model}')")
                        out.append(f"              field: {field}")
    (MODEL_DIR / "schema.yml").write_text("\n".join(out) + "\n")


def main():
    for t in TABLES:
        gen_sql(t)
    gen_schema()
    print(
        "generated:",
        ", ".join(f"{DATASET}__{t}.sql" for t in TABLES),
        "+ schema.yml",
    )


if __name__ == "__main__":
    main()
