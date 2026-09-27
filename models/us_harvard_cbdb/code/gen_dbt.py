"""Generate dbt SQL models + schema.yml for us_harvard_cbdb from schema_spec.

Tables are unpartitioned. All FKs verified 0-orphan, so relationships tests are
safe. association is the only non-unique table (0.04%) -> relaxed custom test.
"""

import os

# pyrefly: ignore [missing-import]
from schema_spec import TABLE_ORDER, TABLES

MODELS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)  # models/us_harvard_cbdb
DS = "us_harvard_cbdb"

BQ = {"STRING": "string", "INT64": "int64", "FLOAT64": "float64"}

# A dbt_utils `relationships` test to a table whose grain column shares the
# table's name compiles to `select <name> from ...<name>`, which BigQuery binds
# to the table STRUCT (not the column) -> "STRING = STRUCT" error. These four
# code tables all have that property, so relationships tests targeting them are
# skipped. FK integrity to them is verified 0-orphan at clean time and the
# dataset is frozen; the intra-dataset FK is documented in column observations.
NO_REL_TARGETS = {
    "address_code",
    "kinship_code",
    "office_code",
    "association_code",
}

# Per-table test config (calibrated against the cleaned parquet)
P = "person"
TESTS = {
    "person": dict(
        uniq=["person_id"],
        relaxed=False,
        not_null=["person_id"],
        rel={"index_address_code": ("address_code", "address_code")},
        ignore=["flourished_earliest_year", "flourished_latest_year"],
    ),
    "kinship": dict(
        uniq=["person_id", "kin_person_id", "kinship_code"],
        relaxed=False,
        not_null=["person_id", "kin_person_id", "kinship_code"],
        rel={
            "person_id": (P, "person_id"),
            "kin_person_id": (P, "person_id"),
            "kinship_code": ("kinship_code", "kinship_code"),
        },
        ignore=[],
    ),
    "office_posting": dict(
        uniq=["office_code", "posting_id"],
        relaxed=False,
        not_null=["person_id", "office_code", "posting_id"],
        rel={
            "person_id": (P, "person_id"),
            "office_code": ("office_code", "office_code"),
        },
        ignore=[],
    ),
    "association": dict(
        uniq=[
            "person_id",
            "assoc_person_id",
            "association_code",
            "kinship_code",
            "kin_person_id",
            "assoc_kinship_code",
            "assoc_kin_person_id",
            "text_title",
            "first_year",
        ],
        relaxed=True,
        not_null=[
            "person_id",
            "assoc_person_id",
            "association_code",
            "text_title",
        ],
        rel={
            "person_id": (P, "person_id"),
            "assoc_person_id": (P, "person_id"),
            "kin_person_id": (P, "person_id"),
            "assoc_kin_person_id": (P, "person_id"),
            "association_code": ("association_code", "association_code"),
            "kinship_code": ("kinship_code", "kinship_code"),
            "assoc_kinship_code": ("kinship_code", "kinship_code"),
            "address_code": ("address_code", "address_code"),
        },
        ignore=["last_year"],
    ),
    "address": dict(
        uniq=["person_id", "address_code", "address_type_code", "sequence"],
        relaxed=False,
        not_null=[
            "person_id",
            "address_code",
            "address_type_code",
            "sequence",
        ],
        rel={
            "person_id": (P, "person_id"),
            "address_code": ("address_code", "address_code"),
        },
        ignore=["first_year", "last_year"],
    ),
    "office_code": dict(
        uniq=["office_code"],
        relaxed=False,
        not_null=["office_code"],
        rel={},
        ignore=["name_english_alt"],
    ),
    "address_code": dict(
        uniq=["address_code"],
        relaxed=False,
        not_null=["address_code"],
        rel={},
        ignore=[],
    ),
    "kinship_code": dict(
        uniq=["kinship_code"],
        relaxed=False,
        not_null=["kinship_code"],
        rel={},
        ignore=[],
    ),
    "association_code": dict(
        uniq=["association_code"],
        relaxed=False,
        not_null=["association_code"],
        rel={},
        ignore=[],
    ),
    "dicionario": dict(
        uniq=["id_tabela", "nome_coluna", "chave"],
        relaxed=False,
        not_null=[],
        rel={},
        ignore=[],
    ),
}


def sql_model(name):
    spec = TABLES[name]
    lines = []
    for c in spec["columns"]:
        lines.append(
            f"    safe_cast({c['name']} as {BQ[c['type']]}) {c['name']},"
        )
    lines[-1] = lines[-1].rstrip(",")
    body = "\n".join(lines)
    return f'''{{{{
    config(
        schema="{DS}",
        alias="{name}",
        materialized="table",
    )
}}}}


select
{body}
from
    {{{{ set_datalake_project("{DS}_staging.{name}") }}}}
    as t
'''


def yaml_block(s, indent):
    """Wrap a description in a > block scalar."""
    pad = " " * indent
    return f">\n{pad}  {s}"


def schema_yml():
    out = ["---", "version: 2", "models:"]
    for name in TABLE_ORDER:
        spec = TABLES[name]
        tc = TESTS[name]
        out.append(f"  - name: {DS}__{name}")
        out.append(f"    description: {yaml_block(spec['desc_pt'], 4)}")
        out.append("    tests:")
        if tc["relaxed"]:
            out.append("      - custom_unique_combinations_of_columns:")
            out.append(
                f"          combination_of_columns: [{', '.join(tc['uniq'])}]"
            )
            out.append("          proportion_allowed_failures: 0.05")
        else:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(
                f"          combination_of_columns: [{', '.join(tc['uniq'])}]"
            )
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if tc["ignore"]:
            out.append("          ignore_values:")
            for c in tc["ignore"]:
                out.append(f"            - {c}")
        out.append("    columns:")
        for c in spec["columns"]:
            out.append(f"      - name: {c['name']}")
            out.append(f"        description: {yaml_block(c['pt'], 8)}")
            tests = []
            if c["name"] in tc["not_null"]:
                tests.append("not_null")
            rel = tc["rel"].get(c["name"])
            if rel and rel[0] in NO_REL_TARGETS:
                rel = None
            if tests or rel:
                out.append("        tests:")
                for t in tests:
                    out.append(f"          - {t}")
                if rel:
                    out.append("          - relationships:")
                    out.append(f"              to: ref('{DS}__{rel[0]}')")
                    out.append(f"              field: {rel[1]}")
        out.append("")
    return "\n".join(out) + "\n"


def main():
    for name in TABLE_ORDER:
        p = os.path.join(MODELS, f"{DS}__{name}.sql")
        with open(p, "w", encoding="utf-8") as f:
            f.write(sql_model(name))
        print("wrote", os.path.basename(p))
    p = os.path.join(MODELS, "schema.yml")
    with open(p, "w", encoding="utf-8") as f:
        f.write(schema_yml())
    print("wrote schema.yml")


if __name__ == "__main__":
    main()
