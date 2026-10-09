"""Generate the world_openalex dbt models and schema.yml from the architecture.

    uv run python -m models.world_openalex.code.gen_dbt

Writes ``models/world_openalex/world_openalex__<table>.sql`` and
``models/world_openalex/schema.yml``. Run yamlfix afterwards (pre-commit does)
so a regeneration does not churn against the formatter.

Test cost policy: the works tables hold up to 2.9 billion rows, so their tests
are scoped to the last two publication years (``SCOPE``). Every other table is
tested in full.
"""

import csv
from pathlib import Path

from models.world_openalex.code.tables import TABLES

DS = "world_openalex"
HERE = Path(__file__).resolve().parent
MODELS = HERE.parent
ARCH = HERE / "architecture"
SCOPE = "publication_year >= extract(year from current_date()) - 1"
CAST = {
    "INT64": "int64",
    "FLOAT64": "float64",
    "STRING": "string",
    "DATE": "date",
    "BOOLEAN": "bool",
}

# Columns legitimately below the 5% non-null floor of not_null_proportion.
SPARSE = {
    "work": ["pmid", "pmcid", "mag_id", "apc_list_usd", "apc_paid_usd"],
    "work_authorship": ["raw_orcid"],
}
DICTIONARY = {
    "work": ["language"],
    "work_location": ["license"],
    "work_sdg": ["sdg_id"],
}
# Child -> parent foreign keys worth a test (scoped on the child side).
RELATIONSHIPS = {
    "work_authorship": ("work_id", "work", "work_id"),
    "work_location": ("work_id", "work", "work_id"),
    "work_topic": ("topic_id", "topic", "topic_id"),
    "author_topic": ("topic_id", "topic", "topic_id"),
    "topic": ("subfield_id", "subfield", "subfield_id"),
    "subfield": ("field_id", "field", "field_id"),
    "field": ("domain_id", "domain", "domain_id"),
}


def columns(table: str) -> list[dict]:
    """Architecture rows of one table."""
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def sql(table: str) -> str:
    """The dbt model of one table."""
    spec = TABLES[table]
    config = [f'schema="{DS}"', f'alias="{table}"', 'materialized="table"']
    if spec["partition"]:
        col, (start, end) = spec["partition"]
        config.append(
            "partition_by={\n"
            f'            "field": "{col}",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        }"
        )
    if spec["cluster"]:
        config.append(f"cluster_by={spec['cluster']!r}".replace("'", '"'))
    cfg = ",\n        ".join(config)
    sel = ",\n    ".join(
        f"safe_cast({c['name']} as {CAST[c['bigquery_type']]}) {c['name']}"
        for c in columns(table)
    )
    return (
        "{{\n    config(\n        " + cfg + ",\n    )\n}}\n\n\n"
        f"select\n    {sel}\n"
        f'from {{{{ set_datalake_project("{DS}_staging.{table}") }}}} as t\n'
    )


def q(s: str) -> str:
    """A YAML double-quoted scalar."""
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def schema() -> str:
    """The schema.yml of the dataset."""
    out = ["---", "version: 2", "models:"]
    for table, spec in TABLES.items():
        where = (
            f"\n          config:\n            where: {q(SCOPE)}"
            if spec["scoped"]
            else ""
        )
        out += [
            f"  - name: {DS}__{table}",
            f"    description: {q(spec['description'][0])}",
            "    tests:",
        ]
        out += [
            "      - dbt_utils.unique_combination_of_columns:",
            f"          combination_of_columns: [{', '.join(spec['key'])}]{where}",
            "      - not_null_proportion_multiple_columns:",
            "          at_least: 0.05",
        ]
        if SPARSE.get(table):
            out.append(
                f"          ignore_values: [{', '.join(SPARSE[table])}]"
            )
        if spec["scoped"]:
            out += ["          config:", f"            where: {q(SCOPE)}"]
        if table in DICTIONARY:
            out += [
                "      - custom_dictionary_coverage:",
                f"          dictionary_model: ref('{DS}__dicionario')",
                f"          columns_covered_by_dictionary: [{', '.join(DICTIONARY[table])}]",
            ]
            if spec["scoped"]:
                out += ["          config:", f"            where: {q(SCOPE)}"]
        out.append("    columns:")
        for c in columns(table):
            out += [
                f"      - name: {c['name']}",
                f"        description: {q(c['description_pt'])}",
            ]
            tests = []
            if c["name"] in spec["key"] and table != "work_mesh":
                tests.append(
                    "          - not_null"
                    + (
                        f":\n              config:\n                where: {q(SCOPE)}"
                        if spec["scoped"]
                        else ""
                    )
                )
            rel = RELATIONSHIPS.get(table)
            if rel and rel[0] == c["name"]:
                t = (
                    "          - relationships:\n"
                    f"              to: ref('{DS}__{rel[1]}')\n"
                    f"              field: {rel[2]}"
                )
                if spec["scoped"]:
                    t += f"\n              config:\n                where: {q(SCOPE)}"
                tests.append(t)
            if tests:
                out.append("        tests:")
                out += tests
    return "\n".join(out) + "\n"


def main() -> None:
    """Write every model and the schema.yml."""
    for table in TABLES:
        (MODELS / f"{DS}__{table}.sql").write_text(
            sql(table), encoding="utf-8"
        )
    (MODELS / "schema.yml").write_text(schema(), encoding="utf-8")
    print(f"{len(TABLES)} models written to {MODELS}")


if __name__ == "__main__":
    main()
