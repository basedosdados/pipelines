"""Generate the dbt models and schema.yml for us_fema_openfema.

Both are derived from the architecture CSVs, so column order, types and
descriptions cannot drift between the catalog and the warehouse. Edit
``build_architecture.py`` (or the glossary) and regenerate — never hand-edit
``../us_fema_openfema__*.sql`` or ``../schema.yml``.

    uv run python build_dbt.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODELS = HERE.parent
sys.path.insert(0, str(HERE))

import tables as spec  # noqa: E402

DATASET = "us_fema_openfema"

# Coverage read from the cleaned output; the partition range must extend past
# the last year present, per the partitioning convention.
PARTITION_START = {
    "disaster_declaration": 1953,
    "public_assistance_project": 1998,
    "nfip_claim": 1978,
    "nfip_policy": 2009,
}
PARTITION_END = 2031

# Columns that are legitimately sparse: the not-null-proportion test would
# otherwise fail on a column FEMA simply stopped populating.
SPARSE_THRESHOLD = 0.05

# `not_null_proportion_multiple_columns` builds a CASE per column and scans the
# whole model — at dbt compile, not only at dbt test. On the two wide NFIP
# tables (85 and 90 columns, 2.7M and 74.3M rows) an unscoped pass is large
# enough to threaten the project-wide daily byte quota on basedosdados-dev, so
# it is scoped to the newest partition. See [[reference_null_proportion_test_cost]].
SCOPED_TESTS = {"nfip_claim", "nfip_policy"}

# County codes that occur in the data but not in the US county directory, which
# is a current-vintage snapshot. Two different things are on this list and both
# are the source's, not ours, so the values are published as FEMA wrote them and
# the referential check is told to skip exactly these:
#
#   1. Geographies that existed when the record was written and no longer do. A
#      1985 declaration for Shannon County SD legitimately says 46113; rewriting
#      it to Oglala Lakota's 46102 would falsify the record.
#   2. Public Assistance rows where FEMA paired a county code with the wrong
#      state — 35101 is labelled "Pueblo County", which is Colorado's 08101, and
#      55161 is labelled "Washtenaw County", which is Michigan's 26161.
#
# The list is explicit rather than a proportion so that a code FEMA has not
# emitted before fails the build instead of being absorbed.
COUNTY_FK_IGNORE = [
    # Alaska census areas retired or re-split since 2008
    "02201", "02232", "02261", "02270", "02280",
    # Connecticut's eight legacy counties, replaced by planning regions in 2022
    "09001", "09003", "09005", "09007", "09009", "09011", "09013", "09015",
    # renamed or dissolved: Shannon SD -> Oglala Lakota, Bedford City VA merged
    "46113", "51515",
    # Public Assistance rows whose county code belongs to a different state
    "32073", "34055", "34085", "35101", "38109", "38141", "46155", "55161",
    # Freely associated states and territories with no county-equivalent in the
    # directory: Micronesia, the Marshall Islands, the Northern Marianas
    "64002", "64005", "64040", "64060",
    "68010", "68030", "68040", "68070", "68080", "68090", "68110", "68120",
    "68140", "68150", "68160", "68170", "68180", "68190", "68300", "68310",
    "68320", "68330", "68340", "68390", "68400", "68410", "68420", "68430",
    "69010",
]


def cast(name: str, bq_type: str) -> str:
    if bq_type == "BOOLEAN":
        return f"safe_cast({name} as boolean) {name}"
    return f"safe_cast({name} as {bq_type.lower()}) {name}"


def architecture(table: str) -> list[dict[str, str]]:
    with (HERE / "architecture" / f"{table}.csv").open() as fh:
        return list(csv.DictReader(fh))


def write_model(table: str) -> None:
    rows = architecture(table)
    partitioned = table in spec.TABLES
    config = [
        f'        alias="{table}",',
        f'        schema="{DATASET}",',
        '        materialized="table",',
    ]
    if partitioned:
        config.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {PARTITION_START[table]}, '
            f'"end": {PARTITION_END}, "interval": 1}},\n'
            "        },"
        )
    casts = ",\n".join(
        "    " + cast(r["name"], r["bigquery_type"]) for r in rows
    )
    body = (
        "{{\n    config(\n"
        + "\n".join(config)
        + "\n    )\n}}\n\n\nselect\n"
        + casts
        + "\nfrom\n"
        + f'    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        + "    as t\n"
    )
    (MODELS / f"{DATASET}__{table}.sql").write_text(body)


def yaml_block(text: str, indent: int) -> str:
    pad = " " * indent
    return "\n".join(pad + line for line in _wrap(text, 78 - indent))


def _wrap(text: str, width: int) -> list[str]:
    words, lines, current = text.split(), [], ""
    for word in words:
        if current and len(current) + 1 + len(word) > width:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    if current:
        lines.append(current)
    return lines


def write_schema() -> None:
    out = ["---", "version: 2", "models:"]
    for table in [*spec.TABLES, "dicionario"]:
        rows = architecture(table)
        cfg = spec.TABLES.get(table)
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >")
        out.append(yaml_block(_description(table, cfg), 6))
        out.append("    tests:")
        if cfg:
            key = cfg["primary_key"]
            combo = ", ".join(key if "year" in key else ["year", *key])
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(f"          combination_of_columns: [{combo}]")
        else:
            out.append("      - dbt_utils.unique_combination_of_columns:")
            out.append(
                "          combination_of_columns: "
                "[id_tabela, nome_coluna, chave]"
            )
        out.append("      - not_null_proportion_multiple_columns:")
        out.append(f"          at_least: {SPARSE_THRESHOLD}")
        if table in SCOPED_TESTS:
            out.append("          config:")
            out.append("            where: __most_recent_year_en__")
        out.append("    columns:")
        for row in rows:
            out.append(f"      - name: {row['name']}")
            out.append("        description: >")
            out.append(yaml_block(row["description"], 12))
            tests = _tests(table, row, cfg)
            if tests:
                out.extend(tests)
    (MODELS / "schema.yml").write_text("\n".join(out) + "\n")


def _description(table: str, cfg: dict | None) -> str:
    if cfg:
        return cfg["description_pt"]
    return (
        "Dicionário de códigos das colunas categóricas do conjunto, com uma "
        "linha por par tabela-coluna-código. Os rótulos estão em inglês, a "
        "língua da fonte."
    )


def _tests(table: str, row: dict, cfg: dict | None) -> list[str]:
    out: list[str] = []
    key = (
        set(cfg["primary_key"])
        if cfg
        else {"id_tabela", "nome_coluna", "chave"}
    )
    not_null = row["name"] in key or (cfg and row["name"] == "year")
    directory = row["directory_column"]
    if not_null:
        out.append("        tests:")
        out.append("          - not_null")
        if directory:
            out.extend(_relationship(directory, row['name']))
    elif directory:
        out.append("        tests:")
        out.extend(_relationship(directory, row['name']))
    return out


def _relationship(directory: str, column: str) -> list[str]:
    dataset, rest = directory.split(".", 1)
    table, field = rest.split(":", 1)
    prefix = "br_bd_" if dataset.startswith("diretorios") else ""
    ref = f"{prefix}{dataset}__{table}"
    # dbt quotes the relation path in parts, so a directory table whose name
    # equals its key column ("...`ano`") turns that trailing part into an
    # implicit BigQuery range variable, and a bare `field: ano` then binds to
    # the whole row STRUCT instead of the column. Qualifying it fixes the bind.
    # See [[reference_dbt_time_directory_relationships_broken]].
    if field == table:
        field = f"{table}.{field}"
    if column != "county_id":
        return [
            "          - relationships:",
            f"              to: ref('{ref}')",
            f"              field: {field}",
        ]
    out = [
        "          - custom_relationships:",
        f"              to: ref('{ref}')",
        f"              field: {field}",
        "              ignore_values:",
    ]
    out += [f"                - '{code}'" for code in COUNTY_FK_IGNORE]
    return out


def main() -> None:
    for table in [*spec.TABLES, "dicionario"]:
        write_model(table)
        print(f"{DATASET}__{table}.sql")
    write_schema()
    print("schema.yml")


if __name__ == "__main__":
    main()
