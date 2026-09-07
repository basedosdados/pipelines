"""Generate the dbt models and schema.yml for us_fbi_cde from the specification.

Writing these by hand across twelve tables and 174 columns invites the column
order in the SQL to drift from the architecture. Generating both from
``spec.py`` makes that impossible.

Two test-cost decisions are baked in here:

* ``not_null_proportion_multiple_columns`` builds a ``SUM(CASE WHEN … IS NULL)``
  for every column and scans the whole table, so on the incident-level tables it
  is scoped to the most recent year. Unscoped it would read hundreds of
  gigabytes per run against a project-wide daily quota.
* the uniqueness test is scoped the same way for the same reason.

The ``year`` relationship test uses ``field: ano.ano``. dbt quotes the
destination path in parts, so the trailing ``ano`` in
``br_bd_diretorios_data_tempo.ano`` becomes a BigQuery range variable and an
unqualified ``ano`` binds to the whole row struct instead of the column.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fbi_cde.spec import TABLES

DATASET = "us_fbi_cde"
MODELS = Path(__file__).resolve().parents[1]

# Tables large enough that an unscoped full-table test is not worth its bytes.
# Their tests are scoped with __most_recent_year_en__, the English-partition
# variant: the plain __most_recent_year__ filters on `ano`, which these tables
# do not have, so every scoped test would error rather than run.
LARGE = {
    "incident",
    "offense",
    "offender",
    "victim",
    "victim_offense",
    "victim_offender_relationship",
    "arrestee",
    "property",
    "ucr_summary",
}

CLUSTER = {
    "incident": ["state_abbr", "ori"],
    "offense": ["state_abbr", "offense_code"],
    "offender": ["state_abbr"],
    "victim": ["state_abbr"],
    "victim_offense": ["state_abbr"],
    "victim_offender_relationship": ["state_abbr"],
    "arrestee": ["state_abbr", "offense_code"],
    "property": ["state_abbr"],
    "agency": ["state_abbr", "ori"],
    "ucr_summary": ["state_abbr", "ori", "offense_code"],
    "hate_crime": ["state_abbr", "ori"],
}

TABLE_DESCRIPTIONS = {
    "agency": (
        "Uma linha por agência policial e ano, de 1960 a 2025, reunindo os "
        "quadros de pessoal do Law Enforcement Employees, a população coberta e "
        "os campos de cobertura de que se precisa para ponderar as demais "
        "tabelas: quantos meses a agência reportou ao sistema resumido, quantos "
        "reportou ao NIBRS e qual agência a cobre quando ela não reporta "
        "diretamente."
    ),
    "incident": (
        "Uma linha por incidente registrado no NIBRS, de 1991 a 2025, com a "
        "data, a hora, a agência e o esclarecimento por meio excepcional. A "
        "cobertura do NIBRS é parcial e cresce ao longo do período, de três "
        "estados em 1991 para todos em 2020."
    ),
    "offense": (
        "Uma linha por ofensa dentro de um incidente do NIBRS. Um incidente "
        "admite até dez ofensas e a regra da hierarquia do sistema resumido não "
        "se aplica, de modo que todas são contadas. Traz o local, a arma "
        "principal e a motivação por preconceito principal."
    ),
    "offender": (
        "Uma linha por agressor identificado em um incidente do NIBRS, com "
        "idade, sexo, raça e etnia quando informados. Agressores desconhecidos "
        "aparecem com número de ordem 0 e atributos nulos."
    ),
    "victim": (
        "Uma linha por vítima de um incidente do NIBRS. Nem toda vítima é uma "
        "pessoa: empresas, instituições financeiras, governos e a sociedade "
        "também são registrados como vítimas."
    ),
    "victim_offense": (
        "Liga cada vítima às ofensas específicas que sofreu dentro do "
        "incidente. É necessária para contar vítimas por tipo de ofensa: "
        "atribuir todas as ofensas de um incidente a todas as suas vítimas "
        "superestima as contagens."
    ),
    "victim_offender_relationship": (
        "Liga cada vítima aos agressores do incidente e registra a relação "
        "entre eles. O preenchimento é obrigatório apenas quando o incidente "
        "inclui um crime contra a pessoa ou um roubo."
    ),
    "arrestee": (
        "Uma linha por pessoa presa, reunindo as prisões do Grupo A, ligadas a "
        "um incidente, e as do Grupo B, que chegam sem vínculo com incidente ou "
        "agência nos arquivos publicados."
    ),
    "property": (
        "Uma linha por descrição de bem envolvido em um incidente do NIBRS, com "
        "o tipo de perda, o valor em dólares correntes e, nas ofensas de "
        "drogas, o tipo e a quantidade da substância apreendida."
    ),
    "hate_crime": (
        "Uma linha por incidente de crime de ódio registrado pelo programa UCR "
        "entre 1991 e 2025, com a motivação por preconceito, as ofensas, o local "
        "e as contagens de vítimas e agressores. Cobre também os anos "
        "anteriores ao NIBRS."
    ),
    "ucr_summary": (
        "Contagens mensais do formulário resumido Return A por agência e item de "
        "ofensa, de 1985 a 2025: ofensas efetivas, infundadas, esclarecidas e "
        "esclarecidas apenas com menores de 18 anos. É a série longa que "
        "atravessa a transição para o NIBRS, já que o formulário continua a ser "
        "coletado das agências que não migraram."
    ),
    "dicionario": (
        "Dicionário de códigos das colunas categóricas de todas as tabelas do "
        "conjunto, com o rótulo em inglês, a língua da fonte."
    ),
}

SQL_TEMPLATE = """{{{{
    config(
        alias="{table}",
        schema="{dataset}",
        materialized="table",{partition}{cluster}
    )
}}}}


select
{columns}
from {{{{ set_datalake_project("{dataset}_staging.{table}") }}}} as t
"""


def render_sql(table, spec):
    partition = ""
    cluster = ""
    if "year" in spec["partitions"]:
        partition = (
            "\n        partition_by={{\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            '            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        }},"
        ).format(start=spec["first_year"], end=spec["last_year"] + 5)
    if table in CLUSTER:
        names = ", ".join(f'"{c}"' for c in CLUSTER[table])
        cluster = f"\n        cluster_by=[{names}],"
    lines = []
    for column in spec["columns"]:
        name = column["name"]
        kind = column["bigquery_type"].lower()
        lines.append(f"    safe_cast({name} as {kind}) {name},")
    lines[-1] = lines[-1].rstrip(",")
    return SQL_TEMPLATE.format(
        table=table,
        dataset=DATASET,
        partition=partition,
        cluster=cluster,
        columns="\n".join(lines),
    )


def yaml_block(text, indent):
    """Render a description as a folded block scalar at the given indent."""
    pad = " " * indent
    words = text.split()
    lines, current = [], ""
    for word in words:
        if len(current) + len(word) + 1 > 72:
            lines.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    if current:
        lines.append(current)
    body = "\n".join(f"{pad}  {line}" for line in lines)
    return f"{pad}>\n{body}"


def render_schema():
    out = ["---", "version: 2", "models:"]
    for table, spec in TABLES.items():
        dictionary_columns = [
            c["name"]
            for c in spec["columns"]
            if c["covered_by_dictionary"] == "yes"
        ]
        scoped = table in LARGE
        out.append(f"  - name: {DATASET}__{table}")
        out.append(
            "    description: "
            + yaml_block(TABLE_DESCRIPTIONS[table], 4).lstrip()
        )
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(
            "          combination_of_columns: ["
            + ", ".join(spec["unique_key"])
            + "]"
        )
        if scoped:
            out.append("          config:")
            out.append("            where: __most_recent_year_en__")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        sparse = SPARSE_COLUMNS.get(table)
        if sparse:
            out.append("          ignore_values:")
            for name in sparse:
                out.append(f"            - {name}")
        if scoped:
            out.append("          config:")
            out.append("            where: __most_recent_year_en__")
        if dictionary_columns:
            out.append("      - custom_dictionary_coverage:")
            out.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
            out.append(
                "          columns_covered_by_dictionary: ["
                + ", ".join(dictionary_columns)
                + "]"
            )
        out.append("    columns:")
        for column in spec["columns"]:
            name = column["name"]
            out.append(f"      - name: {name}")
            out.append(
                "        description: "
                + yaml_block(column["description_pt"], 8).lstrip()
            )
            tests = []
            if name in spec["partitions"] or name in spec["unique_key"]:
                tests.append("          - not_null")
            if name == "year" and table != "dicionario":
                tests.append("          - relationships:")
                tests.append(
                    "              to: ref('br_bd_diretorios_data_tempo__ano')"
                )
                # dbt quotes the path in parts, so a bare "ano" binds to the
                # range variable rather than the column.
                tests.append("              field: ano.ano")
            if name == "state_id":
                tests.append("          - relationships:")
                tests.append(
                    "              to: ref('br_bd_diretorios_us__state')"
                )
                tests.append("              field: id_state")
            if name == "county_id":
                tests.append("          - relationships:")
                tests.append(
                    "              to: ref('br_bd_diretorios_us__county')"
                )
                tests.append("              field: id_county")
            if tests:
                out.append("        tests:")
                out.extend(tests)
        out.append("")
    return "\n".join(out) + "\n"


# Columns that are legitimately mostly or entirely null, so the null-proportion
# test must not fail on them.
SPARSE_COLUMNS = {
    "agency": [
        "county_id",
        "county_name",
        "agency_unit",
        "covered_by_ori",
        "nibrs_start_date",
        "nibrs_months_reported",
        "nibrs_participated",
        "legacy_ori",
        "core_city_flag",
        "employee_per_1000_inhabitants",
        "male_officer_count",
        "male_civilian_count",
        "female_officer_count",
        "female_civilian_count",
        "officer_count",
        "civilian_count",
        "employee_count",
        "population_group_code",
        "population_group_description",
        "division_name",
        "region_name",
        "agency_type",
        "summary_months_reported",
        "officer_killed_felonious_count",
        "officer_killed_accidental_count",
        "officer_assaulted_count",
    ],
    "incident": [
        "cleared_except_date",
        "incident_hour",
        "submission_date",
        "cargo_theft_flag",
        "incident_status",
        "ori",
    ],
    "offense": [
        "premises_entered_count",
        "method_entry_code",
        "weapon_code",
        "weapon_count",
        "bias_motivation_code",
        "bias_motivation_count",
    ],
    "offender": ["age", "age_range_low", "age_range_high", "ethnicity_code"],
    "victim": [
        "age",
        "age_range_low",
        "age_range_high",
        "ethnicity_code",
        "assignment_type_code",
        "activity_type_code",
        "injury_code",
        "injury_count",
        "resident_status_code",
    ],
    "arrestee": [
        "age",
        "age_range_low",
        "age_range_high",
        "ethnicity_code",
        "incident_id",
        "ori",
        "under_18_disposition_code",
        "weapon_code",
        "multiple_arrestee_indicator",
        "resident_status_code",
        "arrestee_sequence_number",
    ],
    "property": [
        "date_recovered",
        "stolen_count",
        "recovered_count",
        "property_description_code",
        "property_value",
        "suspected_drug_code",
        "drug_quantity",
        "drug_measure_code",
    ],
    "hate_crime": [
        "agency_unit",
        "adult_victim_count",
        "juvenile_victim_count",
        "adult_offender_count",
        "juvenile_offender_count",
        "offender_ethnicity",
        "population_group_code",
    ],
    "ucr_summary": [
        "unfounded_count",
        "juvenile_cleared_count",
        "ori",
        "legacy_ori",
        "state_abbr",
    ],
    "victim_offender_relationship": ["relationship_code"],
}


def main():
    for table, spec in TABLES.items():
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(render_sql(table, spec))
        print(f"wrote {path.name}")
    schema = MODELS / "schema.yml"
    schema.write_text(render_schema())
    print(f"wrote {schema.name}")


if __name__ == "__main__":
    main()
