"""Generate the dbt models and schema.yml from the architecture CSVs.

Column order and types come from the architecture, which is the source of truth,
so the models cannot drift from it. Everything in staging is STRING; each model
safe_casts to the architecture's declared type.
"""

from __future__ import annotations

import csv
import pathlib

CODE = pathlib.Path(__file__).resolve().parent
ARCH = CODE / "architecture"
MODELS = CODE.parent

DATASET = "au_abs_migration"

# Partition range per table: first published year to the last plus five.
PARTITIONS = {
    "overseas_country_of_birth_australia": (2004, 2029),
    "overseas_country_of_birth_state": (2004, 2029),
    "overseas_age_sex_australia": (2004, 2029),
    "overseas_age_sex_state": (2004, 2029),
    "overseas_age_sex_australia_calendar_year": (2004, 2029),
    "overseas_age_sex_state_calendar_year": (2004, 2029),
    "overseas_visa_australia": (2004, 2029),
    "overseas_visa_state": (2004, 2029),
    "overseas_visa_quarter_australia": (2006, 2030),
    "overseas_visa_quarter_state": (2006, 2030),
    "interstate_age_sex_australia": (1996, 2029),
    "interstate_age_sex_state": (1996, 2029),
    "interstate_age_sex_australia_calendar_year": (1997, 2030),
    "interstate_age_sex_state_calendar_year": (1997, 2030),
}

PRIMARY_KEYS = {
    "overseas_country_of_birth_australia": ["year", "country_of_birth_id"],
    "overseas_country_of_birth_state": [
        "year",
        "state_id",
        "country_of_birth_id",
    ],
    "overseas_age_sex_australia": ["year", "age_group", "sex"],
    "overseas_age_sex_state": ["year", "state_id", "age_group", "sex"],
    "overseas_age_sex_australia_calendar_year": ["year", "age_group", "sex"],
    "overseas_age_sex_state_calendar_year": [
        "year",
        "state_id",
        "age_group",
        "sex",
    ],
    "overseas_visa_australia": ["year", "visa_group_id"],
    "overseas_visa_state": ["year", "state_id", "visa_group_id"],
    "overseas_visa_quarter_australia": ["year", "quarter", "visa_group_id"],
    "overseas_visa_quarter_state": [
        "year",
        "quarter",
        "state_id",
        "visa_group_id",
    ],
    "interstate_age_sex_australia": ["year", "age_group", "sex"],
    "interstate_age_sex_state": ["year", "state_id", "age_group", "sex"],
    "interstate_age_sex_australia_calendar_year": ["year", "age_group", "sex"],
    "interstate_age_sex_state_calendar_year": [
        "year",
        "state_id",
        "age_group",
        "sex",
    ],
    "dicionario": ["id_tabela", "nome_coluna", "chave"],
}

NOT_NULL = {
    "year",
    "quarter",
    "state_id",
    "country_of_birth_id",
    "age_group",
    "sex",
    "visa_group_id",
    "id_tabela",
    "nome_coluna",
    "chave",
}

# cobertura_temporal is empty throughout: every dictionary entry holds for the
# whole coverage of its table.
IGNORE_VALUES = {"dicionario": ["cobertura_temporal"]}

RELATIONSHIPS = {
    "year": ("br_bd_diretorios_data_tempo__ano", "ano.ano"),
    "state_id": ("br_bd_diretorios_au__state", "id_state"),
    "country_iso3_code": ("br_bd_diretorios_mundo__pais", "sigla_iso3"),
}

ROUNDING = (
    "Os valores são arredondados pelo ABS para a dezena mais próxima, de modo que "
    "os componentes nem sempre somam os totais."
)

DESCRIPTIONS = {
    "overseas_country_of_birth_australia": (
        "Migração internacional da Austrália por país de nascimento, uma linha por "
        "exercício fiscal e país, com chegadas, partidas e migração líquida do país "
        "como um todo. " + ROUNDING
    ),
    "overseas_country_of_birth_state": (
        "Migração internacional por país de nascimento e estado ou território de "
        "residência, uma linha por exercício fiscal, estado e país. "
        + ROUNDING
    ),
    "overseas_age_sex_australia": (
        "Migração internacional da Austrália por grupo etário e sexo, uma linha por "
        "exercício fiscal, grupo etário e sexo. " + ROUNDING
    ),
    "overseas_age_sex_state": (
        "Migração internacional por grupo etário, sexo e estado ou território de "
        "residência, uma linha por exercício fiscal, estado, grupo etário e sexo. "
        + ROUNDING
    ),
    "overseas_age_sex_australia_calendar_year": (
        "Migração internacional da Austrália por grupo etário e sexo, em anos civis. "
        "Mesma estatística da tabela por exercício fiscal, recortada por outro "
        "calendário. " + ROUNDING
    ),
    "overseas_age_sex_state_calendar_year": (
        "Migração internacional por grupo etário, sexo e estado ou território de "
        "residência, em anos civis. " + ROUNDING
    ),
    "overseas_visa_australia": (
        "Chegadas e partidas de migrantes internacionais da Austrália por grupo de "
        "visto ou de cidadania, uma linha por exercício fiscal e grupo de visto. "
        "Contabiliza migrações segundo o visto detido no momento do deslocamento, e "
        "não vistos concedidos. " + ROUNDING
    ),
    "overseas_visa_state": (
        "Chegadas e partidas de migrantes internacionais por grupo de visto ou de "
        "cidadania e estado ou território de residência, uma linha por exercício "
        "fiscal, estado e grupo de visto. " + ROUNDING
    ),
    "overseas_visa_quarter_australia": (
        "Chegadas e partidas de migrantes internacionais da Austrália por grupo de "
        "visto ou de cidadania, em trimestres civis. O ABS revisa esta série a cada "
        "divulgação trimestral da população, de modo que o ano preliminar pode "
        "divergir das tabelas anuais, que refletem a divulgação anual. O código de "
        "visto 03 (outros vistos) existe apenas nesta série; as planilhas anuais o "
        "incluem apenas no total. " + ROUNDING
    ),
    "overseas_visa_quarter_state": (
        "Chegadas e partidas de migrantes internacionais por grupo de visto ou de "
        "cidadania e estado ou território de residência, em trimestres civis. O código "
        "de visto 03 (outros vistos) existe apenas nesta série; as planilhas anuais o "
        "incluem apenas no total. " + ROUNDING
    ),
    "interstate_age_sex_australia": (
        "Migração interestadual na Austrália por grupo etário e sexo, uma linha por "
        "exercício fiscal, grupo etário e sexo. Os totais nacionais correspondem à "
        "soma dos deslocamentos entre estados e territórios. " + ROUNDING
    ),
    "interstate_age_sex_state": (
        "Migração interestadual por grupo etário, sexo e estado ou território, uma "
        "linha por exercício fiscal, estado, grupo etário e sexo. " + ROUNDING
    ),
    "interstate_age_sex_australia_calendar_year": (
        "Migração interestadual na Austrália por grupo etário e sexo, em anos civis. "
        + ROUNDING
    ),
    "interstate_age_sex_state_calendar_year": (
        "Migração interestadual por grupo etário, sexo e estado ou território, em anos "
        "civis. Inclui os Territórios Externos (código 9), ausentes da série por "
        "exercício fiscal. " + ROUNDING
    ),
    "dicionario": (
        "Dicionário dos valores codificados usados nas tabelas de au_abs_migration: "
        "país de nascimento (SACC), grupo etário, sexo e grupo de visto."
    ),
}


def read_architecture(table: str) -> list[dict]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sql_for(table: str, columns: list[dict]) -> str:
    config = [
        f'        schema="{DATASET}"',
        f'        alias="{table}"',
        '        materialized="table"',
    ]
    if table in PARTITIONS:
        start, end = PARTITIONS[table]
        config.append(
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        }"
        )
    selects = ",\n".join(
        f"    safe_cast({column['name']} as {column['bigquery_type'].lower()}) {column['name']}"
        for column in columns
    )
    return (
        "{{\n    config(\n" + ",\n".join(config) + ",\n    )\n}}\n\n\n"
        f"select\n{selects}\n"
        f'from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t\n'
    )


def wrap(text: str, indent: str, width: int = 84) -> list[str]:
    words, lines, current = text.split(), [], indent
    for word in words:
        if len(current) + len(word) + 1 > width and current.strip():
            lines.append(current.rstrip())
            current = indent + word
        else:
            current = f"{current} {word}" if current.strip() else indent + word
    if current.strip():
        lines.append(current.rstrip())
    return lines


def schema_yaml() -> str:
    out = ["---", "version: 2", "models:"]
    for table, keys in PRIMARY_KEYS.items():
        columns = read_architecture(table)
        out.append(f"  - name: {DATASET}__{table}")
        out.append("    description: >-")
        out += wrap(DESCRIPTIONS[table], "      ")
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append("          combination_of_columns:")
        out += [f"            - {key}" for key in keys]
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        if table in IGNORE_VALUES:
            out.append("          ignore_values:")
            out += [f"            - {name}" for name in IGNORE_VALUES[table]]
        out.append("    columns:")
        for column in columns:
            name = column["name"]
            out.append(f"      - name: {name}")
            out.append("        description: >-")
            out += wrap(column["description"], "          ")
            tests: list[str] = []
            if name in NOT_NULL:
                tests.append("          - not_null")
            if name in RELATIONSHIPS:
                model, field = RELATIONSHIPS[name]
                tests += [
                    "          - relationships:",
                    f"              to: ref('{model}')",
                    f"              field: {field}",
                ]
            if tests:
                out.append("        tests:")
                out += tests
    return "\n".join(out) + "\n"


def main() -> None:
    MODELS.mkdir(parents=True, exist_ok=True)
    for table in PRIMARY_KEYS:
        columns = read_architecture(table)
        path = MODELS / f"{DATASET}__{table}.sql"
        path.write_text(sql_for(table, columns))
        print(f"{path.name}: {len(columns)} columns")
    (MODELS / "schema.yml").write_text(schema_yaml())
    print("schema.yml written")


if __name__ == "__main__":
    main()
