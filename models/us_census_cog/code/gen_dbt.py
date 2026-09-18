"""Write the us_census_cog dbt models and schema.yml from the architecture.

Column names, order and types come from the architecture CSVs, so a change
there propagates to the models rather than being restated by hand.

    python gen_dbt.py
"""

from pathlib import Path

import yaml
from common import DATASET_ID

from pipelines.datasets.us_census_cog.utils import load_cols

MODELS = Path(__file__).resolve().parents[1]

# Partition range per table: the first year published, and an end far enough
# ahead that a new survey year needs no schema change.
PARTITIONS = {
    "government_unit": (1997, 2035),
    "employment": (1992, 2035),
    "employment_unit": (1992, 2035),
    "finance": (1967, 2035),
    "finance_unit": (1967, 2035),
}
DICTIONARY_COLUMNS = {
    "government_unit": [
        "government_type",
        "unit_category",
        "function_code",
        "school_level_code",
        "is_active",
    ],
    "employment": [
        "government_type",
        "function_code",
        "full_time_employees_flag",
        "full_time_payroll_flag",
        "part_time_employees_flag",
        "part_time_payroll_flag",
    ],
    "employment_unit": [
        "government_type",
        "census_region_code",
        "school_level_code",
        "worksheet_code",
    ],
    "finance": ["item_code", "data_flag"],
    # data_flag is deliberately absent: in the historical archive the field
    # concatenates single-letter flags, so it carries 31 distinct values such as
    # "ED", "GK" and "KLP" that are combinations rather than codes.
    "finance_unit": [
        "government_type",
        "census_region_code",
        "school_level_code",
        "special_district_function_code",
        "is_imputed_record",
    ],
}
DIRECTORY_TESTS = {
    "state_id": ("br_bd_diretorios_us__state", "id_state"),
    "county_id": ("br_bd_diretorios_us__county", "id_county"),
    "place_id": ("br_bd_diretorios_us__place", "id_place"),
}
# Share of rows whose geographic identifier is absent from the Data Basis US
# directories, measured by validate.py on the cleaned output and set with
# headroom. These are not defects: county and place codes are retired and
# reassigned over a 58-year span, and the directories carry the current
# vintage. Connecticut replaced its counties with planning regions in 2022 and
# several Alaska boroughs have been created or renamed.
#
#   government_unit.county_id  3,040 of 644,259  0.0047
#   government_unit.place_id     439 of 136,322  0.0032
#   employment_unit.county_id  6,813 of 895,701  0.0076
#   employment_unit.state_id       6 of 897,290  0.0000
#   finance_unit.county_id     1,074 of 210,328  0.0051
#   finance_unit.place_id        109 of  36,994  0.0029
DIRECTORY_TOLERANCE: dict[tuple[str, str], float] = {
    ("government_unit", "county_id"): 0.01,
    ("government_unit", "place_id"): 0.01,
    ("employment_unit", "county_id"): 0.02,
    ("employment_unit", "state_id"): 0.001,
    ("finance_unit", "county_id"): 0.02,
    ("finance_unit", "place_id"): 0.01,
}
# Columns legitimately sparse enough to fail, or to come within reach of, the 5%
# non-null floor. Measured by validate.py rather than guessed; the share of
# non-null rows is noted beside each one. Columns above roughly 10% are left in
# the test, because a new survey year moves them by well under a point.
SPARSE_COLUMNS: dict[str, list[str]] = {
    # parent_government_id 0.0090, address_line_2 0.0911
    "government_unit": ["parent_government_id", "address_line_2"],
    # government_id 0.0142, data_flag 0.0421
    "finance": ["government_id", "data_flag"],
    # government_id 0.0465, school_enrollment 0.0424, data_flag 0.0407,
    # special_district_function_code 0.0303, place_id 0.0193,
    # county_subdivision_id 0.0112
    "finance_unit": [
        "government_id",
        "school_enrollment",
        "data_flag",
        "special_district_function_code",
        "place_id",
        "county_subdivision_id",
    ],
}
# The 1992 employment unit directory lists 14 governments twice, which carries
# into the data file as 28 duplicated rows. Measured: 14 of 897,572 unit rows
# and 28 of 5,643,839 data rows, all in 1992, all exact-name repeats.
DUPLICATE_TOLERANCE = {"employment": 0.0001, "employment_unit": 0.0001}
# Uniqueness is tested per identifier era: no single identifier is populated in
# every year, so each test is scoped to the rows that carry its key.
UNIQUE_KEYS = {
    "government_unit": [
        (["year", "government_id"], "government_id is not null"),
        (["year", "government_id_govs"], "government_id_govs is not null"),
    ],
    "employment": [
        (
            ["year", "government_id_govs", "function_code"],
            "government_id_govs is not null",
        ),
    ],
    "employment_unit": [
        (["year", "government_id_govs"], "government_id_govs is not null"),
    ],
    "finance": [
        (
            ["year", "government_id_govs", "item_code"],
            "government_id_govs is not null",
        ),
        (["year", "government_id", "item_code"], "government_id is not null"),
    ],
    "finance_unit": [
        (["year", "government_id_govs"], "government_id_govs is not null"),
        (["year", "government_id"], "government_id is not null"),
    ],
}

DESCRIPTIONS = {
    "government_unit": (
        "Um registro por unidade de governo estadual ou local listada no "
        "levantamento de unidades de governo (Government Units Survey) do "
        "Census Bureau, com nome, tipo, endereço, população ou matrícula e "
        "localização. Cobre os anos de levantamento de 1997 a 2025. Os anos de "
        "2002 e 2007 são publicados em um formato por tipo de governo "
        "incompatível com os demais e ficaram de fora. A coluna unit_category "
        "separa os governos independentes dos sistemas escolares e "
        "previdenciários dependentes, que a fonte lista ao lado deles mas não "
        "conta como governos."
    ),
    "employment": (
        "Um registro por unidade de governo e categoria funcional no "
        "levantamento anual de emprego e folha de pagamento do setor público "
        "(Annual Survey of Public Employment & Payroll), de 1992 a 2024. "
        "Emprego e folha referem-se ao mês de março. Os anos de 1992, 1997, "
        "2002, 2007, 2012, 2017 e 2022 são censos e cobrem todas as unidades; "
        "os demais são amostras de cerca de onze mil unidades, cuja "
        "probabilidade de seleção está em employment_unit. Não há levantamento "
        "em 1996. Horas em tempo parcial e emprego equivalente a tempo integral "
        "deixaram de ser publicados em 2019."
    ),
    "employment_unit": (
        "Um registro por unidade de governo pesquisada em cada ano do "
        "levantamento anual de emprego e folha de pagamento do setor público, "
        "de 1992 a 2024, com nome, localização, nível de ensino e "
        "probabilidade de seleção na amostra. Complementa a tabela employment, "
        "que traz os valores por categoria funcional."
    ),
    "finance": (
        "Um registro por unidade de governo e item financeiro nas finanças de "
        "governos estaduais e locais, dos exercícios fiscais de 1967 e 1970 a "
        "2018, cobrindo receita, despesa, dívida e ativos. Os exercícios até "
        "2012 vêm do arquivo histórico, publicado com uma linha por governo e "
        "529 colunas, aqui transposto para uma linha por item; os exercícios de "
        "2013 em diante já são publicados nessa forma. Nem todo item_code é um "
        "item coletado: os códigos de três posições são coletados e os demais "
        "são agregados calculados pela fonte, de modo que somar todos os "
        "registros de um governo duplica valores. Exceção: não há dados para "
        "1968 e 1969, e a fonte não publica microdados por unidade a partir de "
        "2019."
    ),
    "finance_unit": (
        "Um registro por unidade de governo em cada exercício fiscal das "
        "finanças de governos estaduais e locais, de 1967 e 1970 a 2018, com "
        "nome, localização, população, fim do exercício e peso amostral. "
        "Complementa a tabela finance, que traz os valores por item financeiro."
    ),
    "dicionario": (
        "Correspondência entre os valores codificados das colunas categóricas "
        "do conjunto e sua descrição, incluindo tipo de governo, categoria "
        "funcional do emprego, item financeiro, marcadores de qualidade, nível "
        "de ensino e região censitária."
    ),
}


def model_sql(table: str) -> str:
    """Render one dbt model."""
    columns = load_cols(table)
    casts = ",\n".join(
        f"    safe_cast({c.name} as {c.bigquery_type.lower()}) {c.name}"
        for c in columns
    )
    if table == "dicionario":
        config = (
            f'    config(\n        schema="{DATASET_ID}",\n'
            f'        alias="{table}",\n        materialized="table",\n    )'
        )
    else:
        start, end = PARTITIONS[table]
        config = (
            f'    config(\n        schema="{DATASET_ID}",\n'
            f'        alias="{table}",\n        materialized="table",\n'
            '        partition_by={\n            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": {start}, "end": {end}, "interval": 1}},\n'
            "        },\n    )"
        )
    return (
        "{{\n"
        f"{config}\n"
        "}}\n\n\n"
        "select\n"
        f"{casts}\n"
        "from\n"
        '    {{ set_datalake_project("'
        f'{DATASET_ID}_staging.{table}") }}}}\n'
        "    as t\n"
    )


def model_yaml(table: str) -> dict:
    """Render one schema.yml model entry."""
    columns = load_cols(table)
    tests: list = []
    for key, where in UNIQUE_KEYS.get(table, []):
        tolerance = DUPLICATE_TOLERANCE.get(table)
        if tolerance:
            tests.append(
                {
                    "custom_unique_combinations_of_columns": {
                        "combination_of_columns": key,
                        "proportion_allowed_failures": tolerance,
                        "config": {"where": where},
                    }
                }
            )
        else:
            tests.append(
                {
                    "dbt_utils.unique_combination_of_columns": {
                        "combination_of_columns": key,
                        "config": {"where": where},
                    }
                }
            )
    proportion: dict = {"at_least": 0.05}
    if SPARSE_COLUMNS.get(table):
        proportion["ignore_values"] = SPARSE_COLUMNS[table]
    if table != "dicionario":
        tests.append({"not_null_proportion_multiple_columns": proportion})
    if DICTIONARY_COLUMNS.get(table):
        tests.append(
            {
                "custom_dictionary_coverage": {
                    "dictionary_model": f"ref('{DATASET_ID}__dicionario')",
                    "columns_covered_by_dictionary": DICTIONARY_COLUMNS[table],
                }
            }
        )

    column_entries = []
    for column in columns:
        entry: dict = {
            "name": column.name,
            "description": column.description,
        }
        column_tests: list = []
        if column.name == "year" and table != "dicionario":
            column_tests.append("not_null")
            column_tests.append(
                {
                    "relationships": {
                        "to": "ref('br_bd_diretorios_data_tempo__ano')",
                        "field": "ano.ano",
                    }
                }
            )
        if column.name in DIRECTORY_TESTS:
            model, field = DIRECTORY_TESTS[column.name]
            tolerance = DIRECTORY_TOLERANCE.get((table, column.name), 0.0)
            # custom_relationships adds its own "is not null" guard only when
            # ignore_values is set, so an unguarded test counts every null row
            # as a missing key: place_id is null on 79% of government_unit and
            # the test failed at a measured miss rate of 0.3%. Scope it to the
            # rows that carry a value instead.
            column_tests.append(
                {
                    "custom_relationships": {
                        "to": f"ref('{model}')",
                        "field": field,
                        "ignore_values": [],
                        "proportion_allowed_failures": tolerance,
                        "config": {"where": f"{column.name} is not null"},
                    }
                }
            )
        if column_tests:
            entry["tests"] = column_tests
        column_entries.append(entry)

    return {
        "name": f"{DATASET_ID}__{table}",
        "description": DESCRIPTIONS[table],
        "tests": tests,
        "columns": column_entries,
    }


class _Dumper(yaml.SafeDumper):
    """Emit long descriptions as folded scalars without a trailing newline."""


def _str_presenter(dumper: yaml.Dumper, data: str) -> yaml.nodes.ScalarNode:
    """Represent a long single-line string as a folded scalar.

    Args:
        dumper: The active YAML dumper.
        data: The string being written.

    Returns:
        The scalar node, folded when the string is long enough to wrap.
    """
    if len(data) > 70 and "\n" not in data:
        return dumper.represent_scalar(
            "tag:yaml.org,2002:str", data, style=">"
        )
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_Dumper.add_representer(str, _str_presenter)


def main() -> None:
    """Write every model file and the schema.yml."""
    for table in [*PARTITIONS, "dicionario"]:
        path = MODELS / f"{DATASET_ID}__{table}.sql"
        path.write_text(model_sql(table))
        print(f"{path.name}")

    schema = {
        "version": 2,
        "models": [model_yaml(t) for t in [*PARTITIONS, "dicionario"]],
    }
    text = yaml.dump(
        schema, Dumper=_Dumper, sort_keys=False, allow_unicode=True, width=78
    )
    # A folded scalar written by PyYAML keeps a trailing newline, which reaches
    # BigQuery as a description that no longer matches the backend's. Strip the
    # block chomping indicator in, rather than post-processing every value.
    text = text.replace(": >\n", ": >-\n")
    (MODELS / "schema.yml").write_text("---\n" + text)
    print("schema.yml")


if __name__ == "__main__":
    main()
