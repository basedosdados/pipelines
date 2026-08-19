"""Generate the architecture CSVs for au_ato_taxation_statistics.

The architecture table is the source of truth for column names, types,
order and descriptions. Types follow arithmetic meaning: only genuine
quantities (record counts, monetary amounts) are numeric; every code,
label and identifier is STRING.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

from pipelines.datasets.au_ato_taxation_statistics.constants import (  # noqa: E402
    constants,
)

OUT = Path(__file__).resolve().parent / "architecture"
FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
    "description_en",
    "description_es",
]


def col(
    name,
    btype,
    pt,
    en,
    es,
    *,
    dic="no",
    directory="",
    unit="",
    obs="",
    original="",
    coverage="",
):
    """Build one architecture row."""
    return {
        "name": name,
        "bigquery_type": btype,
        "description": pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dic,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": original,
        "description_en": en,
        "description_es": es,
    }


YEAR = col(
    "year",
    "INT64",
    "Ano inicial do exercício fiscal a que se referem os dados; 2023 corresponde ao exercício 2023-24",
    "Start year of the financial year the data refer to; 2023 denotes the 2023-24 year",
    "Año inicial del ejercicio fiscal al que se refieren los datos; 2023 corresponde al ejercicio 2023-24",
    directory="br_bd_diretorios_data_tempo.ano:ano",
    unit="year",
    obs="Partition column. Derived from each source sheet's own title rather than from the release that ships it, because some tables cover a different financial year from their release",
)
STATE = col(
    "state_abbreviation",
    "STRING",
    "Sigla do estado ou território de residência do contribuinte",
    "Abbreviation of the taxpayer's state or territory of residence",
    "Sigla del estado o territorio de residencia del contribuyente",
    dic="yes",
    directory="br_bd_diretorios_au.state:abbreviation",
    obs="Includes the non-geographic values 'Overseas' and 'Unknown', which have no directory match",
    original="State/Territory",
)
SEX = col(
    "sex",
    "STRING",
    "Sexo do contribuinte conforme registrado na declaração",
    "Sex of the taxpayer as recorded in the return",
    "Sexo del contribuyente según consta en la declaración",
    dic="yes",
    original="Sex",
)
TAXSTAT = col(
    "taxable_status",
    "STRING",
    "Situação tributável do contribuinte no exercício",
    "Taxable status of the taxpayer in the year",
    "Situación tributable del contribuyente en el ejercicio",
    dic="yes",
    original="Taxable status",
)
ITEM = col(
    "item",
    "STRING",
    "Item tributário declarado, conforme o rótulo da coluna na tabela de origem",
    "Taxation item reported, as labelled by the column in the source table",
    "Ítem tributario declarado, según la etiqueta de la columna en la tabla de origen",
    obs="The set of items varies across years as the ATO adds or retires labels",
)
COUNT = col(
    "record_count",
    "INT64",
    "Número de registros com valor informado para o item",
    "Number of records reporting a value for the item",
    "Número de registros que declaran un valor para el ítem",
    unit="record",
    original="<item> no.",
)
AMOUNT = col(
    "amount",
    "FLOAT64",
    "Valor monetário agregado do item, em dólares australianos",
    "Aggregate monetary value of the item, in Australian dollars",
    "Valor monetario agregado del ítem, en dólares australianos",
    unit="AUD",
    original="<item> $",
)


def industry(entity_pt, entity_en, entity_es):
    """Broad and fine ANZSIC-based industry columns."""
    return [
        col(
            "broad_industry_id",
            "STRING",
            "Código da divisão industrial ampla (letra ANZSIC)",
            "Broad industry division code (ANZSIC letter)",
            "Código de la división industrial amplia (letra ANZSIC)",
            dic="yes",
            original="Broad industry",
        ),
        col(
            "broad_industry",
            "STRING",
            f"Nome da divisão industrial ampla {entity_pt}",
            f"Name of the broad industry division {entity_en}",
            f"Nombre de la división industrial amplia {entity_es}",
            original="Broad industry",
        ),
        col(
            "fine_industry_id",
            "STRING",
            "Código do grupo industrial detalhado (ANZSIC de três dígitos)",
            "Fine industry group code (three-digit ANZSIC)",
            "Código del grupo industrial detallado (ANZSIC de tres dígitos)",
            dic="yes",
            original="Fine industry",
        ),
        col(
            "fine_industry",
            "STRING",
            "Nome do grupo industrial detalhado",
            "Name of the fine industry group",
            "Nombre del grupo industrial detallado",
            original="Fine industry",
        ),
    ]


TABLES = {
    "individuals_income_state": [
        YEAR,
        STATE,
        SEX,
        TAXSTAT,
        col(
            "taxable_income_range_code",
            "STRING",
            "Código ordenador da faixa de renda tributável",
            "Sort code of the taxable income range",
            "Código ordenador del rango de renta imponible",
            dic="yes",
            original="Taxable income range",
        ),
        col(
            "taxable_income_range",
            "STRING",
            "Faixa de renda tributável do contribuinte",
            "Taxable income range of the taxpayer",
            "Rango de renta imponible del contribuyente",
            original="Taxable income range",
        ),
        col(
            "taxable_income_bracket_code",
            "STRING",
            "Código ordenador da faixa marginal de imposto",
            "Sort code of the marginal tax bracket",
            "Código ordenador del tramo marginal de impuesto",
            dic="yes",
            original="Taxable income range - tax brackets",
        ),
        col(
            "taxable_income_bracket",
            "STRING",
            "Faixa marginal de imposto aplicável ao contribuinte",
            "Marginal tax bracket applicable to the taxpayer",
            "Tramo marginal de impuesto aplicable al contribuyente",
            obs="Thresholds change across years as tax brackets are legislated",
            original="Taxable income range - tax brackets",
        ),
        ITEM,
        COUNT,
        AMOUNT,
    ],
    "individuals_industry": [
        YEAR,
        STATE,
        SEX,
        col(
            "broad_industry_id",
            "STRING",
            "Código da divisão industrial ampla (letra ANZSIC)",
            "Broad industry division code (ANZSIC letter)",
            "Código de la división industrial amplia (letra ANZSIC)",
            dic="yes",
            original="Broad industry",
        ),
        col(
            "broad_industry",
            "STRING",
            "Nome da divisão industrial ampla da atividade principal do contribuinte",
            "Name of the broad industry division of the taxpayer's main activity",
            "Nombre de la división industrial amplia de la actividad principal del contribuyente",
            original="Broad industry",
        ),
        ITEM,
        COUNT,
        AMOUNT,
    ],
    "individuals_postcode": [
        YEAR,
        STATE,
        col(
            "sa4_id",
            "STRING",
            "Código da Área Estatística Nível 4 (SA4) de residência do contribuinte",
            "Code of the Statistical Area Level 4 (SA4) of the taxpayer's residence",
            "Código del Área Estadística Nivel 4 (SA4) de residencia del contribuyente",
            directory="br_bd_diretorios_au.sa4_2021:id_sa4",
            obs=(
                "Not published by the ATO: resolved by matching sa4_name against "
                "the SA4 2021 directory. Null for the nine ATO residual groupings "
                "such as 'NSW other' and 'Overseas', and for years before 2021-22"
            ),
        ),
        col(
            "sa4_name",
            "STRING",
            "Nome da Área Estatística Nível 4 (SA4) de residência do contribuinte",
            "Name of the Statistical Area Level 4 (SA4) of the taxpayer's residence",
            "Nombre del Área Estadística Nivel 4 (SA4) de residencia del contribuyente",
            obs=(
                "Only published from the 2021-22 release; null for earlier "
                "years. Retains the ATO residual groupings ('NSW other', "
                "'Overseas') that have no ABS SA4 code"
            ),
            original="Statistical Area Level 4 (SA4)",
        ),
        col(
            "postcode",
            "STRING",
            "Código postal de residência do contribuinte",
            "Postcode of the taxpayer's residence",
            "Código postal de residencia del contribuyente",
            obs="Includes ATO residual groupings such as 'NSW other', 'Overseas' and 'Unknown', so it is not linked to a postal area directory",
            original="Postcode",
        ),
        TAXSTAT,
        ITEM,
        COUNT,
        AMOUNT,
    ],
    "company_industry": [
        YEAR,
        *industry(
            "da atividade principal da empresa",
            "of the company's main activity",
            "de la actividad principal de la empresa",
        ),
        ITEM,
        COUNT,
        AMOUNT,
    ],
    "gst_industry": [
        YEAR,
        *industry(
            "da atividade principal do contribuinte de GST",
            "of the GST taxpayer's main activity",
            "de la actividad principal del contribuyente de GST",
        ),
        ITEM,
        COUNT,
        AMOUNT,
    ],
    "dicionario": [
        col(
            "id_tabela",
            "STRING",
            "Nome da tabela",
            "Table name",
            "Nombre de la tabla",
        ),
        col(
            "nome_coluna",
            "STRING",
            "Nome da coluna",
            "Column name",
            "Nombre de la columna",
        ),
        col(
            "chave",
            "STRING",
            "Chave (valor armazenado na coluna)",
            "Key (value stored in the column)",
            "Clave (valor almacenado en la columna)",
        ),
        col(
            "cobertura_temporal",
            "STRING",
            "Cobertura temporal da chave",
            "Temporal coverage of the key",
            "Cobertura temporal de la clave",
        ),
        col(
            "valor",
            "STRING",
            "Valor (rótulo legível correspondente à chave)",
            "Value (human-readable label matching the key)",
            "Valor (etiqueta legible correspondiente a la clave)",
        ),
    ],
}


def main() -> None:
    """Write one CSV per table."""
    OUT.mkdir(parents=True, exist_ok=True)
    for table, columns in TABLES.items():
        if table in constants.DIMENSIONS.value:
            dims = list(constants.DIMENSIONS.value[table])
            derived = constants.DERIVED_COLUMNS.value.get(table, {})
            for name, after in derived.items():
                dims.insert(dims.index(after) + 1, name)
            expected = ["year", *dims, *constants.MEASURES.value]
        else:
            expected = [c["name"] for c in columns]
        got = [c["name"] for c in columns]
        if got != expected:
            raise SystemExit(
                f"{table}: architecture order {got} != cleaner order {expected}"
            )
        path = OUT / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(columns)
        print(f"{table:28s} {len(columns):>2} columns -> {path.name}")


if __name__ == "__main__":
    main()
