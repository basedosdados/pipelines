"""Trilingual column descriptions for au_treasury_budget.

The architecture CSVs are the source of truth for name, order, type, unit,
directory link and dictionary flag. They carry one language, so the Portuguese,
English and Spanish descriptions live here, keyed by column name, and are merged
with the architecture into the ``columns_json`` payload ``bulk_upsert_columns``
takes.

Two columns of the same name in different tables mean the same thing throughout,
so descriptions are keyed by name alone. ``year`` is the reference year in
``aggregate`` and ``igr_projection`` but the *publishing* release's year in
``payment_growth``, so that one is keyed by table.

House rule: a column description never ends with a full stop, in any language.
"""

from __future__ import annotations

import csv
import pathlib

CODE = pathlib.Path(__file__).resolve().parent
ARCHITECTURE = CODE / "architecture"

# column name -> (pt, en, es)
DESCRIPTIONS: dict[str, tuple[str, str, str]] = {
    "year": (
        "Primeiro ano-calendário do exercício financeiro australiano a que o valor "
        "se refere, portanto 1970 para 1970-71",
        "First calendar year of the Australian financial year the figure "
        "describes, so 1970 for 1970-71",
        "Primer año calendario del ejercicio financiero australiano al que se "
        "refiere el valor, por lo tanto 1970 para 1970-71",
    ),
    "financial_year": (
        "Exercício financeiro australiano a que o valor se refere, como publicado",
        "Australian financial year the figure describes, as published",
        "Ejercicio financiero australiano al que se refiere el valor, tal como se "
        "publica",
    ),
    "release_id": (
        "Identificador da divulgação fiscal em que o valor foi publicado",
        "Identifier of the fiscal release the figure was published in",
        "Identificador de la publicación fiscal en la que se publicó el valor",
    ),
    "release_label": (
        "Nome da divulgação fiscal como o Tesouro australiano se refere a ela",
        "Name of the fiscal release as the Australian Treasury refers to it",
        "Nombre de la publicación fiscal tal como la denomina el Tesoro australiano",
    ),
    "release_type": (
        "Tipo de divulgação fiscal: orçamento ou resultado orçamentário final",
        "Kind of fiscal release: budget or final budget outcome",
        "Tipo de publicación fiscal: presupuesto o resultado presupuestario final",
    ),
    "release_financial_year": (
        "Exercício financeiro australiano que dá nome à própria divulgação",
        "Australian financial year the release itself is named for",
        "Ejercicio financiero australiano que da nombre a la propia publicación",
    ),
    "sector": (
        "Setor institucional coberto pelo valor",
        "Institutional sector the figure covers",
        "Sector institucional que cubre el valor",
    ),
    "measure": (
        "Agregado fiscal informado pelo valor",
        "Fiscal aggregate the figure reports",
        "Agregado fiscal que informa el valor",
    ),
    "estimate_type": (
        "Se o ano é um resultado realizado ou uma projeção",
        "Whether the year is a realised outcome or a projection",
        "Si el año es un resultado realizado o una proyección",
    ),
    "source_tables": (
        "Tabelas de dados históricos de onde a linha foi construída, separadas por "
        "vírgula",
        "Historical-data tables the row was built from, comma separated",
        "Tablas de datos históricos a partir de las cuales se construyó la fila, "
        "separadas por comas",
    ),
    "value_aud_million": (
        "Valor em milhões de dólares australianos correntes",
        "Value in millions of Australian dollars of the day",
        "Valor en millones de dólares australianos corrientes",
    ),
    "value_percent_gdp": (
        "Valor como porcentagem do produto interno bruto nominal",
        "Value as a percentage of nominal gross domestic product",
        "Valor como porcentaje del producto interno bruto nominal",
    ),
    "value_percent_real_growth": (
        "Crescimento real sobre o ano anterior, em porcentagem",
        "Real growth on the previous year, in per cent",
        "Crecimiento real sobre el año anterior, en porcentaje",
    ),
    "value_aud_per_person_real": (
        "Valor por pessoa em dólares australianos reais do ano-base da própria "
        "divulgação",
        "Value per person in real Australian dollars of the release's own base year",
        "Valor por persona en dólares australianos reales del año base de la propia "
        "publicación",
    ),
    # --- payment_growth ----------------------------------------------------
    "source_release_id": (
        "Identificador da divulgação de cujo gráfico a linha foi lida",
        "Identifier of the release whose chart the row was read from",
        "Identificador de la publicación de cuyo gráfico se leyó la fila",
    ),
    "source_release_label": (
        "Nome da divulgação de cujo gráfico a linha foi lida",
        "Name of the release whose chart the row was read from",
        "Nombre de la publicación de cuyo gráfico se leyó la fila",
    ),
    "series_release_id": (
        "Identificador da divulgação que a série plotada informa, que difere da "
        "divulgação de origem sempre que o gráfico compara duas safras",
        "Identifier of the release the plotted series reports, which differs from "
        "the source release wherever the chart compares two vintages",
        "Identificador de la publicación que informa la serie graficada, que "
        "difiere de la publicación de origen cuando el gráfico compara dos cosechas",
    ),
    "series_release_label": (
        "Nome da divulgação que a série plotada informa",
        "Name of the release the plotted series reports",
        "Nombre de la publicación que informa la serie graficada",
    ),
    "payment_program": (
        "Programa de pagamento a que a taxa de crescimento se refere",
        "Major payment program the growth rate describes",
        "Programa de pago al que se refiere la tasa de crecimiento",
    ),
    "projection_period_start_year": (
        "Primeiro ano-calendário do exercício a partir do qual o crescimento é "
        "medido",
        "First calendar year of the financial year the growth rate is measured from",
        "Primer año calendario del ejercicio a partir del cual se mide el "
        "crecimiento",
    ),
    "projection_period_end_year": (
        "Primeiro ano-calendário do exercício até o qual o crescimento é medido",
        "First calendar year of the financial year the growth rate is measured to",
        "Primer año calendario del ejercicio hasta el cual se mide el crecimiento",
    ),
    "growth_basis": (
        "Se a taxa de crescimento é nominal ou real",
        "Whether the growth rate is nominal or real",
        "Si la tasa de crecimiento es nominal o real",
    ),
    "average_annual_growth_percent": (
        "Crescimento médio anual dos pagamentos do programa no período de projeção, "
        "em porcentagem",
        "Average annual growth in the program's payments over the projection "
        "period, in per cent",
        "Crecimiento promedio anual de los pagos del programa durante el período de "
        "proyección, en porcentaje",
    ),
    # --- igr_projection ----------------------------------------------------
    "igr_edition": (
        "Edição do Relatório Intergeracional de onde vem a projeção",
        "Edition of the Intergenerational Report the projection comes from",
        "Edición del Informe Intergeneracional del que proviene la proyección",
    ),
    "scenario": (
        "Cenário de projeção: a linha de base ou uma das seis variantes de "
        "sensibilidade",
        "Projection scenario: the baseline, or one of six sensitivity variants",
        "Escenario de proyección: la línea base o una de las seis variantes de "
        "sensibilidad",
    ),
    "measure_category": (
        "Agrupamento amplo da medida: demográfica, econômica, fiscal ou pagamentos",
        "Broad grouping of the measure: demographic, economic, fiscal or payments",
        "Agrupación amplia de la medida: demográfica, económica, fiscal o pagos",
    ),
    "source_table": (
        "Tabela do apêndice de onde a linha foi construída, separada por vírgula",
        "Appendix table the row was built from, comma separated",
        "Tabla del apéndice a partir de la cual se construyó la fila, separada por "
        "comas",
    ),
    "value_percent": (
        "Valor em porcentagem, para taxas de crescimento e de participação",
        "Value as a percentage, for growth rates and participation rates",
        "Valor en porcentaje, para tasas de crecimiento y de participación",
    ),
    "value_persons_million": (
        "Valor em milhões de pessoas",
        "Value in millions of people",
        "Valor en millones de personas",
    ),
    "value_years": (
        "Valor em anos, para expectativa de vida ao nascer",
        "Value in years, for life expectancy at birth",
        "Valor en años, para esperanza de vida al nacer",
    ),
    "value_births_per_woman": (
        "Taxa de fecundidade total, em nascimentos por mulher",
        "Total fertility rate, in births per woman",
        "Tasa de fecundidad total, en nacimientos por mujer",
    ),
    # --- dicionario --------------------------------------------------------
    "id_tabela": (
        "Slug da tabela de au_treasury_budget que a entrada descreve",
        "Slug of the au_treasury_budget table the entry describes",
        "Slug de la tabla de au_treasury_budget que describe la entrada",
    ),
    "nome_coluna": (
        "Nome da coluna que a entrada descreve",
        "Name of the column the entry describes",
        "Nombre de la columna que describe la entrada",
    ),
    "chave": (
        "Valor codificado exatamente como armazenado nos dados",
        "Coded value exactly as stored in the data",
        "Valor codificado exactamente como está almacenado en los datos",
    ),
    "cobertura_temporal": (
        "Cobertura temporal da entrada",
        "Temporal coverage of the entry",
        "Cobertura temporal de la entrada",
    ),
    "valor": (
        "Significado do valor codificado",
        "Meaning of the coded value",
        "Significado del valor codificado",
    ),
}

#: Columns whose meaning depends on the table they sit in.
PER_TABLE_DESCRIPTIONS: dict[tuple[str, str], tuple[str, str, str]] = {
    ("payment_growth", "year"): (
        "Primeiro ano-calendário do exercício financeiro que dá nome à divulgação "
        "que publica a linha",
        "First calendar year of the Australian financial year the publishing "
        "release is named for",
        "Primer año calendario del ejercicio financiero que da nombre a la "
        "publicación que publica la fila",
    ),
    ("igr_projection", "year"): (
        "Primeiro ano-calendário do exercício financeiro australiano projetado",
        "First calendar year of the Australian financial year projected",
        "Primer año calendario del ejercicio financiero australiano proyectado",
    ),
    ("igr_projection", "financial_year"): (
        "Exercício financeiro australiano projetado, como publicado",
        "Australian financial year projected, as published",
        "Ejercicio financiero australiano proyectado, tal como se publica",
    ),
}

TABLES = ("aggregate", "payment_growth", "igr_projection", "dicionario")


def read_architecture(table: str) -> list[dict]:
    with (ARCHITECTURE / f"{table}.csv").open() as handle:
        return list(csv.DictReader(handle))


def columns_json(table: str) -> list[dict]:
    """The payload ``bulk_upsert_columns`` takes, for one table."""
    payload = []
    for order, column in enumerate(read_architecture(table)):
        name = column["name"]
        described = PER_TABLE_DESCRIPTIONS.get(
            (table, name)
        ) or DESCRIPTIONS.get(name)
        if described is None:
            raise KeyError(
                f"{table}.{name} has no description. Add it to columns.DESCRIPTIONS "
                "-- a column published without one is a column nobody can use."
            )
        pt, en, es = described
        for language, text in (("pt", pt), ("en", en), ("es", es)):
            if text.endswith("."):
                raise ValueError(
                    f"{table}.{name} [{language}]: column descriptions must not end "
                    "with a full stop"
                )
        entry = {
            "name": name,
            "bigquery_type": column["bigquery_type"],
            "description": pt,
            "description_pt": pt,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": column["covered_by_dictionary"] == "yes",
            "order": order,
        }
        if column["measurement_unit"]:
            entry["measurement_unit"] = column["measurement_unit"]
        if column["directory_column"]:
            entry["directory_column"] = column["directory_column"]
        if column["observations"]:
            entry["observations"] = column["observations"]
        payload.append(entry)
    return payload
