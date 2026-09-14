"""Emit the architecture CSVs for us_usda_nass (schema source of truth).

The two source programs (SURVEY, CENSUS) are split by geography grain into one
table each, so every table carries a single grain and only its relevant columns
(no geography_level, no null geo columns):

    survey_national, survey_state, survey_agricultural_district, survey_county
    census_of_agriculture_national, census_of_agriculture_state, census_of_agriculture_county
    dicionario  (shared value-suppression code map)

Census has no agricultural-district grain in QuickStats, so it has no such table.

Column metadata is defined once in COLUMN_DEFS and each table selects its subset,
so descriptions stay consistent across grains. See ``SEED_EXCLUSIONS.md`` for the
seed filter and dropped raw columns.

Run: ``uv run python models/us_usda_nass/code/build_architecture.py``
"""

import csv
from pathlib import Path

ARCH_DIR = Path(__file__).resolve().parent / "architecture"

HEADER = [
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

# name -> (type, pt, en, es, covered_by_dict, measurement_unit, observations, original_name)
COLUMN_DEFS = {
    "year": (
        "INT64",
        "Ano de referência da observação",
        "Reference year of the observation",
        "Año de referencia de la observación",
        "no",
        "year",
        "",
        "YEAR",
    ),
    "state_fips": (
        "STRING",
        "Código FIPS de dois dígitos do estado",
        "Two-digit state FIPS code",
        "Código FIPS de dos dígitos del estado",
        "no",
        "",
        "",
        "STATE_FIPS_CODE",
    ),
    "state_abbreviation": (
        "STRING",
        "Sigla de duas letras do estado",
        "Two-letter state abbreviation",
        "Abreviatura de dos letras del estado",
        "no",
        "",
        "",
        "STATE_ALPHA",
    ),
    "state_name": (
        "STRING",
        "Nome do estado",
        "State name",
        "Nombre del estado",
        "no",
        "",
        "",
        "STATE_NAME",
    ),
    "agricultural_district_code": (
        "STRING",
        "Código do distrito estatístico agrícola dentro do estado",
        "Agricultural statistics district code within the state",
        "Código del distrito estadístico agrícola dentro del estado",
        "no",
        "",
        "",
        "ASD_CODE",
    ),
    "agricultural_district_name": (
        "STRING",
        "Nome do distrito estatístico agrícola",
        "Agricultural statistics district name",
        "Nombre del distrito estadístico agrícola",
        "no",
        "",
        "",
        "ASD_DESC",
    ),
    "county_fips": (
        "STRING",
        "Código FIPS de cinco dígitos do condado (estado mais condado)",
        "Five-digit county FIPS code (state plus county)",
        "Código FIPS de cinco dígitos del condado (estado más condado)",
        "no",
        "",
        "Concatenação de state_fips (2 dígitos) e do código de condado (3 dígitos)",
        "STATE_FIPS_CODE+COUNTY_CODE",
    ),
    "county_name": (
        "STRING",
        "Nome do condado",
        "County name",
        "Nombre del condado",
        "no",
        "",
        "",
        "COUNTY_NAME",
    ),
    "sector": (
        "STRING",
        "Setor da commodity no QuickStats (crops, animals & products, economics, demographics, environmental)",
        "QuickStats commodity sector (crops, animals & products, economics, demographics, environmental)",
        "Sector de la mercancía en QuickStats (crops, animals & products, economics, demographics, environmental)",
        "no",
        "",
        "",
        "SECTOR_DESC",
    ),
    "commodity_group": (
        "STRING",
        "Grupo de commodities ao qual a observação pertence",
        "Commodity group the observation belongs to",
        "Grupo de mercancías al que pertenece la observación",
        "no",
        "",
        "",
        "GROUP_DESC",
    ),
    "commodity": (
        "STRING",
        "Commodity agrícola medida",
        "Agricultural commodity measured",
        "Mercancía agrícola medida",
        "no",
        "",
        "",
        "COMMODITY_DESC",
    ),
    "commodity_class": (
        "STRING",
        "Classe ou subtipo da commodity",
        "Class or subtype of the commodity",
        "Clase o subtipo de la mercancía",
        "no",
        "",
        "",
        "CLASS_DESC",
    ),
    "production_practice": (
        "STRING",
        "Prática de produção (ex.: irrigada, orgânica, todas as práticas)",
        "Production practice (e.g. irrigated, organic, all practices)",
        "Práctica de producción (p. ej. irrigada, orgánica, todas las prácticas)",
        "no",
        "",
        "",
        "PRODN_PRACTICE_DESC",
    ),
    "utilization_practice": (
        "STRING",
        "Prática de utilização (ex.: para grão, para silagem, todas as práticas)",
        "Utilization practice (e.g. for grain, for silage, all practices)",
        "Práctica de utilización (p. ej. para grano, para ensilaje, todas las prácticas)",
        "no",
        "",
        "",
        "UTIL_PRACTICE_DESC",
    ),
    "statistic_category": (
        "STRING",
        "Categoria estatística medida (ex.: production, yield, area planted, price received)",
        "Statistical category measured (e.g. production, yield, area planted, price received)",
        "Categoría estadística medida (p. ej. production, yield, area planted, price received)",
        "no",
        "",
        "",
        "STATISTICCAT_DESC",
    ),
    "unit": (
        "STRING",
        "Unidade de medida do valor; varia por linha (ex.: BU, LB, ACRES, HEAD, $)",
        "Unit of measurement of the value; varies by row (e.g. BU, LB, ACRES, HEAD, $)",
        "Unidad de medida del valor; varía por fila (p. ej. BU, LB, ACRES, HEAD, $)",
        "no",
        "",
        "Coluna que carrega a unidade do valor observado; por isso value não tem measurement_unit fixo",
        "UNIT_DESC",
    ),
    "short_description": (
        "STRING",
        "Descrição completa da série (commodity, classe, práticas e estatística concatenadas)",
        "Full series descriptor (commodity, class, practices and statistic concatenated)",
        "Descripción completa de la serie (mercancía, clase, prácticas y estadística concatenadas)",
        "no",
        "",
        "",
        "SHORT_DESC",
    ),
    "domain": (
        "STRING",
        "Domínio de desagregação da observação (TOTAL quando não desagregada)",
        "Breakdown domain of the observation (TOTAL when not broken down)",
        "Dominio de desagregación de la observación (TOTAL cuando no está desagregada)",
        "no",
        "",
        "",
        "DOMAIN_DESC",
    ),
    "domain_category": (
        "STRING",
        "Categoria dentro do domínio de desagregação",
        "Category within the breakdown domain",
        "Categoría dentro del dominio de desagregación",
        "no",
        "",
        "",
        "DOMAINCAT_DESC",
    ),
    "frequency": (
        "STRING",
        "Frequência de referência da observação",
        "Reference frequency of the observation",
        "Frecuencia de referencia de la observación",
        "no",
        "",
        "",
        "FREQ_DESC",
    ),
    "reference_period": (
        "STRING",
        "Período de referência dentro do ano (ex.: YEAR, MARKETING YEAR)",
        "Reference period within the year (e.g. YEAR, MARKETING YEAR)",
        "Período de referencia dentro del año (p. ej. YEAR, MARKETING YEAR)",
        "no",
        "",
        "",
        "REFERENCE_PERIOD_DESC",
    ),
    "value": (
        "FLOAT64",
        "Valor observado; a unidade está na coluna unit e varia por linha",
        "Observed value; the unit is in the unit column and varies by row",
        "Valor observado; la unidad está en la columna unit y varía por fila",
        "no",
        "",
        "measurement_unit fica em branco de propósito: a unidade varia por linha (coluna unit). Valores suprimidos pela fonte ((D), (Z), (X), (S), (H), (L), (NA)) são nulos, com o código em value_suppression_flag",
        "VALUE",
    ),
    "value_suppression_flag": (
        "STRING",
        "Código de supressão quando o valor foi retido pela fonte ((D), (Z) etc.); nulo caso contrário",
        "Suppression code when the value was withheld by the source ((D), (Z) etc.); null otherwise",
        "Código de supresión cuando el valor fue retenido por la fuente ((D), (Z) etc.); nulo en caso contrario",
        "yes",
        "",
        "Coberto pelo dicionario; (D) retido para evitar divulgar operações individuais, (Z) menos da metade da unidade de arredondamento",
        "VALUE",
    ),
    "coefficient_of_variation": (
        "FLOAT64",
        "Coeficiente de variação da estimativa, em porcentagem",
        "Coefficient of variation of the estimate, in percent",
        "Coeficiente de variación de la estimación, en porcentaje",
        "no",
        "percent",
        "",
        "CV_%",
    ),
}

# Columns after the geography block (shared by every fact table).
COMMON = [
    "sector",
    "commodity_group",
    "commodity",
    "commodity_class",
    "production_practice",
    "utilization_practice",
    "statistic_category",
    "unit",
    "short_description",
    "domain",
    "domain_category",
    "frequency",
    "reference_period",
    "value",
    "value_suppression_flag",
    "coefficient_of_variation",
]

# Geography columns per grain (right after year), coarsest hierarchy first.
GEO = {
    "national": [],
    "state": ["state_fips", "state_abbreviation", "state_name"],
    "agricultural_district": [
        "state_fips",
        "state_abbreviation",
        "state_name",
        "agricultural_district_code",
        "agricultural_district_name",
    ],
    "county": [
        "state_fips",
        "state_abbreviation",
        "state_name",
        "agricultural_district_code",
        "agricultural_district_name",
        "county_fips",
        "county_name",
    ],
}

# Published fact tables: (table_slug, grain).
FACT_TABLES = [
    ("survey_national", "national"),
    ("survey_state", "state"),
    ("survey_agricultural_district", "agricultural_district"),
    ("survey_county", "county"),
    ("census_of_agriculture_national", "national"),
    ("census_of_agriculture_state", "state"),
    ("census_of_agriculture_county", "county"),
]


def table_columns(grain: str) -> list[str]:
    return ["year", *GEO[grain], *COMMON]


DICIONARIO_COLUMNS = [
    (
        "id_tabela",
        "STRING",
        "Nome da tabela à qual a coluna pertence",
        "Name of the table the column belongs to",
        "Nombre de la tabla a la que pertenece la columna",
        "no",
        "",
        "",
        "",
    ),
    (
        "nome_coluna",
        "STRING",
        "Nome da coluna codificada",
        "Name of the coded column",
        "Nombre de la columna codificada",
        "no",
        "",
        "",
        "",
    ),
    (
        "chave",
        "STRING",
        "Valor codificado",
        "Coded value",
        "Valor codificado",
        "no",
        "",
        "",
        "",
    ),
    (
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal do mapeamento",
        "Temporal coverage of the mapping",
        "Cobertura temporal del mapeo",
        "no",
        "",
        "",
        "",
    ),
    (
        "valor",
        "STRING",
        "Rótulo legível correspondente à chave",
        "Human-readable label corresponding to the key",
        "Etiqueta legible correspondiente a la clave",
        "no",
        "",
        "",
        "",
    ),
]


def _row_from_def(name: str) -> dict:
    btype, pt, en, es, cbd, unit, obs, orig = COLUMN_DEFS[name]
    return {
        "name": name,
        "bigquery_type": btype,
        "description": pt,
        "temporal_coverage": "",
        "covered_by_dictionary": cbd,
        "directory_column": "",
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": orig,
        "description_en": en,
        "description_es": es,
    }


def _row_from_tuple(col: tuple) -> dict:
    name, btype, pt, en, es, cbd, unit, obs, orig = col
    return {
        "name": name,
        "bigquery_type": btype,
        "description": pt,
        "temporal_coverage": "",
        "covered_by_dictionary": cbd,
        "directory_column": "",
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": orig,
        "description_en": en,
        "description_es": es,
    }


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  wrote {path.name} ({len(rows)} columns)")


def main():
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for slug, grain in FACT_TABLES:
        rows = [_row_from_def(n) for n in table_columns(grain)]
        write_csv(ARCH_DIR / f"{slug}.csv", rows)
    write_csv(
        ARCH_DIR / "dicionario.csv",
        [_row_from_tuple(c) for c in DICIONARIO_COLUMNS],
    )
    print("architecture CSVs written")


if __name__ == "__main__":
    main()
