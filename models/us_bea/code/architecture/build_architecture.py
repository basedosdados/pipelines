"""Generate the six us_bea architecture CSVs.

Schema columns (data-basis-style order), trilingual descriptions:
  name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
  directory_column, measurement_unit, has_sensitive_data, observations,
  original_name, description_en, description_es

Rule notes applied:
  - value FLOAT64 mixes units per row (current $, chained $, index, percent) ->
    measurement_unit blank + observations note; real unit in unit/metric/unit_mult.
  - codes (table_name, line_code, frequency, table_id, industry) -> STRING,
    covered_by_dictionary=yes (labels live in dicionario).
  - geography FIPS -> STRING, covered_by_dictionary=no, directory_column FK.
  - column descriptions: capitalize first letter, NO trailing period.
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
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


def col(
    name,
    bqtype,
    pt,
    en,
    es,
    *,
    dic="no",
    directory="",
    unit="",
    sensitive="no",
    obs="",
    original="",
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": bqtype,
        "description": pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dic,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": sensitive,
        "observations": obs,
        "original_name": original,
        "description_en": en,
        "description_es": es,
    }


# ---- reusable columns -------------------------------------------------------
YEAR = col(
    "year",
    "INT64",
    "Ano de referência",
    "Reference year",
    "Año de referencia",
    original="TimePeriod",
)
QUARTER = col(
    "quarter",
    "STRING",
    "Trimestre (1 a 4); nulo para frequência não trimestral",
    "Quarter (1 to 4); null for non-quarterly frequency",
    "Trimestre (1 a 4); nulo para frecuencia no trimestral",
    original="TimePeriod",
)
MONTH = col(
    "month",
    "STRING",
    "Mês (01 a 12); nulo para frequência não mensal",
    "Month (01 to 12); null for non-monthly frequency",
    "Mes (01 a 12); nulo para frecuencia no mensual",
    original="TimePeriod",
)
FREQ = col(
    "frequency",
    "STRING",
    "Frequência da série: A (anual), Q (trimestral), M (mensal)",
    "Series frequency: A (annual), Q (quarterly), M (monthly)",
    "Frecuencia de la serie: A (anual), Q (trimestral), M (mensual)",
    dic="yes",
    original="Frequency",
)
VALUE = col(
    "value",
    "FLOAT64",
    "Valor observado, na unidade indicada nas colunas unit/unit_mult",
    "Observed value, in the unit given by the unit/unit_mult columns",
    "Valor observado, en la unidad indicada en las columnas unit/unit_mult",
    obs="Unidade varia por linha (dólares correntes, dólares encadeados, índice, "
    "porcentagem); a unidade real está em unit, metric_name e unit_mult, por isso "
    "measurement_unit fica em branco.",
    original="DataValue",
)
UNIT = col(
    "unit",
    "STRING",
    "Unidade de medida da série (rótulo BEA CL_UNIT)",
    "Unit of measure of the series (BEA CL_UNIT label)",
    "Unidad de medida de la serie (etiqueta BEA CL_UNIT)",
    original="CL_UNIT",
)
UNITM = col(
    "unit_mult",
    "STRING",
    "Multiplicador de base 10 aplicado ao valor (expoente; ex.: 6 = milhões)",
    "Base-10 multiplier applied to the value (exponent; e.g. 6 = millions)",
    "Multiplicador de base 10 aplicado al valor (exponente; p. ej. 6 = millones)",
    original="UNIT_MULT",
)
NOTEREF = col(
    "note_ref",
    "STRING",
    "Referência(s) de nota de rodapé da BEA para a observação",
    "BEA footnote reference(s) for the observation",
    "Referencia(s) de nota al pie de la BEA para la observación",
    original="NoteRef",
)


def geo_fips(desc_pt, desc_en, desc_es):
    return col(
        "geo_fips", "STRING", desc_pt, desc_en, desc_es, original="GeoFips"
    )


GEONAME = col(
    "geo_name",
    "STRING",
    "Nome da área geográfica",
    "Name of the geographic area",
    "Nombre del área geográfica",
    original="GeoName",
)
SERIES = col(
    "series_code",
    "STRING",
    "Código da série BEA da observação",
    "BEA series code of the observation",
    "Código de serie BEA de la observación",
    original="Code",
)
LINEDESC = col(
    "line_description",
    "STRING",
    "Descrição da linha da tabela",
    "Description of the table line",
    "Descripción de la línea de la tabla",
    original="LineDescription",
)


def line_code():
    # table-specific: same code means different things across tables, so not globally
    # dictionary-covered; the per-row line_description carries the label.
    return col(
        "line_code",
        "STRING",
        "Código da linha dentro da tabela BEA",
        "Line code within the BEA table",
        "Código de línea dentro de la tabla BEA",
        dic="no",
        original="LineCode",
    )


def table_name():
    return col(
        "table_name",
        "STRING",
        "Identificador da tabela BEA (ex.: SAGDP2)",
        "BEA table identifier (e.g. SAGDP2)",
        "Identificador de la tabla BEA (p. ej. SAGDP2)",
        dic="yes",
        original="TableName",
    )


# ---- table definitions ------------------------------------------------------
TABLES = {}

TABLES["nipa"] = [
    YEAR,
    QUARTER,
    MONTH,
    FREQ,
    col(
        "table_name",
        "STRING",
        "Identificador da tabela NIPA (ex.: T10105)",
        "NIPA table identifier (e.g. T10105)",
        "Identificador de la tabla NIPA (p. ej. T10105)",
        dic="yes",
        original="TableName",
    ),
    col(
        "line_number",
        "STRING",
        "Número de ordem da linha dentro da tabela",
        "Ordinal line number within the table",
        "Número de orden de la línea dentro de la tabla",
        original="LineNumber",
    ),
    col(
        "series_code",
        "STRING",
        "Código único da série NIPA (ex.: A191RC)",
        "Unique NIPA series code (e.g. A191RC)",
        "Código único de serie NIPA (p. ej. A191RC)",
        original="SeriesCode",
    ),
    LINEDESC,
    col(
        "metric_name",
        "STRING",
        "Tipo de métrica (ex.: dólares correntes, dólares encadeados, índice)",
        "Metric type (e.g. current dollars, chained dollars, index)",
        "Tipo de métrica (p. ej. dólares corrientes, dólares encadenados, índice)",
        original="METRIC_NAME",
    ),
    UNIT,
    UNITM,
    VALUE,
    NOTEREF,
]

TABLES["gdp_by_industry"] = [
    YEAR,
    QUARTER,
    FREQ,
    col(
        "table_id",
        "STRING",
        "Identificador da tabela de PIB por indústria (ex.: 1)",
        "GDP-by-industry table identifier (e.g. 1)",
        "Identificador de la tabla de PIB por industria (p. ej. 1)",
        dic="yes",
        original="TableID",
    ),
    col(
        "table_description",
        "STRING",
        "Descrição da tabela de PIB por indústria",
        "Description of the GDP-by-industry table",
        "Descripción de la tabla de PIB por industria",
        original="TableID",
    ),
    col(
        "industry",
        "STRING",
        "Código de indústria BEA",
        "BEA industry code",
        "Código de industria BEA",
        dic="yes",
        original="Industry",
    ),
    col(
        "industry_description",
        "STRING",
        "Descrição da indústria",
        "Description of the industry",
        "Descripción de la industria",
        original="IndustrYDescription",
    ),
    VALUE,
    NOTEREF,
]

TABLES["regional_state"] = [
    YEAR,
    QUARTER,
    FREQ,
    geo_fips(
        "Código geográfico BEA da entidade estadual (5 dígitos; NN000 para estados, "
        "00000 EUA, 9x000 regiões BEA)",
        "BEA geographic code of the state-level entity (5 digits; NN000 for states, "
        "00000 US, 9x000 BEA regions)",
        "Código geográfico BEA de la entidad estatal (5 dígitos; NN000 para estados, "
        "00000 EE. UU., 9x000 regiones BEA)",
    ),
    col(
        "id_state",
        "STRING",
        "Código FIPS do estado (2 dígitos); nulo para EUA e regiões BEA",
        "State FIPS code (2 digits); null for US and BEA regions",
        "Código FIPS del estado (2 dígitos); nulo para EE. UU. y regiones BEA",
        directory="br_bd_diretorios_us.state:id_state",
        original="GeoFips",
    ),
    GEONAME,
    table_name(),
    line_code(),
    SERIES,
    LINEDESC,
    UNIT,
    UNITM,
    VALUE,
    NOTEREF,
]

TABLES["regional_county"] = [
    YEAR,
    geo_fips(
        "Código FIPS do condado (5 dígitos)",
        "County FIPS code (5 digits)",
        "Código FIPS del condado (5 dígitos)",
    ),
    col(
        "id_county",
        "STRING",
        "Código FIPS do condado (5 dígitos)",
        "County FIPS code (5 digits)",
        "Código FIPS del condado (5 dígitos)",
        directory="br_bd_diretorios_us.county:id_county",
        original="GeoFips",
    ),
    col(
        "id_state",
        "STRING",
        "Código FIPS do estado (2 dígitos)",
        "State FIPS code (2 digits)",
        "Código FIPS del estado (2 dígitos)",
        directory="br_bd_diretorios_us.state:id_state",
        original="GeoFips",
    ),
    GEONAME,
    table_name(),
    line_code(),
    SERIES,
    LINEDESC,
    UNIT,
    UNITM,
    VALUE,
    NOTEREF,
]

TABLES["regional_metro"] = [
    YEAR,
    geo_fips(
        "Código CBSA da área metropolitana (5 dígitos)",
        "CBSA code of the metropolitan area (5 digits)",
        "Código CBSA del área metropolitana (5 dígitos)",
    ),
    col(
        "id_cbsa",
        "STRING",
        "Código CBSA da área estatística baseada em núcleo",
        "CBSA code of the core-based statistical area",
        "Código CBSA del área estadística basada en núcleo",
        directory="br_bd_diretorios_us.cbsa_2023:id_cbsa",
        original="GeoFips",
    ),
    GEONAME,
    table_name(),
    line_code(),
    SERIES,
    LINEDESC,
    UNIT,
    UNITM,
    VALUE,
    NOTEREF,
]

TABLES["dicionario"] = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela à qual a coluna pertence",
        "Name of the table the column belongs to",
        "Nombre de la tabla a la que pertenece la columna",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna codificada",
        "Name of the coded column",
        "Nombre de la columna codificada",
    ),
    col(
        "chave",
        "STRING",
        "Valor codificado (chave)",
        "Coded value (key)",
        "Valor codificado (clave)",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal do mapeamento",
        "Temporal coverage of the mapping",
        "Cobertura temporal del mapeo",
    ),
    col(
        "valor",
        "STRING",
        "Rótulo legível correspondente à chave",
        "Human-readable label corresponding to the key",
        "Etiqueta legible correspondiente a la clave",
    ),
]


def main():
    for tbl, cols in TABLES.items():
        path = os.path.join(HERE, f"{tbl}.csv")
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=HEADER)
            w.writeheader()
            for c in cols:
                w.writerow(c)
        print(f"wrote {tbl}.csv ({len(cols)} cols)")


if __name__ == "__main__":
    main()
