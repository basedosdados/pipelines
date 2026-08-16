"""Generate the six us_bea architecture CSVs.

Schema columns (data-basis-style order), trilingual descriptions:
  name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
  directory_column, measurement_unit, has_sensitive_data, observations,
  original_name, description_en, description_es

Rule notes applied:
  - Column DESCRIPTIONS are bare (no embedded values, ranges, or examples); those
    move to `observations`. Descriptions carry no trailing period.
  - `year`, `quarter`, `month` are INT64 quantities and carry a measurement_unit.
  - `value` FLOAT64 mixes units per row (current $, chained $, index, percent) ->
    measurement_unit blank + observations note; real unit in unit/unit_mult.
  - codes (table_id, industry, frequency) -> STRING, covered_by_dictionary=yes
    (labels live in dicionario). line_code is table-specific -> dictionary=no.
  - geography FIPS -> STRING, covered_by_dictionary=no, directory_column FK.

NOTE on names/types vs. the raw staging: clean.py writes raw staging where the
BEA table id column is `table_name` and quarter/month are STRING. The dbt models
alias `table_name`->`table_id` and cast quarter/month to INT64, so the PUBLISHED
tables (which this architecture describes) match these definitions.
"""
import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
HEADER = ["name","bigquery_type","description","temporal_coverage",
          "covered_by_dictionary","directory_column","measurement_unit",
          "has_sensitive_data","observations","original_name",
          "description_en","description_es"]

def col(name, bqtype, pt, en, es, *, dic="no", directory="", unit="",
        sensitive="no", obs="", original="", coverage=""):
    return {"name":name,"bigquery_type":bqtype,"description":pt,
            "temporal_coverage":coverage,"covered_by_dictionary":dic,
            "directory_column":directory,"measurement_unit":unit,
            "has_sensitive_data":sensitive,"observations":obs,
            "original_name":original,"description_en":en,"description_es":es}

# ---- reusable columns -------------------------------------------------------
YEAR   = col("year","INT64","Ano de referência","Reference year","Año de referencia",
             unit="year", original="TimePeriod")
QUARTER= col("quarter","INT64","Trimestre","Quarter","Trimestre", unit="quarter",
             obs="Valores de 1 a 4; nulo para frequência não trimestral.",
             original="TimePeriod")
MONTH  = col("month","INT64","Mês","Month","Mes", unit="month",
             obs="Valores de 1 a 12; nulo para frequência não mensal.",
             original="TimePeriod")
FREQ   = col("frequency","STRING","Frequência da série","Series frequency",
             "Frecuencia de la serie", dic="yes",
             obs="A (anual/annual), Q (trimestral/quarterly), M (mensal/monthly).",
             original="Frequency")
VALUE  = col("value","FLOAT64","Valor observado","Observed value","Valor observado",
             obs="Na unidade indicada em unit e unit_mult; a unidade varia por linha "
                 "(dólares correntes, dólares encadeados, índice, porcentagem), por isso "
                 "measurement_unit fica em branco.", original="DataValue")
UNIT   = col("unit","STRING","Unidade de medida da série","Unit of measure of the series",
             "Unidad de medida de la serie", obs="Rótulo BEA CL_UNIT.", original="CL_UNIT")
UNITM  = col("unit_mult","STRING","Multiplicador de base 10 aplicado ao valor",
             "Base-10 multiplier applied to the value",
             "Multiplicador de base 10 aplicado al valor",
             obs="Expoente; por exemplo, 6 = milhões, 9 = bilhões.", original="UNIT_MULT")
NOTEREF= col("note_ref","STRING","Referência de nota de rodapé da BEA para a observação",
             "BEA footnote reference for the observation",
             "Referencia de nota al pie de la BEA para la observación", original="NoteRef")
GEONAME= col("geo_name","STRING","Nome da área geográfica",
             "Name of the geographic area","Nombre del área geográfica", original="GeoName")
SERIES = col("series_code","STRING","Código da série BEA da observação",
             "BEA series code of the observation","Código de serie BEA de la observación",
             original="Code")
LINEDESC=col("line_description","STRING","Descrição da linha da tabela",
             "Description of the table line","Descripción de la línea de la tabla",
             original="LineDescription")

def line_code():
    # table-specific: same code means different things across tables, so not globally
    # dictionary-covered; the per-row line_description carries the label.
    return col("line_code","STRING","Código da linha dentro da tabela BEA",
               "Line code within the BEA table","Código de línea dentro de la tabla BEA",
               dic="no", original="LineCode")

def table_id(example):
    return col("table_id","STRING","Identificador da tabela BEA",
               "BEA table identifier","Identificador de la tabla BEA",
               dic="yes", obs=f"Por exemplo, {example}.", original="TableName")

def geo_fips(desc_pt, desc_en, desc_es, obs):
    return col("geo_fips","STRING",desc_pt,desc_en,desc_es, obs=obs, original="GeoFips")

# ---- table definitions ------------------------------------------------------
TABLES = {}

TABLES["nipa"] = [
    YEAR, QUARTER, MONTH, FREQ,
    table_id("T10105"),
    col("line_number","STRING","Número de ordem da linha dentro da tabela",
        "Ordinal line number within the table","Número de orden de la línea dentro de la tabla",
        original="LineNumber"),
    col("series_code","STRING","Código único da série NIPA",
        "Unique NIPA series code","Código único de serie NIPA",
        obs="Por exemplo, A191RC.", original="SeriesCode"),
    LINEDESC,
    col("metric_name","STRING","Tipo de métrica","Metric type","Tipo de métrica",
        obs="Por exemplo, dólares correntes, dólares encadeados, índice.",
        original="METRIC_NAME"),
    UNIT, UNITM, VALUE, NOTEREF,
]

TABLES["gdp_by_industry"] = [
    YEAR, QUARTER, FREQ,
    table_id("1"),
    col("table_description","STRING","Descrição da tabela de PIB por indústria",
        "Description of the GDP-by-industry table","Descripción de la tabla de PIB por industria",
        original="TableID"),
    col("industry","STRING","Código de indústria BEA","BEA industry code","Código de industria BEA",
        dic="yes", original="Industry"),
    col("industry_description","STRING","Descrição da indústria ou do componente",
        "Description of the industry or component","Descripción de la industria o del componente",
        obs="Nas tabelas de composição (por exemplo, TableID 6, 7, 25, 26) identifica o "
            "componente do agregado, não a indústria.", original="IndustrYDescription"),
    VALUE, NOTEREF,
]

TABLES["regional_state"] = [
    YEAR, QUARTER, FREQ,
    geo_fips("Código geográfico BEA da entidade estadual",
             "BEA geographic code of the state-level entity",
             "Código geográfico BEA de la entidad estatal",
             "Cinco dígitos; NN000 para estados, 00000 para os EUA, 9x000 para regiões BEA."),
    col("id_state","STRING","Código FIPS do estado","State FIPS code","Código FIPS del estado",
        directory="br_bd_diretorios_us.state:id_state",
        obs="Dois dígitos; nulo para os EUA e as regiões BEA.", original="GeoFips"),
    GEONAME, table_id("SAGDP2"), line_code(), SERIES, LINEDESC, UNIT, UNITM, VALUE, NOTEREF,
]

TABLES["regional_county"] = [
    YEAR,
    geo_fips("Código FIPS do condado","County FIPS code","Código FIPS del condado",
             "Cinco dígitos."),
    col("id_county","STRING","Código FIPS do condado","County FIPS code","Código FIPS del condado",
        directory="br_bd_diretorios_us.county:id_county", obs="Cinco dígitos.", original="GeoFips"),
    col("id_state","STRING","Código FIPS do estado","State FIPS code","Código FIPS del estado",
        directory="br_bd_diretorios_us.state:id_state", obs="Dois dígitos.", original="GeoFips"),
    GEONAME, table_id("CAGDP2"), line_code(), SERIES, LINEDESC, UNIT, UNITM, VALUE, NOTEREF,
]

TABLES["regional_metro"] = [
    YEAR,
    geo_fips("Código CBSA da área metropolitana","CBSA code of the metropolitan area",
             "Código CBSA del área metropolitana", "Cinco dígitos."),
    col("id_cbsa","STRING","Código CBSA da área estatística baseada em núcleo",
        "CBSA code of the core-based statistical area","Código CBSA del área estadística basada en núcleo",
        directory="br_bd_diretorios_us.cbsa_2023:id_cbsa", obs="Cinco dígitos.", original="GeoFips"),
    GEONAME, table_id("MARPP"), line_code(), SERIES, LINEDESC, UNIT, UNITM, VALUE, NOTEREF,
]

TABLES["dicionario"] = [
    col("id_tabela","STRING","Nome da tabela à qual a coluna pertence",
        "Name of the table the column belongs to","Nombre de la tabla a la que pertenece la columna"),
    col("nome_coluna","STRING","Nome da coluna codificada",
        "Name of the coded column","Nombre de la columna codificada"),
    col("chave","STRING","Valor codificado","Coded value","Valor codificado"),
    col("cobertura_temporal","STRING","Cobertura temporal do mapeamento",
        "Temporal coverage of the mapping","Cobertura temporal del mapeo"),
    col("valor","STRING","Rótulo legível correspondente à chave",
        "Human-readable label corresponding to the key","Etiqueta legible correspondiente a la clave"),
]

def main():
    for tbl, cols in TABLES.items():
        path = os.path.join(HERE, f"{tbl}.csv")
        with open(path,"w",newline="") as f:
            w=csv.DictWriter(f, fieldnames=HEADER)
            w.writeheader()
            for c in cols:
                w.writerow(c)
        print(f"wrote {tbl}.csv ({len(cols)} cols)")

if __name__=="__main__":
    main()
