"""Write the us_census_cog architecture CSVs.

The CSVs under ``architecture/`` are the single source of truth for column
names, order, BigQuery types, directory foreign keys and the raw -> clean name
mapping. Every other artefact -- the cleaning transform, the dbt models, the
``schema.yml`` and the backend column payloads -- is generated from them.

    python gen_architecture.py
"""

import csv
import re
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"
HEADER = [
    "name",
    "bigquery_type",
    "description_pt",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations_pt",
    "observations_en",
    "observations_es",
    "original_name",
]


# Portuguese and Spanish share most of this vocabulary but not its accents, so
# the two are spelled from a common base rather than by one pass over the file.
# Only words that actually differ between the languages appear here.
# Portuguese and Spanish share most of this vocabulary but not its accents, so
# the two are spelled from a common base rather than by one pass over the file.
# Text added to this file is written unaccented and gets its diacritics here, at
# generation time, which is what keeps a later edit from shipping bare words --
# every accent defect found in review so far came from a string added after an
# earlier one-off pass over the source.
SHARED_SPELLING = {
    "codigo": "código",
    "Codigo": "Código",
    "codigos": "códigos",
    "unico": "único",
    "publico": "público",
    "area": "área",
    "areas": "áreas",
    "numero": "número",
    "Numero": "Número",
    "matricula": "matrícula",
    "digitos": "dígitos",
    "dolares": "dólares",
    "estatistico": "estadístico",
}
LANGUAGE_SPELLING = {
    "pt": {
        "referencia": "referência",
        "atribuida": "atribuída",
        "tres": "três",
        "mes": "mês",
        "Populacao": "População",
        "populacao": "população",
        "funcao": "função",
        "funcoes": "funções",
        "correspondencia": "correspondência",
        "Nao": "Não",
        "nao": "não",
        "ate": "até",
        "Ate": "Até",
        "endereco": "endereço",
        "Endereco": "Endereço",
        "diretorio": "diretório",
        "municipio": "município",
        "municipios": "municípios",
        "estatistico": "estatístico",
        "variavel": "variável",
        "eletronico": "eletrônico",
        "sitio": "sítio",
        "pseudocodigo": "pseudocódigo",
        "orgao": "órgão",
        "nivel": "nível",
        "regiao": "região",
        "censitaria": "censitária",
        "censitarios": "censitários",
        "disponivel": "disponível",
        "Disponivel": "Disponível",
        "exercicio": "exercício",
        "propria": "própria",
        "estatisticas": "estatísticas",
        "Descricao": "Descrição",
        "descricao": "descrição",
        "financas": "finanças",
        "divida": "dívida",
        "imputacao": "imputação",
        "somatorio": "somatório",
        "compoem": "compõem",
        "posicoes": "posições",
        "Extensao": "Extensão",
        "extensao": "extensão",
        "formulario": "formulário",
        "selecao": "seleção",
        "publicacao": "publicação",
        "previdenciarios": "previdenciários",
        "previdencia": "previdência",
        "responsavel": "responsável",
        "Designacao": "Designação",
        "politica": "política",
        "marco": "março",
        "estavel": "estável",
        "atribuido": "atribuído",
        "alfabetico": "alfabético",
        "California": "Califórnia",
        "subcodigo": "subcódigo",
        "Composicao": "Composição",
    },
    "es": {
        "Ano": "Año",
        "ano": "año",
        "anos": "años",
        "funcao": "función",
        "funcoes": "funciones",
        "Populacao": "Población",
        "populacao": "población",
        "poblacion": "población",
        "Categoria": "Categoría",
        "categoria": "categoría",
        "publicacion": "publicación",
        "Nomina": "Nómina",
        "nomina": "nómina",
        "region": "región",
        "estadistica": "estadística",
        "estadisticas": "estadísticas",
        "dias": "días",
        "demas": "demás",
        "imputacion": "imputación",
        "extension": "extensión",
        "Extension": "Extensión",
        "subdivision": "subdivisión",
        "geografico": "geográfico",
        "foranea": "foránea",
        "seleccion": "selección",
        "Descripcion": "Descripción",
        "descripcion": "descripción",
        "ningun": "ningún",
        "segun": "según",
        "ensenanza": "enseñanza",
        "aqui": "aquí",
        "Designacion": "Designación",
        "politica": "política",
        "direccion": "dirección",
        "Direccion": "Dirección",
        "linea": "línea",
        "pseudocodigo": "pseudocódigo",
        "alfabetico": "alfabético",
        "subcodigo": "subcódigo",
        "Composicion": "Composición",
        # "esta" is deliberately absent from both maps. It is the demonstrative
        # in "esta unidad" and the verb in "está ubicada", and a blanket
        # substitution turns the first into the second -- which is exactly the
        # defect review found. Write "está" directly where the verb is meant.
    },
}


def spell(text: str, language: str) -> str:
    """Apply one language's spelling to a description written from the base."""
    mapping = {**SHARED_SPELLING, **LANGUAGE_SPELLING[language]}
    for base, accented in mapping.items():
        text = re.sub(
            rf"(?<![A-Za-zÀ-ÿ]){re.escape(base)}(?![A-Za-zÀ-ÿ])",
            accented,
            text,
        )
    return text


DIR_YEAR = "diretorios_data_tempo.ano:ano"
DIR_STATE = "br_bd_diretorios_us.state:id_state"
DIR_COUNTY = "br_bd_diretorios_us.county:id_county"
DIR_PLACE = "br_bd_diretorios_us.place:id_place"


def col(
    name: str,
    typ: str,
    pt: str,
    en: str,
    es: str,
    *,
    coverage: str = "",
    dictionary: str = "no",
    directory: str = "",
    unit: str = "",
    sensitive: str = "no",
    obs: tuple[str, str, str] = ("", "", ""),
    original: str = "",
) -> list[str]:
    """Build one architecture row in the order HEADER declares."""
    return [
        name,
        typ,
        pt,
        en,
        es,
        coverage,
        dictionary,
        directory,
        unit,
        sensitive,
        obs[0],
        obs[1],
        obs[2],
        original,
    ]


# --------------------------------------------------------------------------
# Shared columns
# --------------------------------------------------------------------------

OBS_YEAR = (
    "Coluna de particionamento",
    "Partition column",
    "Columna de particionamiento",
)
OBS_GOV_ID = (
    "Identificador estável de 6 dígitos (CENSUS_ID_PID6) atribuído pelo Census "
    "Bureau. Presente apenas onde a fonte o publica: no diretorio de unidades, "
    "nas finanças de 2013 em diante e no emprego de 2021 em diante. Nao e "
    "derivado por cruzamento em nenhum outro caso",
    "Stable 6-digit identifier (CENSUS_ID_PID6) assigned by the Census Bureau. "
    "Present only where the source publishes it: in the unit directory, in "
    "finance from 2013 on and in employment from 2021 on. It is not "
    "back-derived by crosswalk anywhere else",
    "Identificador estable de 6 dígitos (CENSUS_ID_PID6) asignado por el Census "
    "Bureau. Presente solo donde la fuente lo publica: en el directorio de "
    "unidades, en finanzas desde 2013 y en empleo desde 2021. No se deriva por "
    "cruce en ningún otro caso",
)
OBS_GOVS_ID = (
    "Identificador legado GOVS de 14 caracteres. Até 2012 o arquivo de finanças "
    "publica apenas as nove primeiras posições, completadas aqui com zeros, "
    "como a fonte passa a fazer a partir de 2013. Composição: código GOVS de estado (2), "
    "tipo de governo (1), código GOVS de condado (3), unidade (3), suplemento "
    "(3) e subcódigo (2). O código GOVS de estado é alfabético e difere do FIPS "
    "(Califórnia é 05 em GOVS e 06 em FIPS)",
    "Legacy 14-character GOVS identifier. Through 2012 the finance archive "
    "publishes only the first nine positions, padded with zeros here as the "
    "source itself does from 2013 on. Composition: GOVS state code (2), government type "
    "(1), GOVS county code (3), unit (3), supplement (3) and sub-code (2). The "
    "GOVS state code is alphabetical and differs from FIPS (California is 05 in "
    "GOVS and 06 in FIPS)",
    "Identificador heredado GOVS de 14 caracteres. Hasta 2012 el archivo de "
    "finanzas publica solo las nueve primeras posiciones, completadas aquí con "
    "ceros, como la fuente hace desde 2013. Composición: código GOVS de estado (2), "
    "tipo de gobierno (1), código GOVS de condado (3), unidad (3), suplemento "
    "(3) y subcódigo (2). El código GOVS de estado es alfabético y difiere del "
    "FIPS (California es 05 en GOVS y 06 en FIPS)",
)


def c_year(
    desc_extra: tuple[str, str, str] = ("", "", ""),
    original: str = "",
    coverage: str = "",
) -> list[str]:
    """The partition column, shared by every table but dicionario."""
    return col(
        "year",
        "INT64",
        "Ano de referencia" + desc_extra[0],
        "Reference year" + desc_extra[1],
        "Ano de referencia" + desc_extra[2],
        coverage=coverage,
        directory=DIR_YEAR,
        unit="year",
        obs=OBS_YEAR,
        original=original,
    )


def c_government_id(original: str = "") -> list[str]:
    """The 6-digit Census identifier of the government unit."""
    return col(
        "government_id",
        "STRING",
        "Identificador da unidade de governo no Census Bureau",
        "Census Bureau identifier of the government unit",
        "Identificador de la unidad de gobierno en el Census Bureau",
        obs=OBS_GOV_ID,
        original=original,
    )


def c_government_id_govs(original: str = "") -> list[str]:
    """The legacy GOVS identifier of the government unit."""
    return col(
        "government_id_govs",
        "STRING",
        "Identificador legado da unidade de governo no sistema GOVS",
        "Legacy identifier of the government unit in the GOVS scheme",
        "Identificador heredado de la unidad de gobierno en el esquema GOVS",
        obs=OBS_GOVS_ID,
        original=original,
    )


def c_government_type(original: str = "") -> list[str]:
    """The type of government, from state through independent school district."""
    return col(
        "government_type",
        "STRING",
        "Tipo de unidade de governo",
        "Type of government unit",
        "Tipo de unidad de gobierno",
        dictionary="yes",
        original=original,
    )


def c_state_id(
    original: str = "", obs: tuple[str, str, str] = ("", "", "")
) -> list[str]:
    """The FIPS state code, keyed to the US state directory."""
    return col(
        "state_id",
        "STRING",
        "Código FIPS de duas posições do estado",
        "Two-digit FIPS code of the state",
        "Código FIPS de dos posiciones del estado",
        directory=DIR_STATE,
        obs=obs,
        original=original,
    )


def c_county_id(
    original: str = "", obs: tuple[str, str, str] = ("", "", "")
) -> list[str]:
    """The FIPS county code, keyed to the US county directory."""
    return col(
        "county_id",
        "STRING",
        "Código FIPS de cinco posições do condado, estado seguido de condado",
        "Five-digit FIPS code of the county, state followed by county",
        "Código FIPS de cinco posiciones del condado, estado seguido de condado",
        directory=DIR_COUNTY,
        obs=obs,
        original=original,
    )


def c_county_subdivision_id(
    original: str = "", coverage: str = ""
) -> list[str]:
    """The FIPS county subdivision code, which townships carry instead of a place."""
    return col(
        "county_subdivision_id",
        "STRING",
        "Código FIPS de sete posições da subdivisão de condado, estado seguido "
        "de subdivisão",
        "Seven-digit FIPS county subdivision code, state followed by "
        "subdivision",
        "Codigo FIPS de siete posiciones de la subdivision de condado, estado "
        "seguido de subdivision",
        coverage=coverage,
        obs=(
            "A fonte grava um único campo de código geográfico para municípios "
            "e townships, mas os dois são espaços de código distintos. O "
            "município traz um código de lugar incorporado e o township um "
            "código de subdivisão de condado, separados aqui em duas colunas. "
            "Não há diretório de subdivisões de condado, então esta coluna não "
            "tem chave estrangeira",
            "The source writes one geography code field for both municipalities "
            "and townships, but the two are different code spaces. A "
            "municipality carries an incorporated place code and a township a "
            "county subdivision code, separated here into two columns. There is "
            "no county subdivision directory, so this column carries no foreign "
            "key",
            "La fuente escribe un unico campo de codigo geografico para "
            "municipios y townships, pero son espacios de codigo distintos. El "
            "municipio trae un codigo de lugar incorporado y el township un "
            "codigo de subdivision de condado, separados aqui en dos columnas. "
            "No hay directorio de subdivisiones de condado, por lo que esta "
            "columna no lleva clave foranea",
        ),
        original=original,
    )


# --------------------------------------------------------------------------
# government_unit
# --------------------------------------------------------------------------

GOVERNMENT_UNIT = [
    c_year(
        (
            " do levantamento de unidades de governo",
            " of the government units survey",
            " del censo de unidades de gobierno",
        ),
        coverage="1997(5)2025",
    ),
    c_government_id("CENSUS_ID_PID6"),
    c_government_id_govs("CENSUS_ID_GIDID"),
    c_government_type("UNIT_TYPE"),
    col(
        "unit_category",
        "STRING",
        "Categoria da unidade na publicação, que separa governos independentes "
        "dos sistemas escolares e previdenciários dependentes",
        "Category of the unit in the publication, separating independent "
        "governments from dependent school and pension systems",
        "Categoria de la unidad en la publicacion, que separa gobiernos "
        "independientes de los sistemas escolares y de pensiones dependientes",
        dictionary="yes",
        obs=(
            "Derivada da aba da planilha de origem. Sistemas escolares e "
            "previdenciários dependentes são listados pela fonte mas não contam "
            "como governos no total publicado",
            "Derived from the source worksheet. Dependent school and pension "
            "systems are listed by the source but do not count as governments in "
            "the published total",
            "Derivada de la hoja de origen. Los sistemas escolares y de "
            "pensiones dependientes son listados por la fuente pero no cuentan "
            "como gobiernos en el total publicado",
        ),
    ),
    col(
        "unit_name",
        "STRING",
        "Nome da unidade de governo",
        "Name of the government unit",
        "Nombre de la unidad de gobierno",
        original="UNIT_NAME",
    ),
    col(
        "function_code",
        "STRING",
        "Código da funcao exercida, apenas para distritos especiais",
        "Code of the function performed, special districts only",
        "Código de la funcao ejercida, solo para distritos especiales",
        dictionary="yes",
        original="FUNCTION_NAME",
    ),
    col(
        "function_name",
        "STRING",
        "Nome da funcao exercida, apenas para distritos especiais",
        "Name of the function performed, special districts only",
        "Nombre de la funcao ejercida, solo para distritos especiales",
        original="FUNCTION_NAME",
    ),
    col(
        "school_level_code",
        "STRING",
        "Código do nível de ensino, apenas para distritos escolares",
        "School level code, school districts only",
        "Código del nível de enseñanza, solo para distritos escolares",
        dictionary="yes",
        original="SCHOOL_LEVEL_DESCRIPTION",
    ),
    col(
        "political_description",
        "STRING",
        "Designação política da unidade, como county, city, town ou village",
        "Political designation of the unit, such as county, city, town or village",
        "Designación política de la unidad, como county, city, town o village",
        obs=(
            "Publicado apenas em 1997 e a partir de 2024",
            "Published only in 1997 and from 2024 on",
            "Publicado solo en 1997 y a partir de 2024",
        ),
        original="POLITICAL_CODE_DESCRIPTION",
    ),
    col(
        "officer_title",
        "STRING",
        "Cargo do responsável indicado como contato da unidade",
        "Title of the officer listed as the unit contact",
        "Cargo del responsable indicado como contacto de la unidad",
        original="TITLE",
    ),
    col(
        "address_line_1",
        "STRING",
        "Primeira linha do endereço postal da unidade",
        "First line of the unit postal address",
        "Primera línea de la dirección postal de la unidad",
        original="ADDRESS1",
    ),
    col(
        "address_line_2",
        "STRING",
        "Segunda linha do endereço postal da unidade",
        "Second line of the unit postal address",
        "Segunda línea de la dirección postal de la unidad",
        original="ADDRESS2",
    ),
    col(
        "city",
        "STRING",
        "Cidade do endereço postal da unidade",
        "City of the unit postal address",
        "Ciudad de la dirección postal de la unidad",
        original="CITY",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Sigla de duas letras do estado do endereço postal",
        "Two-letter abbreviation of the state in the postal address",
        "Sigla de dos letras del estado de la dirección postal",
        original="STATE",
    ),
    col(
        "zip_code",
        "STRING",
        "Código postal de cinco dígitos do endereço da unidade",
        "Five-digit ZIP code of the unit address",
        "Código postal de cinco dígitos de la dirección de la unidad",
        original="ZIP",
    ),
    col(
        "zip_code_extension",
        "STRING",
        "Extensão de quatro dígitos do código postal",
        "Four-digit extension of the ZIP code",
        "Extension de cuatro dígitos del código postal",
        original="ZIP4",
    ),
    col(
        "web_address",
        "STRING",
        "Endereco do sítio eletrônico oficial da unidade",
        "Address of the official website of the unit",
        "Dirección del sítio web oficial de la unidad",
        original="WEB_ADDRESS",
    ),
    col(
        "population",
        "INT64",
        "Populacao residente atribuida à unidade",
        "Resident population attributed to the unit",
        "Populacao residente atribuida a la unidad",
        unit="person",
        obs=(
            "Publicada apenas para estados, condados, municipios e townships",
            "Published only for state, county, municipal and township units",
            "Publicada solo para estados, condados, municipios y townships",
        ),
        original="POPULATION",
    ),
    col(
        "population_year",
        "INT64",
        "Ano a que se refere a populacao informada",
        "Year the reported population refers to",
        "Ano al que se refiere la poblacion informada",
        unit="year",
        original="POPULATION_YEAR",
    ),
    col(
        "school_enrollment",
        "INT64",
        "Número de alunos matriculados, apenas para distritos escolares",
        "Number of enrolled students, school districts only",
        "Número de alumnos matriculados, solo para distritos escolares",
        unit="person",
        original="SCHOOL_ENROLLMENT",
    ),
    col(
        "enrollment_year",
        "INT64",
        "Ano a que se refere a matrícula informada",
        "Year the reported enrollment refers to",
        "Ano al que se refiere la matrícula informada",
        unit="year",
        original="ENROLLMENT_YEAR",
    ),
    c_state_id("FIPS_STATE"),
    c_county_id(
        "FIPS_COUNTY",
        obs=(
            "Condado onde a unidade está localizada ou ao qual foi atribuida",
            "County the unit is located in or was assigned to",
            "Condado donde la unidad está ubicada o al que fue asignada",
        ),
    ),
    col(
        "place_id",
        "STRING",
        "Código FIPS de sete posições do lugar, estado seguido de lugar",
        "Seven-digit FIPS place code, state followed by place",
        "Código FIPS de siete posiciones del lugar, estado seguido de lugar",
        directory=DIR_PLACE,
        obs=(
            "Nulo quando a fonte traz o pseudocódigo 99xxx, que designa a área "
            "de um condado e não um lugar incorporado",
            "Null where the source carries the 99xxx pseudo-code, which denotes "
            "a county area rather than an incorporated place",
            "Nulo cuando la fuente trae el pseudocódigo 99xxx, que designa el "
            "área de un condado y no un lugar incorporado",
        ),
        original="FIPS_PLACE",
    ),
    c_county_subdivision_id("FIPS_PLACE"),
    col(
        "county_area_name",
        "STRING",
        "Nome da área de condado atribuida à unidade na fonte",
        "Name of the county area assigned to the unit in the source",
        "Nombre del área de condado asignada a la unidad en la fuente",
        original="COUNTY_AREA_NAME",
    ),
    col(
        "parent_government_id",
        "STRING",
        "Identificador da unidade de governo a que esta unidade é subordinada",
        "Identifier of the government unit this unit is dependent on",
        "Identificador de la unidad de gobierno a la que está subordinada",
        obs=(
            "Preenchido a partir de 2025 para sistemas escolares dependentes e "
            "sistemas de previdência",
            "Populated from 2025 on for dependent school systems and pension "
            "systems",
            "Completado a partir de 2025 para sistemas escolares dependientes y "
            "sistemas de pensiones",
        ),
        original="PARENT_CENSUS_ID_PID6",
    ),
    col(
        "is_active",
        "STRING",
        "Indica se a unidade estava ativa na data do levantamento",
        "Whether the unit was active at the survey date",
        "Indica si la unidad estaba activa en la fecha del censo",
        dictionary="yes",
        original="ACTIVE",
    ),
]

# --------------------------------------------------------------------------
# employment
# --------------------------------------------------------------------------

OBS_MARCH = (
    "Refere-se ao mes de março; a folha de pagamento é o equivalente mensal de "
    "31 dias",
    "Refers to the month of March; payroll is the 31-day monthly equivalent",
    "Se refiere al mes de marzo; la nómina es el equivalente mensual de 31 dias",
)
OBS_FLAG = (
    "Indica se o valor foi reportado pelo governo ou imputado pelo Census "
    "Bureau. Ausente antes de 2007, quando a fonte não publicava marcadores",
    "Whether the value was reported by the government or imputed by the Census "
    "Bureau. Absent before 2007, when the source published no flags",
    "Indica si el valor fue reportado por el gobierno o imputado por el Census "
    "Bureau. Ausente antes de 2007, cuando la fuente no publicaba marcadores",
)


def flag_col(
    name: str, subject_pt: str, subject_en: str, subject_es: str
) -> list[str]:
    """The data quality flag that accompanies one employment measure."""
    return col(
        f"{name}_flag",
        "STRING",
        f"Marcador de qualidade de {subject_pt}",
        f"Data quality flag for {subject_en}",
        f"Marcador de calidad de {subject_es}",
        dictionary="yes",
        obs=OBS_FLAG,
    )


EMPLOYMENT = [
    c_year(
        (
            " do levantamento de emprego publico",
            " of the public employment survey",
            " del censo de empleo publico",
        ),
        coverage="1992(1)2024",
    ),
    c_government_id("New Individual Unit ID"),
    c_government_id_govs("Individual Unit ID"),
    c_state_id(
        "State Code",
        obs=(
            "Convertido do código GOVS alfabético publicado no arquivo para o "
            "código FIPS",
            "Converted from the alphabetical GOVS code published in the file to "
            "the FIPS code",
            "Convertido del código GOVS alfabético publicado en el archivo al "
            "código FIPS",
        ),
    ),
    c_government_type("Unit Type Code"),
    col(
        "function_code",
        "STRING",
        "Código da categoria funcional do emprego",
        "Code of the functional category of employment",
        "Código de la categoria funcional del empleo",
        dictionary="yes",
        original="Item Code",
    ),
    col(
        "full_time_employees",
        "INT64",
        "Número de empregados em tempo integral",
        "Number of full-time employees",
        "Número de empleados a tiempo completo",
        unit="person",
        obs=OBS_MARCH,
        original="Full-Time Employees",
    ),
    flag_col(
        "full_time_employees",
        "empregados em tempo integral",
        "full-time employees",
        "empleados a tiempo completo",
    ),
    col(
        "full_time_payroll",
        "INT64",
        "Folha de pagamento dos empregados em tempo integral",
        "Payroll of full-time employees",
        "Nomina de los empleados a tiempo completo",
        unit="usd",
        obs=OBS_MARCH,
        original="Full-Time Payroll",
    ),
    flag_col(
        "full_time_payroll",
        "folha de pagamento em tempo integral",
        "full-time payroll",
        "nómina a tiempo completo",
    ),
    col(
        "part_time_employees",
        "INT64",
        "Número de empregados em tempo parcial",
        "Number of part-time employees",
        "Número de empleados a tiempo parcial",
        unit="person",
        obs=OBS_MARCH,
        original="Part-Time Employees",
    ),
    flag_col(
        "part_time_employees",
        "empregados em tempo parcial",
        "part-time employees",
        "empleados a tiempo parcial",
    ),
    col(
        "part_time_payroll",
        "INT64",
        "Folha de pagamento dos empregados em tempo parcial",
        "Payroll of part-time employees",
        "Nomina de los empleados a tiempo parcial",
        unit="usd",
        obs=OBS_MARCH,
        original="Part-Time Payroll",
    ),
    flag_col(
        "part_time_payroll",
        "folha de pagamento em tempo parcial",
        "part-time payroll",
        "nómina a tiempo parcial",
    ),
    col(
        "part_time_hours",
        "INT64",
        "Horas trabalhadas pelos empregados em tempo parcial",
        "Hours worked by part-time employees",
        "Horas trabajadas por los empleados a tiempo parcial",
        coverage="1992(1)2018",
        unit="hour",
        obs=(
            "A fonte deixou de publicar esta variável a partir de 2019",
            "The source stopped publishing this variable from 2019 on",
            "La fuente dejó de publicar esta variable a partir de 2019",
        ),
        original="Part-Time Hours",
    ),
    col(
        "full_time_equivalent_employees",
        "INT64",
        "Emprego equivalente a tempo integral",
        "Full-time equivalent employment",
        "Empleo equivalente a tiempo completo",
        coverage="1992(1)2018",
        unit="person",
        obs=(
            "A fonte deixou de publicar esta variável a partir de 2019",
            "The source stopped publishing this variable from 2019 on",
            "La fuente dejó de publicar esta variable a partir de 2019",
        ),
        original="Full-Time Equivalent Employment",
    ),
]

# --------------------------------------------------------------------------
# employment_unit
# --------------------------------------------------------------------------

EMPLOYMENT_UNIT = [
    c_year(
        (
            " do levantamento de emprego publico",
            " of the public employment survey",
            " del censo de empleo publico",
        ),
        coverage="1992(1)2024",
    ),
    c_government_id("New Individual Unit ID"),
    c_government_id_govs("Individual Unit ID"),
    c_government_type("Unit Type Code"),
    col(
        "unit_name",
        "STRING",
        "Nome da unidade de governo",
        "Name of the government unit",
        "Nombre de la unidad de gobierno",
        original="Name of Government",
    ),
    c_state_id("FIPS State"),
    c_county_id("FIPS County"),
    col(
        "county_name",
        "STRING",
        "Nome do condado onde a unidade está localizada ou ao qual foi atribuida",
        "Name of the county the unit is located in or was assigned to",
        "Nombre del condado donde la unidad está ubicada o al que fue asignada",
        original="County Name",
    ),
    col(
        "census_region_code",
        "STRING",
        "Código da região censitária",
        "Census region code",
        "Código de la region censal",
        dictionary="yes",
        original="Census Region Code",
    ),
    col(
        "population_enrollment_function",
        "STRING",
        "Campo multiuso da fonte, que traz populacao, matrícula ou código de "
        "funcao conforme o tipo de governo",
        "Multi-purpose source field carrying population, enrollment or function "
        "code depending on the type of government",
        "Campo multiuso de la fuente, que trae poblacion, matrícula o código de "
        "funcao según el tipo de gobierno",
        obs=(
            "Populacao para estados, condados, municipios e townships; "
            "matrícula para distritos escolares; código de funcao para "
            "distritos especiais. A fonte grava os tres no mesmo campo, por "
            "isso a coluna e texto",
            "Population for state, county, municipal and township units; "
            "enrollment for school districts; function code for special "
            "districts. The source writes all three into the same field, which "
            "is why the column is text",
            "Populacao para estados, condados, municipios y townships; "
            "matrícula para distritos escolares; código de funcao para "
            "distritos especiales. La fuente escribe los tres en el mismo "
            "campo, por eso la columna es texto",
        ),
        original="Population/Enrollment/Function Code",
    ),
    col(
        "population_enrollment_year",
        "INT64",
        "Ano a que se refere a populacao ou matrícula informada",
        "Year the reported population or enrollment refers to",
        "Ano al que se refiere la poblacion o matrícula informada",
        unit="year",
        original="Year of Population/Enrollment",
    ),
    col(
        "school_level_code",
        "STRING",
        "Código do nível de ensino, apenas para distritos escolares",
        "School level code, school districts only",
        "Código del nível de enseñanza, solo para distritos escolares",
        dictionary="yes",
        original="School Level Code",
    ),
    col(
        "selection_probability",
        "FLOAT64",
        "Probabilidade de seleção da unidade na amostra",
        "Probability of selecting the unit into the sample",
        "Probabilidad de selección de la unidad en la muestra",
        obs=(
            "Peso amostral adimensional; vale 1 nos anos censitários, quando "
            "todas as unidades são pesquisadas",
            "Dimensionless sampling weight; it is 1 in census years, when every "
            "unit is surveyed",
            "Peso muestral adimensional; vale 1 en los anos censales, cuando "
            "todas las unidades son encuestadas",
        ),
        original="Probability of Selection",
    ),
    col(
        "worksheet_code",
        "STRING",
        "Código do formulário usado para coletar os dados da unidade",
        "Code of the worksheet used to collect the unit data",
        "Código del formulário usado para recolectar los datos de la unidad",
        dictionary="yes",
        original="Worksheet Code",
    ),
]

# --------------------------------------------------------------------------
# finance
# --------------------------------------------------------------------------

FINANCE = [
    c_year(
        (
            " fiscal das finanças do governo",
            " of the government finances fiscal year",
            " fiscal de las finanzas del gobierno",
        ),
        coverage="1967(1)2018",
    ),
    c_government_id(),
    c_government_id_govs("ID"),
    col(
        "item_code",
        "STRING",
        "Código do item financeiro de receita, despesa, dívida ou ativo",
        "Code of the revenue, expenditure, debt or asset finance item",
        "Código del item financiero de ingreso, gasto, deuda o activo",
        dictionary="yes",
        obs=(
            "Códigos de tres posições são itens coletados; os demais são "
            "agregados calculados pelo Census Bureau, cujo somatório duplica os "
            "itens que os compõem. A tabela dicionario informa o tipo de cada "
            "código",
            "Three-character codes are collected items; the rest are aggregates "
            "computed by the Census Bureau, and summing them double-counts the "
            "items they are built from. The dicionario table records the type of "
            "each code",
            "Los códigos de tres posiciones son items recolectados; los demas "
            "son agregados calculados por el Census Bureau, cuya suma duplica "
            "los items que los componen. La tabla dicionario informa el tipo de "
            "cada código",
        ),
        original="Item code",
    ),
    col(
        "amount",
        "INT64",
        "Valor do item financeiro",
        "Amount of the finance item",
        "Monto del item financiero",
        unit="usd",
        obs=(
            "A fonte publica valores em milhares de dólares; aqui foram "
            "multiplicados por mil para ficar em dólares",
            "The source publishes amounts in thousands of dollars; they are "
            "multiplied by a thousand here so the column is in dollars",
            "La fuente publica valores en miles de dólares; aqui se "
            "multiplicaron por mil para quedar en dólares",
        ),
        original="Amount",
    ),
    col(
        "data_flag",
        "STRING",
        "Marcador de imputação do item",
        "Imputation flag for the item",
        "Marcador de imputacion del item",
        coverage="2013(1)2018",
        dictionary="yes",
        obs=(
            "Publicado por item apenas de 2013 em diante; ate 2012 a fonte "
            "traz um único marcador por unidade, disponível em finance_unit",
            "Published per item only from 2013 on; through 2012 the source "
            "carries a single flag per unit, available in finance_unit",
            "Publicado por item solo desde 2013; hasta 2012 la fuente trae un "
            "único marcador por unidad, disponible en finance_unit",
        ),
        original="Imputation type/item data flag",
    ),
]

# --------------------------------------------------------------------------
# finance_unit
# --------------------------------------------------------------------------

FINANCE_UNIT = [
    c_year(
        (
            " fiscal das finanças do governo",
            " of the government finances fiscal year",
            " fiscal de las finanzas del gobierno",
        ),
        coverage="1967(1)2018",
    ),
    c_government_id(),
    c_government_id_govs("ID"),
    c_government_type("Type Code"),
    col(
        "unit_name",
        "STRING",
        "Nome da unidade de governo",
        "Name of the government unit",
        "Nombre de la unidad de gobierno",
        original="Name",
    ),
    c_state_id("FIPS Code-State"),
    c_county_id(
        "FIPS County",
        obs=(
            "Disponível apenas de 2013 em diante; ate 2012 a fonte identifica o "
            "condado pelo código GOVS, que não e o FIPS",
            "Available only from 2013 on; through 2012 the source identifies the "
            "county by its GOVS code, which is not the FIPS one",
            "Disponible solo desde 2013; hasta 2012 la fuente identifica el "
            "condado por el código GOVS, que no es el FIPS",
        ),
    ),
    col(
        "county_name",
        "STRING",
        "Nome do condado onde a unidade está localizada",
        "Name of the county the unit is located in",
        "Nombre del condado donde la unidad está ubicada",
        original="County name",
    ),
    col(
        "place_id",
        "STRING",
        "Código FIPS de sete posições do lugar, estado seguido de lugar",
        "Seven-digit FIPS place code, state followed by place",
        "Código FIPS de siete posiciones del lugar, estado seguido de lugar",
        coverage="2013(1)2018",
        directory=DIR_PLACE,
        obs=(
            "Nulo quando a fonte traz o pseudocódigo 99xxx, que designa a área "
            "de um condado e não um lugar incorporado",
            "Null where the source carries the 99xxx pseudo-code, which denotes "
            "a county area rather than an incorporated place",
            "Nulo cuando la fuente trae el pseudocódigo 99xxx, que designa el "
            "área de un condado y no un lugar incorporado",
        ),
        original="FIPS place code",
    ),
    c_county_subdivision_id("FIPS place code", coverage="2013(1)2018"),
    col(
        "census_region_code",
        "STRING",
        "Código da região censitária",
        "Census region code",
        "Código de la region censal",
        coverage="1967(1)2012",
        dictionary="yes",
        original="Census Region",
    ),
    col(
        "population",
        "INT64",
        "Populacao residente atribuida à unidade",
        "Resident population attributed to the unit",
        "Populacao residente atribuida a la unidad",
        unit="person",
        original="Population",
    ),
    col(
        "population_year",
        "INT64",
        "Ano a que se refere a populacao informada",
        "Year the reported population refers to",
        "Ano al que se refiere la poblacion informada",
        unit="year",
        original="Population year",
    ),
    col(
        "school_enrollment",
        "INT64",
        "Número de alunos matriculados, apenas para distritos escolares",
        "Number of enrolled students, school districts only",
        "Número de alumnos matriculados, solo para distritos escolares",
        coverage="2013(1)2018",
        unit="person",
        original="Enrollment",
    ),
    col(
        "school_level_code",
        "STRING",
        "Código do nível de ensino, apenas para distritos escolares",
        "School level code, school districts only",
        "Código del nível de enseñanza, solo para distritos escolares",
        dictionary="yes",
        original="School level code",
    ),
    col(
        "special_district_function_code",
        "STRING",
        "Código da funcao exercida, apenas para distritos especiais",
        "Code of the function performed, special districts only",
        "Código de la funcao ejercida, solo para distritos especiales",
        coverage="2013(1)2018",
        dictionary="yes",
        original="Function code for special districts",
    ),
    col(
        "fiscal_year_end",
        "STRING",
        "Fim do exercício fiscal da unidade, no formato MMDD",
        "End of the fiscal year of the unit, formatted MMDD",
        "Fin del ejercicio fiscal de la unidad, en formato MMDD",
        original="Fiscal year ending",
    ),
    col(
        "survey_weight",
        "FLOAT64",
        "Peso estatístico da unidade na amostra",
        "Statistical weight of the unit in the sample",
        "Peso estadístico de la unidad en la muestra",
        coverage="1967(1)2012",
        obs=(
            "Peso adimensional; 10000 indica unidade incluida com certeza e 0 "
            "indica unidade fora da amostra. A própria fonte adverte que o peso "
            "é informativo e não deve ser usado para derivar estatísticas",
            "Dimensionless weight; 10000 marks a unit included with certainty "
            "and 0 a unit outside the sample. The source itself warns the weight "
            "is informational and should not be used to derive statistics",
            "Peso adimensional; 10000 indica unidad incluida con certeza y 0 "
            "unidad fuera de la muestra. La propia fuente advierte que el peso "
            "es informativo y no debe usarse para derivar estadísticas",
        ),
        original="Weight",
    ),
    col(
        "data_flag",
        "STRING",
        "Marcador de qualidade do registro da unidade",
        "Data quality flag for the unit record",
        "Marcador de calidad del registro de la unidad",
        coverage="1967(1)2012",
        dictionary="yes",
        original="Data_Flag",
    ),
    col(
        "is_imputed_record",
        "STRING",
        "Indica se o registro inteiro da unidade foi imputado",
        "Whether the entire unit record was imputed",
        "Indica si el registro entero de la unidad fue imputado",
        coverage="1967(1)2012",
        dictionary="yes",
        original="Imputed Record",
    ),
]

# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela a que a chave se refere",
        "Name of the table the key belongs to",
        "Nombre de la tabla a la que pertenece la clave",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna a que a chave se refere",
        "Name of the column the key belongs to",
        "Nombre de la columna a la que pertenece la clave",
    ),
    col(
        "chave",
        "STRING",
        "Valor codificado presente na coluna",
        "Coded value present in the column",
        "Valor codificado presente en la columna",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal da correspondencia entre chave e valor",
        "Temporal coverage of the key to value correspondence",
        "Cobertura temporal de la correspondencia entre clave y valor",
    ),
    col(
        "valor",
        "STRING",
        "Descrição correspondente ao valor codificado",
        "Description matching the coded value",
        "Descripción correspondiente al valor codificado",
    ),
]

TABLES = {
    "government_unit": GOVERNMENT_UNIT,
    "employment": EMPLOYMENT,
    "employment_unit": EMPLOYMENT_UNIT,
    "finance": FINANCE,
    "finance_unit": FINANCE_UNIT,
    "dicionario": DICIONARIO,
}


PT_COLUMNS = {HEADER.index("description_pt"), HEADER.index("observations_pt")}
ES_COLUMNS = {HEADER.index("description_es"), HEADER.index("observations_es")}


# Words that must never reach the CSV bare, per language. Every accent defect
# review found came from text added to this file after a one-off pass over it,
# so this check runs on every generation rather than being remembered. It is
# language-aware because the base forms differ: "Ano" is correct Portuguese and
# wrong Spanish.
FORBIDDEN_BARE = {
    language: sorted(set(SHARED_SPELLING) | set(LANGUAGE_SPELLING[language]))
    for language in ("pt", "es")
}
BARE_PATTERN = {
    language: re.compile(
        r"(?<![A-Za-zÀ-ÿ])(" + "|".join(words) + r")(?![A-Za-zÀ-ÿ])"
    )
    for language, words in FORBIDDEN_BARE.items()
}


def assert_spelled(rows: list[list[str]], table: str) -> None:
    """Fail if any translated cell still carries an unaccented base word."""
    for row in rows:
        for index in sorted(PT_COLUMNS | ES_COLUMNS):
            language = "pt" if index in PT_COLUMNS else "es"
            found = BARE_PATTERN[language].findall(row[index])
            if found:
                raise SystemExit(
                    f"{table}.{row[0]}: unaccented {sorted(set(found))} in "
                    f"{HEADER[index]}"
                )


def main() -> None:
    """Write one architecture CSV per table."""
    ARCH.mkdir(parents=True, exist_ok=True)
    for table, rows in TABLES.items():
        path = ARCH / f"sheet_{table}.csv"
        spelled = [
            [
                spell(str(value), "pt")
                if index in PT_COLUMNS
                else spell(str(value), "es")
                if index in ES_COLUMNS
                else value
                for index, value in enumerate(row)
            ]
            for row in rows
        ]
        assert_spelled(spelled, table)
        with path.open("w", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(HEADER)
            writer.writerows(spelled)
        print(f"{path.name}: {len(rows)} columns")


if __name__ == "__main__":
    main()
