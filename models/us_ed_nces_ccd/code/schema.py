#!/usr/bin/env python3
"""Canonical schema for us_ed_nces_ccd (NCES Common Core of Data).

Single source of truth. Every downstream artifact -- architecture CSVs, the
cleaning code, the dbt models, `schema.yml`, and the backend column payloads --
is generated from the specs below, so the five tables cannot drift apart.

Source
------
The data is downloaded from the Urban Institute Education Data Portal
(https://educationdata.urban.org), which republishes the NCES Common Core of
Data and the Census/NCES F-33 school district finance survey after
harmonizing variable names, race and grade categories, and missing-value codes
across the full 1986-2024 span. The raw NCES flat files change layout
repeatedly over those 39 years (three-way state splits before 1998, a wide
membership file before 2016-17, a five-category race standard before 2008-09);
the Urban harmonization is what makes a single 39-year panel tractable.

Urban ships the portal under ODC-By v1.0. The underlying NCES and Census
collections are US Government works in the public domain.

Conventions applied here
------------------------
* `year` is the fall of the school year: year = 2020 means school year
  2020-21. For `district_finance` this is fiscal year 2021 (the F-33 for
  school year 2020-21).
* Identifiers are STRING and carry the `_id` suffix (English dataset).
  `school_id` is the 12-character NCES school id (Urban `ncessch`);
  `agency_id` is the 7-character NCES LEAID (Urban `leaid`).
* Urban strips leading zeros from `leaid` in the enrollment extracts and from
  `ncessch` in one row. Every identifier is re-padded on load -- see
  `PAD` and `NCESSCH_FIXUPS`.
* -1 / -2 / -3 are Urban's sentinel codes for "missing/not reported",
  "not applicable" and "suppressed". They are mapped to NULL on every column
  EXCEPT `grade`, where -1 is the legitimate code for prekindergarten.
* Numeric-looking codes with no meaningful arithmetic (school level, charter
  flag, FIPS, locale) are STRING with `covered_by_dictionary = yes`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DATASET = "us_ed_nces_ccd"

# --------------------------------------------------------------------------
# Sentinels and identifier repair
# --------------------------------------------------------------------------

#: Urban's missing-value codes, applied per column (never to ``grade``).
SENTINELS = (-1, -2, -3)

#: The backend measurement-unit vocabulary has 65 entries and no slug for
#: full-time equivalents, decimal degrees, or a count of schools. Those columns
#: therefore carry no unit; the unit is stated in the column description
#: instead. `usd`, `year` and `person` are the three that do exist and are used.
UNIT_VOCABULARY_GAPS = ("full-time equivalent", "decimal degree", "school")

#: Columns on which a negative sentinel is a legitimate value, not a missing
#: marker. ``grade = -1`` is prekindergarten.
SENTINEL_EXEMPT = {"grade"}

#: Identifier columns and the width they are zero-padded to.
PAD = {"school_id": 12, "agency_id": 7, "state_id": 2, "county_id": 5}

#: A single school-year row in the source carries an 11-character `ncessch`
#: (Ashfield-Plainfield Regional, Massachusetts, 1986). Left-padding it would
#: imply state FIPS 02 (Alaska); the correct id is LEAID + 5-digit school
#: number. Corrected explicitly, and identically, in every table so the
#: school and enrollment tables stay joinable.
NCESSCH_FIXUPS = {"25000031636": "250000301636"}


@dataclass
class Col:
    """One output column."""

    name: str
    source: str | None  # column name in the Urban CSV; None if derived
    type: str  # BigQuery type
    desc_en: str
    desc_pt: str
    desc_es: str
    unit: str = ""
    dictionary: bool = False
    directory: str = ""
    observations: str = ""
    observations_pt: str = ""
    observations_es: str = ""
    coverage: str = ""

    @property
    def original_name(self) -> str:
        return self.source or ""


@dataclass
class Table:
    slug: str
    name_en: str
    name_pt: str
    name_es: str
    desc_en: str
    desc_pt: str
    desc_es: str
    columns: list[Col]
    partition: str = "year"
    cluster: list[str] = field(default_factory=list)
    year_min: int = 1986
    year_max: int = 2024
    entities: list[str] = field(default_factory=list)
    #: column -> observation level entity slug
    entity_columns: dict[str, str] = field(default_factory=dict)

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]


# --------------------------------------------------------------------------
# Shared key / geography columns
# --------------------------------------------------------------------------

YEAR = Col(
    "year",
    "year",
    "INT64",
    "Fall year of the school year the record refers to (2020 denotes school year 2020-21)",
    "Ano de outono do ano letivo a que o registro se refere (2020 indica o ano letivo 2020-21)",
    "Año de otoño del año escolar al que se refiere el registro (2020 indica el año escolar 2020-21)",
    unit="year",
    directory="br_bd_diretorios_data_tempo.ano:ano",
)

SCHOOL_ID = Col(
    "school_id",
    "ncessch",
    "STRING",
    "NCES school identification number (12 characters: 7-character LEAID followed by a 5-digit school number)",
    "Código NCES da escola (12 caracteres: LEAID de 7 caracteres seguido do número da escola de 5 dígitos)",
    "Código NCES de la escuela (12 caracteres: LEAID de 7 caracteres seguido del número de escuela de 5 dígitos)",
    directory="br_bd_diretorios_us.school:id_school",
    observations=(
        "Zero-padded to 12 characters on load. The single 11-character value in the "
        "source (25000031636) is corrected to 250000301636."
    ),
)

AGENCY_ID = Col(
    "agency_id",
    "leaid",
    "STRING",
    "NCES local education agency (school district) identification number, or LEAID (7 characters: 2-digit state FIPS followed by a 5-digit agency number)",
    "Código NCES da agência local de educação (distrito escolar), ou LEAID (7 caracteres: FIPS estadual de 2 dígitos seguido do número da agência de 5 dígitos)",
    "Código NCES de la agencia local de educación (distrito escolar), o LEAID (7 caracteres: FIPS estatal de 2 dígitos seguido del número de agencia de 5 dígitos)",
    directory="br_bd_diretorios_us.school_district:id_school_district",
    observations="Zero-padded to 7 characters on load; the source strips leading zeros in the enrollment extracts.",
)

STATE_ID = Col(
    "state_id",
    "fips",
    "STRING",
    "State FIPS code as reported by the CCD",
    "Código FIPS do estado conforme informado pelo CCD",
    "Código FIPS del estado según lo informado por el CCD",
    dictionary=True,
    observations=(
        "Zero-padded to 2 characters. The CCD extends the state FIPS list with codes "
        "for jurisdictions that are not states (58 Department of Defense schools "
        "overseas, 59 Bureau of Indian Education, 61 and 63 Department of Defense "
        "domestic and overseas areas), so this column is not a strict foreign key "
        "into the state directory."
    ),
)


def _geo_cols(prefix: str) -> list[Col]:
    """Location or mailing address block, identical in both directory tables."""
    kind = {
        "location": ("location", "de localização", "de ubicación"),
        "mailing": ("mailing", "de correspondência", "postal"),
    }[prefix]
    en, pt, es = kind
    return [
        Col(
            f"street_{prefix}",
            f"street_{prefix}",
            "STRING",
            f"Street of the {en} address",
            f"Logradouro do endereço {pt}",
            f"Calle de la dirección {es}",
        ),
        Col(
            f"city_{prefix}",
            f"city_{prefix}",
            "STRING",
            f"City of the {en} address",
            f"Cidade do endereço {pt}",
            f"Ciudad de la dirección {es}",
        ),
        Col(
            f"state_{prefix}",
            f"state_{prefix}",
            "STRING",
            f"USPS state abbreviation of the {en} address",
            f"Sigla USPS do estado do endereço {pt}",
            f"Sigla USPS del estado de la dirección {es}",
        ),
        Col(
            f"zip_{prefix}",
            f"zip_{prefix}",
            "STRING",
            f"ZIP code of the {en} address",
            f"CEP (ZIP code) do endereço {pt}",
            f"Código postal (ZIP) de la dirección {es}",
        ),
    ]


# --------------------------------------------------------------------------
# school
# --------------------------------------------------------------------------

SCHOOL_COLUMNS: list[Col] = [
    YEAR,
    SCHOOL_ID,
    AGENCY_ID,
    STATE_ID,
    Col(
        "county_id",
        "county_code",
        "STRING",
        "County FIPS code (5 digits: state FIPS followed by the 3-digit county code)",
        "Código FIPS do condado (5 dígitos: FIPS do estado seguido do código do condado de 3 dígitos)",
        "Código FIPS del condado (5 dígitos: FIPS del estado seguido del código de condado de 3 dígitos)",
        directory="br_bd_diretorios_us.county:id_county",
    ),
    Col(
        "school_id_local",
        "school_id",
        "STRING",
        "NCES school number within the state (state FIPS followed by the school number)",
        "Número NCES da escola dentro do estado (FIPS do estado seguido do número da escola)",
        "Número NCES de la escuela dentro del estado (FIPS del estado seguido del número de escuela)",
        observations="Not the 12-character NCES id; see school_id for that.",
    ),
    Col(
        "school_id_state",
        "seasch",
        "STRING",
        "School identification number assigned by the state education agency",
        "Código da escola atribuído pela secretaria estadual de educação",
        "Código de la escuela asignado por la agencia estatal de educación",
    ),
    Col(
        "agency_id_state",
        "state_leaid",
        "STRING",
        "Local education agency identification number assigned by the state",
        "Código da agência local de educação atribuído pelo estado",
        "Código de la agencia local de educación asignado por el estado",
    ),
    Col(
        "school_name",
        "school_name",
        "STRING",
        "School name",
        "Nome da escola",
        "Nombre de la escuela",
    ),
    Col(
        "agency_name",
        "lea_name",
        "STRING",
        "Local education agency (school district) name",
        "Nome da agência local de educação (distrito escolar)",
        "Nombre de la agencia local de educación (distrito escolar)",
    ),
    Col(
        "cbsa_id",
        "cbsa",
        "STRING",
        "OMB core-based statistical area code of the vintage in force in the school year",
        "Código OMB da área estatística baseada em núcleo (CBSA) da versão vigente no ano letivo",
        "Código OMB del área estadística basada en núcleo (CBSA) de la versión vigente en el año escolar",
        observations="CBSA delineations are revised periodically; codes are not comparable across all years.",
    ),
    Col(
        "csa_id",
        "csa",
        "STRING",
        "OMB combined statistical area code of the vintage in force in the school year",
        "Código OMB da área estatística combinada (CSA) da versão vigente no ano letivo",
        "Código OMB del área estadística combinada (CSA) de la versión vigente en el año escolar",
    ),
    Col(
        "congressional_district_id",
        "congress_district_id",
        "STRING",
        "State and congressional district identification number",
        "Código do estado e do distrito congressional",
        "Código del estado y del distrito congresional",
    ),
    Col(
        "state_leg_district_lower",
        "state_leg_district_lower",
        "STRING",
        "State legislative district, lower chamber",
        "Distrito legislativo estadual, câmara baixa",
        "Distrito legislativo estatal, cámara baja",
    ),
    Col(
        "state_leg_district_upper",
        "state_leg_district_upper",
        "STRING",
        "State legislative district, upper chamber",
        "Distrito legislativo estadual, câmara alta",
        "Distrito legislativo estatal, cámara alta",
    ),
    *_geo_cols("location"),
    *_geo_cols("mailing"),
    Col(
        "phone",
        "phone",
        "STRING",
        "School telephone number",
        "Telefone da escola",
        "Teléfono de la escuela",
    ),
    Col(
        "latitude",
        "latitude",
        "FLOAT64",
        "Latitude of the school in decimal degrees",
        "Latitude da escola em graus decimais",
        "Latitud de la escuela en grados decimales",
        unit="",
    ),
    Col(
        "longitude",
        "longitude",
        "FLOAT64",
        "Longitude of the school in decimal degrees",
        "Longitude da escola em graus decimais",
        "Longitud de la escuela en grados decimales",
        unit="",
    ),
    Col(
        "urban_centric_locale",
        "urban_centric_locale",
        "STRING",
        "Degree of urbanization (urban-centric locale) of the school location",
        "Grau de urbanização (localidade urbano-cêntrica) da localização da escola",
        "Grado de urbanización (localidad urbano-céntrica) de la ubicación de la escuela",
        dictionary=True,
        observations="Nine categories before 2005 and twelve from 2005 onward; both code sets appear in the dictionary.",
    ),
    Col(
        "school_level",
        "school_level",
        "STRING",
        "School level",
        "Nível de ensino da escola",
        "Nivel de enseñanza de la escuela",
        dictionary=True,
    ),
    Col(
        "school_type",
        "school_type",
        "STRING",
        "School type",
        "Tipo de escola",
        "Tipo de escuela",
        dictionary=True,
    ),
    Col(
        "school_status",
        "school_status",
        "STRING",
        "Operational status of the school at the start of the school year",
        "Situação operacional da escola no início do ano letivo",
        "Situación operativa de la escuela al inicio del año escolar",
        dictionary=True,
    ),
    Col(
        "lowest_grade_offered",
        "lowest_grade_offered",
        "STRING",
        "Lowest grade offered by the school",
        "Menor série oferecida pela escola",
        "Grado más bajo ofrecido por la escuela",
        dictionary=True,
    ),
    Col(
        "highest_grade_offered",
        "highest_grade_offered",
        "STRING",
        "Highest grade offered by the school",
        "Maior série oferecida pela escola",
        "Grado más alto ofrecido por la escuela",
        dictionary=True,
    ),
    Col(
        "bureau_indian_education",
        "bureau_indian_education",
        "STRING",
        "Whether the school is funded or operated by the Bureau of Indian Education",
        "Indica se a escola é financiada ou operada pelo Bureau of Indian Education",
        "Indica si la escuela es financiada u operada por el Bureau of Indian Education",
        dictionary=True,
    ),
    Col(
        "title_i_status",
        "title_i_status",
        "STRING",
        "Title I status of the school",
        "Situação da escola quanto ao Title I",
        "Situación de la escuela respecto al Title I",
        dictionary=True,
    ),
    Col(
        "title_i_eligible",
        "title_i_eligible",
        "STRING",
        "Whether the school is eligible for Title I funding",
        "Indica se a escola é elegível ao financiamento do Title I",
        "Indica si la escuela es elegible para el financiamiento del Title I",
        dictionary=True,
    ),
    Col(
        "title_i_schoolwide",
        "title_i_schoolwide",
        "STRING",
        "Whether the school operates a schoolwide Title I program",
        "Indica se a escola opera um programa Title I de abrangência escolar",
        "Indica si la escuela opera un programa Title I de alcance escolar",
        dictionary=True,
    ),
    Col(
        "charter",
        "charter",
        "STRING",
        "Whether the school is a charter school",
        "Indica se a escola é uma charter school",
        "Indica si la escuela es una charter school",
        dictionary=True,
    ),
    Col(
        "magnet",
        "magnet",
        "STRING",
        "Whether the school is a magnet school",
        "Indica se a escola é uma magnet school",
        "Indica si la escuela es una magnet school",
        dictionary=True,
    ),
    Col(
        "shared_time",
        "shared_time",
        "STRING",
        "Whether the school operates on a shared-time basis with another school",
        "Indica se a escola opera em tempo compartilhado com outra escola",
        "Indica si la escuela opera en tiempo compartido con otra escuela",
        dictionary=True,
    ),
    Col(
        "virtual",
        "virtual",
        "STRING",
        "Whether the school delivers instruction virtually",
        "Indica se a escola oferece ensino de forma virtual",
        "Indica si la escuela imparte enseñanza de forma virtual",
        dictionary=True,
    ),
    Col(
        "elem_cedp",
        "elem_cedp",
        "STRING",
        "Elementary school indicator derived by the Urban Institute Center on Education Data and Policy",
        "Indicador de escola primária derivado pelo Center on Education Data and Policy do Urban Institute",
        "Indicador de escuela primaria derivado por el Center on Education Data and Policy del Urban Institute",
        dictionary=True,
    ),
    Col(
        "middle_cedp",
        "middle_cedp",
        "STRING",
        "Middle school indicator derived by the Urban Institute Center on Education Data and Policy",
        "Indicador de escola de ensino fundamental II derivado pelo Center on Education Data and Policy do Urban Institute",
        "Indicador de escuela media derivado por el Center on Education Data and Policy del Urban Institute",
        dictionary=True,
    ),
    Col(
        "high_cedp",
        "high_cedp",
        "STRING",
        "High school indicator derived by the Urban Institute Center on Education Data and Policy",
        "Indicador de escola de ensino médio derivado pelo Center on Education Data and Policy do Urban Institute",
        "Indicador de escuela secundaria derivado por el Center on Education Data and Policy del Urban Institute",
        dictionary=True,
    ),
    Col(
        "ungrade_cedp",
        "ungrade_cedp",
        "STRING",
        "Ungraded school indicator derived by the Urban Institute Center on Education Data and Policy",
        "Indicador de escola sem seriação derivado pelo Center on Education Data and Policy do Urban Institute",
        "Indicador de escuela sin graduación derivado por el Center on Education Data and Policy del Urban Institute",
        dictionary=True,
    ),
    Col(
        "lunch_program",
        "lunch_program",
        "STRING",
        "National School Lunch Program participation status",
        "Situação de participação no National School Lunch Program",
        "Situación de participación en el National School Lunch Program",
        dictionary=True,
    ),
    Col(
        "teachers_fte",
        "teachers_fte",
        "FLOAT64",
        "Full-time equivalent classroom teachers",
        "Professores em sala de aula em equivalente de tempo integral",
        "Docentes de aula en equivalente de tiempo completo",
        unit="",
    ),
    Col(
        "enrollment",
        "enrollment",
        "INT64",
        "Total student enrollment reported in the school directory",
        "Matrícula total de estudantes informada no diretório de escolas",
        "Matrícula total de estudiantes informada en el directorio de escuelas",
        unit="person",
    ),
    Col(
        "free_lunch",
        "free_lunch",
        "INT64",
        "Students eligible for a free lunch under the National School Lunch Act",
        "Estudantes elegíveis a almoço gratuito sob o National School Lunch Act",
        "Estudiantes elegibles para almuerzo gratuito bajo el National School Lunch Act",
        unit="person",
    ),
    Col(
        "reduced_price_lunch",
        "reduced_price_lunch",
        "INT64",
        "Students eligible for a reduced-price lunch under the National School Lunch Act",
        "Estudantes elegíveis a almoço a preço reduzido sob o National School Lunch Act",
        "Estudiantes elegibles para almuerzo a precio reducido bajo el National School Lunch Act",
        unit="person",
    ),
    Col(
        "free_or_reduced_price_lunch",
        "free_or_reduced_price_lunch",
        "INT64",
        "Students eligible for a free or reduced-price lunch under the National School Lunch Act",
        "Estudantes elegíveis a almoço gratuito ou a preço reduzido sob o National School Lunch Act",
        "Estudiantes elegibles para almuerzo gratuito o a precio reducido bajo el National School Lunch Act",
        unit="person",
    ),
    Col(
        "direct_certification",
        "direct_certification",
        "INT64",
        "Students eligible for a free lunch through direct certification",
        "Estudantes elegíveis a almoço gratuito por certificação direta",
        "Estudiantes elegibles para almuerzo gratuito por certificación directa",
        unit="person",
    ),
]


# --------------------------------------------------------------------------
# Phrase-level translation of the formulaic F-33 and staff labels
# --------------------------------------------------------------------------
#
# The 157 F-33 money columns and the 27 staff categories are named from a small,
# rigid vocabulary ("Federal revenue through the state for ...", "Employee
# benefits for ...", "Number of full-time equivalent ..."). Translating the
# vocabulary once and composing is more reliable than hand-typing 550 strings,
# and it keeps the three languages structurally parallel. Ordered longest-first
# so multi-word phrases win over their constituent words.

_GLOSSARY: list[tuple[str, str, str]] = [
    # multi-word programme and function names -------------------------------
    (
        "student transportation support services",
        "serviços de apoio ao transporte escolar",
        "servicios de apoyo al transporte escolar",
    ),
    (
        "instructional support services",
        "serviços de apoio ao corpo docente",
        "servicios de apoyo al personal docente",
    ),
    (
        "Expenditures on instruction",
        "Despesa com ensino",
        "Gasto en enseñanza",
    ),
    (
        "Expenditures on textbooks",
        "Despesa com livros didáticos",
        "Gasto en libros de texto",
    ),
    (
        "Education Stabilization Fund - Rethink K12 Models",
        "Education Stabilization Fund - Rethink K12 Models",
        "Education Stabilization Fund - Rethink K12 Models",
    ),
    (
        "Education Stabilization Fund - Reimagine Workforce Preparation",
        "Education Stabilization Fund - Reimagine Workforce Preparation",
        "Education Stabilization Fund - Reimagine Workforce Preparation",
    ),
    (
        "Project School Emergency Response to Violence",
        "Project School Emergency Response to Violence",
        "Project School Emergency Response to Violence",
    ),
    (
        "Coronavirus Relief Fund",
        "Coronavirus Relief Fund",
        "Coronavirus Relief Fund",
    ),
    (
        "Local revenue from miscellaneous sources",
        "Receita local de fontes diversas",
        "Ingresos locales de fuentes diversas",
    ),
    (
        "Total capital outlay expenditures",
        "Despesa total de investimento de capital",
        "Gasto total de inversión de capital",
    ),
    (
        "Local revenue from public utility taxes",
        "Receita local de impostos sobre concessionárias de serviços públicos",
        "Ingresos locales de impuestos sobre empresas de servicios públicos",
    ),
    (
        "Local revenue from all other taxes",
        "Receita local de todos os demais impostos",
        "Ingresos locales de todos los demás impuestos",
    ),
    (
        "Local revenue from school lunches",
        "Receita local da merenda escolar",
        "Ingresos locales del almuerzo escolar",
    ),
    (
        "general administration support services",
        "serviços de apoio à administração geral",
        "servicios de apoyo a la administración general",
    ),
    (
        "school administration support services",
        "serviços de apoio à administração escolar",
        "servicios de apoyo a la administración escolar",
    ),
    (
        "instructional staff support services",
        "serviços de apoio ao corpo docente",
        "servicios de apoyo al personal docente",
    ),
    (
        "student support services staff",
        "profissionais de serviços de apoio ao estudante",
        "personal de servicios de apoyo al estudiante",
    ),
    (
        "Federal revenue through state for effective instructional practices",
        "Receita federal repassada pelo estado para práticas de ensino eficazes",
        "Ingresos federales transferidos por el estado para prácticas de enseñanza eficaces",
    ),
    (
        "Federal revenue through state to support student achievement",
        "Receita federal repassada pelo estado para apoiar o desempenho dos estudantes",
        "Ingresos federales transferidos por el estado para apoyar el desempeño de los estudiantes",
    ),
    (
        "Federal revenue through state to 21st century learning centers",
        "Receita federal repassada pelo estado para os 21st Century Community Learning Centers",
        "Ingresos federales transferidos por el estado para los 21st Century Community Learning Centers",
    ),
    (
        "Federal revenue through state for rural low-income schools",
        "Receita federal repassada pelo estado para escolas rurais de baixa renda",
        "Ingresos federales transferidos por el estado para escuelas rurales de bajos ingresos",
    ),
    (
        "State revenue for special education programs",
        "Receita estadual para programas de educação especial",
        "Ingresos estatales para programas de educación especial",
    ),
    (
        "State revenue for vocational education programs",
        "Receita estadual para programas de educação profissional",
        "Ingresos estatales para programas de educación profesional",
    ),
    (
        "State revenue for capital outlay and debt services programs",
        "Receita estadual para programas de investimento de capital e serviço da dívida",
        "Ingresos estatales para programas de inversión de capital y servicio de la deuda",
    ),
    (
        "State revenue for transportation programs",
        "Receita estadual para programas de transporte",
        "Ingresos estatales para programas de transporte",
    ),
    (
        "State revenue for other programs",
        "Receita estadual para outros programas",
        "Ingresos estatales para otros programas",
    ),
    (
        "Local revenue from parent government contributions",
        "Receita local de repasses do governo controlador",
        "Ingresos locales de transferencias del gobierno controlador",
    ),
    (
        "Local revenue from general sales taxes",
        "Receita local de impostos gerais sobre vendas",
        "Ingresos locales de impuestos generales sobre ventas",
    ),
    (
        "Local revenue from district activity receipts",
        "Receita local de atividades do distrito escolar",
        "Ingresos locales de actividades del distrito escolar",
    ),
    (
        "Local revenue from property sales",
        "Receita local da venda de propriedades",
        "Ingresos locales de la venta de propiedades",
    ),
    (
        "Capital outlay for instructional equipment",
        "Investimento de capital em equipamentos de ensino",
        "Inversión de capital en equipos de enseñanza",
    ),
    (
        "Capital outlay for other equipment",
        "Investimento de capital em outros equipamentos",
        "Inversión de capital en otros equipos",
    ),
    (
        "Teacher salaries for special education programs",
        "Salários de professores em programas de educação especial",
        "Salarios de docentes en programas de educación especial",
    ),
    (
        "Teacher salaries for vocational education programs",
        "Salários de professores em programas de educação profissional",
        "Salarios de docentes en programas de educación profesional",
    ),
    (
        "Teacher salaries for other education programs",
        "Salários de professores em outros programas educacionais",
        "Salarios de docentes en otros programas educativos",
    ),
    (
        "Local revenue from nonspecified student fees",
        "Receita local de taxas estudantis não especificadas",
        "Ingresos locales de tasas estudiantiles no especificadas",
    ),
    (
        "Capital outlay for nonspecified equipment",
        "Investimento de capital em equipamentos não especificados",
        "Inversión de capital en equipos no especificados",
    ),
    (
        "Federal revenue through the state for the Individuals with Disabilities Education Act",
        "Receita federal repassada pelo estado para o Individuals with Disabilities Education Act",
        "Ingresos federales transferidos por el estado para el Individuals with Disabilities Education Act",
    ),
    (
        "Federal revenue through the state for the Child Nutrition Act",
        "Receita federal repassada pelo estado para o Child Nutrition Act",
        "Ingresos federales transferidos por el estado para el Child Nutrition Act",
    ),
    (
        "Federal revenue through the state for safe and drug-free schools",
        "Receita federal repassada pelo estado para escolas seguras e livres de drogas",
        "Ingresos federales transferidos por el estado para escuelas seguras y libres de drogas",
    ),
    (
        "Federal revenue through state for effective instruction",
        "Receita federal repassada pelo estado para o ensino eficaz",
        "Ingresos federales transferidos por el estado para la enseñanza eficaz",
    ),
    (
        "Federal revenue from the American Recovery and Reinvestment Act",
        "Receita federal do American Recovery and Reinvestment Act",
        "Ingresos federales del American Recovery and Reinvestment Act",
    ),
    (
        "Current expenditures for the American Recovery and Reinvestment Act",
        "Despesa corrente do American Recovery and Reinvestment Act",
        "Gasto corriente del American Recovery and Reinvestment Act",
    ),
    (
        "Capital outlay for the American Recovery and Reinvestment Act",
        "Investimento de capital do American Recovery and Reinvestment Act",
        "Inversión de capital del American Recovery and Reinvestment Act",
    ),
    (
        "Current expenditures made by regional education service agencies on behalf of the local education agency",
        "Despesa corrente realizada por agências regionais de serviços educacionais em nome da agência local de educação",
        "Gasto corriente realizado por agencias regionales de servicios educativos en nombre de la agencia local de educación",
    ),
    (
        "Current expenditures for support services for nonspecified purposes",
        "Despesa corrente com serviços de apoio para finalidades não especificadas",
        "Gasto corriente en servicios de apoyo para fines no especificados",
    ),
    (
        "Federal revenue through the state for other purposes",
        "Receita federal repassada pelo estado para outras finalidades",
        "Ingresos federales transferidos por el estado para otros fines",
    ),
    (
        "Federal revenue for nonspecified purposes",
        "Receita federal para finalidades não especificadas",
        "Ingresos federales para fines no especificados",
    ),
    (
        "Direct federal revenue for other purposes",
        "Receita federal direta para outras finalidades",
        "Ingresos federales directos para otros fines",
    ),
    (
        "State revenue for nonspecified purposes",
        "Receita estadual para finalidades não especificadas",
        "Ingresos estatales para fines no especificados",
    ),
    (
        "Local revenue for nonspecified purposes",
        "Receita local para finalidades não especificadas",
        "Ingresos locales para fines no especificados",
    ),
    ("instructional", "docente", "docente"),
    (
        "Federal revenue through the state for vocational and tech education",
        "Receita federal repassada pelo estado para educação profissional e técnica",
        "Ingresos federales transferidos por el estado para educación profesional y técnica",
    ),
    (
        "Federal revenue through state support small and rural district achievement",
        "Receita federal repassada pelo estado para o desempenho de distritos pequenos e rurais",
        "Ingresos federales transferidos por el estado para el desempeño de distritos pequeños y rurales",
    ),
    (
        "Federal revenue through the state for math, science, and teacher quality",
        "Receita federal repassada pelo estado para matemática, ciências e qualificação docente",
        "Ingresos federales transferidos por el estado para matemáticas, ciencias y calificación docente",
    ),
    (
        "State revenue for compensatory and basic skills programs",
        "Receita estadual para programas de educação compensatória e de habilidades básicas",
        "Ingresos estatales para programas de educación compensatoria y de habilidades básicas",
    ),
    (
        "State revenue, on behalf of the local education agency, for other benefits than employee benefits",
        "Receita estadual, em nome da agência local de educação, para benefícios que não os de empregados",
        "Ingresos estatales, en nombre de la agencia local de educación, para prestaciones distintas de las de empleados",
    ),
    (
        "State revenue, on behalf of the local education agency, for employee benefits",
        "Receita estadual, em nome da agência local de educação, para benefícios a empregados",
        "Ingresos estatales, en nombre de la agencia local de educación, para prestaciones a empleados",
    ),
    (
        "Local revenue from individual and corporate income taxes",
        "Receita local de impostos sobre a renda de pessoas físicas e jurídicas",
        "Ingresos locales de impuestos sobre la renta de personas físicas y jurídicas",
    ),
    (
        "Local revenue from tuition fees from pupils and parents",
        "Receita local de mensalidades pagas por estudantes e responsáveis",
        "Ingresos locales de matrículas pagadas por estudiantes y responsables",
    ),
    (
        "Local revenue from transportation fees from pupils and parents",
        "Receita local de taxas de transporte pagas por estudantes e responsáveis",
        "Ingresos locales de tasas de transporte pagadas por estudiantes y responsables",
    ),
    (
        "Current expenditures for other elementary or secondary school purposes",
        "Despesa corrente com outras finalidades primárias ou secundárias",
        "Gasto corriente en otros fines primarios o secundarios",
    ),
    (
        "Non-elementary or secondary school expenditures for community services",
        "Despesa não primária ou secundária com serviços comunitários",
        "Gasto no primario o secundario en servicios comunitarios",
    ),
    (
        "Non-elementary or secondary school expenditures for adult education",
        "Despesa não primária ou secundária com educação de adultos",
        "Gasto no primario o secundario en educación de adultos",
    ),
    (
        "Non-elementary or secondary school expenditures for other purposes",
        "Despesa não primária ou secundária com outras finalidades",
        "Gasto no primario o secundario en otros fines",
    ),
    (
        "Expenditures for utilities and energy services",
        "Despesa com serviços públicos e energia",
        "Gasto en servicios públicos y energía",
    ),
    (
        "Expenditures for technology-related supplies and purchased services",
        "Despesa com insumos de tecnologia e serviços contratados",
        "Gasto en insumos de tecnología y servicios contratados",
    ),
    (
        "Expenditures using Federal COVID relief funds on Technology and related support services",
        "Despesa com recursos federais de auxílio à COVID em tecnologia e serviços de apoio relacionados",
        "Gasto con fondos federales de ayuda por COVID en tecnología y servicios de apoyo relacionados",
    ),
    (
        "Expenditures using Federal COVID relief funds on Technology and equipment",
        "Despesa com recursos federais de auxílio à COVID em tecnologia e equipamentos",
        "Gasto con fondos federales de ayuda por COVID en tecnología y equipos",
    ),
    (
        "Expenditures using Federal COVID relief funds on PLANT maintenance",
        "Despesa com recursos federais de auxílio à COVID em manutenção das instalações",
        "Gasto con fondos federales de ayuda por COVID en mantenimiento de las instalaciones",
    ),
    (
        "Expenditures using Federal COVID relief funds on capital outlay",
        "Despesa com recursos federais de auxílio à COVID em investimento de capital",
        "Gasto con fondos federales de ayuda por COVID en inversión de capital",
    ),
    (
        "Expenditures using Federal COVID relief funds on support services",
        "Despesa com recursos federais de auxílio à COVID em serviços de apoio",
        "Gasto con fondos federales de ayuda por COVID en servicios de apoyo",
    ),
    (
        "Expenditures using Federal COVID relief funds on instruction",
        "Despesa com recursos federais de auxílio à COVID em ensino",
        "Gasto con fondos federales de ayuda por COVID en enseñanza",
    ),
    (
        "Expenditures using Federal COVID relief funds on Food Services",
        "Despesa com recursos federais de auxílio à COVID em serviços de alimentação",
        "Gasto con fondos federales de ayuda por COVID en servicios de alimentación",
    ),
    (
        "Expenditures using Federal COVID relief funds",
        "Despesa com recursos federais de auxílio à COVID",
        "Gasto con fondos federales de ayuda por COVID",
    ),
    (
        "Current special education expenditures for instructional support services",
        "Despesa corrente de educação especial com serviços de apoio ao corpo docente",
        "Gasto corriente de educación especial en servicios de apoyo al personal docente",
    ),
    (
        "Current special education expenditures for pupil support services",
        "Despesa corrente de educação especial com serviços de apoio ao estudante",
        "Gasto corriente de educación especial en servicios de apoyo al estudiante",
    ),
    (
        "Current special education expenditures for Transportation services",
        "Despesa corrente de educação especial com serviços de transporte",
        "Gasto corriente de educación especial en servicios de transporte",
    ),
    (
        "Current special education expenditures for instruction",
        "Despesa corrente de educação especial com ensino",
        "Gasto corriente de educación especial en enseñanza",
    ),
    (
        "Current special education expenditures",
        "Despesa corrente de educação especial",
        "Gasto corriente de educación especial",
    ),
    (
        "Total non-elementary or secondary school expenditures",
        "Despesa total não primária ou secundária",
        "Gasto total no primario o secundario",
    ),
    (
        "Total current expenditures for other elementary or secondary",
        "Despesa corrente total com outras finalidades primárias ou secundárias",
        "Gasto corriente total en otros fines primarios o secundarios",
    ),
    (
        "Total current expenditures for support services",
        "Despesa corrente total com serviços de apoio",
        "Gasto corriente total en servicios de apoyo",
    ),
    (
        "Total current expenditures for elementary and secondary education",
        "Despesa corrente total com educação primária e secundária",
        "Gasto corriente total en educación primaria y secundaria",
    ),
    (
        "Total current expenditures for instruction",
        "Despesa corrente total com ensino",
        "Gasto corriente total en enseñanza",
    ),
    (
        "Total salary amount",
        "Valor total de salários",
        "Monto total de salarios",
    ),
    (
        "NCES local revenue, Census Bureau state revenue",
        "Receita local segundo o NCES, receita estadual segundo o Census Bureau",
        "Ingresos locales según el NCES, ingresos estatales según el Census Bureau",
    ),
    (
        "Federal CRRSA Act revenues",
        "Receitas federais do CRRSA Act",
        "Ingresos federales del CRRSA Act",
    ),
    (
        "Federal ARP Act revenues",
        "Receitas federais do ARP Act",
        "Ingresos federales del ARP Act",
    ),
    (
        "Revenue from State and Local COVID-19 Fiscal recovery measures",
        "Receita de medidas estaduais e locais de recuperação fiscal da COVID-19",
        "Ingresos de medidas estatales y locales de recuperación fiscal por COVID-19",
    ),
    (
        "instructional support services",
        "serviços de apoio ao corpo docente",
        "servicios de apoyo al personal docente",
    ),
    (
        "Total federal revenue",
        "Receita federal total",
        "Ingresos federales totales",
    ),
    (
        "Total state revenue",
        "Receita estadual total",
        "Ingresos estatales totales",
    ),
    ("Total local revenue", "Receita local total", "Ingresos locales totales"),
    ("Total salaries", "Salários totais", "Salarios totales"),
    (
        "Total employee benefits",
        "Benefícios totais a empregados",
        "Prestaciones totales a empleados",
    ),
    (
        "Total capital outlay",
        "Investimento de capital total",
        "Inversión de capital total",
    ),
    (
        "elementary and secondary education",
        "educação primária e secundária",
        "educación primaria y secundaria",
    ),
    (
        "Number of students for which the reporting local education agency is financially responsible",
        "Número de estudantes pelos quais a agência local de educação declarante é financeiramente responsável",
        "Número de estudiantes por los cuales la agencia local de educación declarante es financieramente responsable",
    ),
    (
        "Number of students enrolled in schools operated by the reporting local education agency",
        "Número de estudantes matriculados em escolas operadas pela agência local de educação declarante",
        "Número de estudiantes matriculados en escuelas operadas por la agencia local de educación declarante",
    ),
    (
        "US Census Bureau 14-digit government identification number",
        "Código de 14 dígitos do governo atribuído pelo US Census Bureau",
        "Código de 14 dígitos del gobierno asignado por el US Census Bureau",
    ),
    (
        "business, central, and other support services",
        "serviços de apoio administrativo, central e outros",
        "servicios de apoyo administrativo, central y otros",
    ),
    (
        "Federal revenue through the state",
        "Receita federal repassada pelo estado",
        "Ingresos federales transferidos por el estado",
    ),
    (
        "Direct federal revenue",
        "Receita federal direta",
        "Ingresos federales directos",
    ),
    (
        "Federal CARES Act revenues",
        "Receitas federais do CARES Act",
        "Ingresos federales del CARES Act",
    ),
    ("Federal revenue", "Receita federal", "Ingresos federales"),
    ("State revenue", "Receita estadual", "Ingresos estatales"),
    ("Local revenue", "Receita local", "Ingresos locales"),
    ("Total revenue", "Receita total", "Ingresos totales"),
    ("Current expenditures", "Despesa corrente", "Gasto corriente"),
    ("current expenditures", "despesa corrente", "gasto corriente"),
    ("Total expenditures", "Despesa total", "Gasto total"),
    ("Expenditures", "Despesa", "Gasto"),
    ("expenditures", "despesa", "gasto"),
    ("Capital outlay", "Investimento de capital", "Inversión de capital"),
    (
        "Employee benefits",
        "Benefícios a empregados",
        "Prestaciones a empleados",
    ),
    ("Teacher salaries", "Salários de professores", "Salarios de docentes"),
    ("Salaries", "Salários", "Salarios"),
    ("salaries", "salários", "salarios"),
    ("Payments to", "Pagamentos a", "Pagos a"),
    (
        "Long-term debt outstanding at beginning of fiscal year",
        "Dívida de longo prazo em aberto no início do exercício fiscal",
        "Deuda de largo plazo pendiente al inicio del ejercicio fiscal",
    ),
    (
        "Long-term debt outstanding at end of fiscal year",
        "Dívida de longo prazo em aberto no fim do exercício fiscal",
        "Deuda de largo plazo pendiente al final del ejercicio fiscal",
    ),
    (
        "Short-term debt outstanding at beginning of fiscal year",
        "Dívida de curto prazo em aberto no início do exercício fiscal",
        "Deuda de corto plazo pendiente al inicio del ejercicio fiscal",
    ),
    (
        "Short-term debt outstanding at end of fiscal year",
        "Dívida de curto prazo em aberto no fim do exercício fiscal",
        "Deuda de corto plazo pendiente al final del ejercicio fiscal",
    ),
    (
        "Long-term debt issued during fiscal year",
        "Dívida de longo prazo emitida no exercício fiscal",
        "Deuda de largo plazo emitida en el ejercicio fiscal",
    ),
    (
        "Long-term debt retired during fiscal year",
        "Dívida de longo prazo quitada no exercício fiscal",
        "Deuda de largo plazo amortizada en el ejercicio fiscal",
    ),
    ("Interest on debt", "Juros da dívida", "Intereses de la deuda"),
    (
        "Assets in a sinking fund",
        "Ativos em fundo de amortização",
        "Activos en fondo de amortización",
    ),
    (
        "Assets in a bond fund",
        "Ativos em fundo de títulos",
        "Activos en fondo de bonos",
    ),
    (
        "Assets in other funds",
        "Ativos em outros fundos",
        "Activos en otros fondos",
    ),
    ("Number of full-time equivalent", "Número de", "Número de"),
    ("Total full-time equivalent", "Total de", "Total de"),
    ("Number of full-time", "Número de", "Número de"),
    (
        "Number of other full-time equivalent staff",
        "Número de outros profissionais",
        "Número de otro personal",
    ),
    # function / object detail ----------------------------------------------
    (
        "general formula assistance",
        "auxílio de fórmula geral",
        "asistencia de fórmula general",
    ),
    (
        "staff improvement programs",
        "programas de aperfeiçoamento de pessoal",
        "programas de mejora del personal",
    ),
    (
        "compensatory and basic education",
        "educação compensatória e básica",
        "educación compensatoria y básica",
    ),
    (
        "capital outlay and debt service",
        "investimento de capital e serviço da dívida",
        "inversión de capital y servicio de la deuda",
    ),
    (
        "gifted and talented programs",
        "programas para superdotados e talentosos",
        "programas para superdotados y talentosos",
    ),
    (
        "bilingual education programs",
        "programas de educação bilíngue",
        "programas de educación bilingüe",
    ),
    ("bilingual education", "educação bilíngue", "educación bilingüe"),
    ("vocational education", "educação profissional", "educación profesional"),
    ("special education", "educação especial", "educación especial"),
    (
        "school lunch programs",
        "programas de merenda escolar",
        "programas de almuerzo escolar",
    ),
    ("school lunch", "merenda escolar", "almuerzo escolar"),
    ("food services", "serviços de alimentação", "servicios de alimentación"),
    ("food service", "serviço de alimentação", "servicio de alimentación"),
    (
        "pupil support services",
        "serviços de apoio ao estudante",
        "servicios de apoyo al estudiante",
    ),
    (
        "instructional staff support services",
        "serviços de apoio ao corpo docente",
        "servicios de apoyo al personal docente",
    ),
    (
        "student support services",
        "serviços de apoio ao estudante",
        "servicios de apoyo al estudiante",
    ),
    ("student transportation", "transporte escolar", "transporte escolar"),
    (
        "general administration",
        "administração geral",
        "administración general",
    ),
    (
        "school administration",
        "administração escolar",
        "administración escolar",
    ),
    (
        "operation and maintenance of plant",
        "operação e manutenção das instalações",
        "operación y mantenimiento de las instalaciones",
    ),
    (
        "operations and maintenance",
        "operação e manutenção",
        "operación y mantenimiento",
    ),
    (
        "enterprise operations",
        "operações de atividades comerciais",
        "operaciones de actividades comerciales",
    ),
    ("community services", "serviços comunitários", "servicios comunitarios"),
    ("adult education", "educação de adultos", "educación de adultos"),
    ("support services", "serviços de apoio", "servicios de apoyo"),
    ("instruction", "ensino", "enseñanza"),
    ("Instruction", "Ensino", "Enseñanza"),
    (
        "property taxes",
        "impostos sobre a propriedade",
        "impuestos sobre la propiedad",
    ),
    ("sales taxes", "impostos sobre vendas", "impuestos sobre ventas"),
    (
        "utility taxes",
        "impostos sobre serviços públicos",
        "impuestos sobre servicios públicos",
    ),
    ("income taxes", "impostos sobre a renda", "impuestos sobre la renta"),
    ("other taxes", "outros impostos", "otros impuestos"),
    ("interest earnings", "rendimentos de juros", "rendimientos de intereses"),
    ("rents and royalties", "aluguéis e royalties", "alquileres y regalías"),
    ("sale of property", "venda de propriedade", "venta de propiedad"),
    ("fines and forfeits", "multas e confiscos", "multas y decomisos"),
    (
        "private contributions",
        "contribuições privadas",
        "contribuciones privadas",
    ),
    ("tuition and fees", "mensalidades e taxas", "matrículas y tasas"),
    ("transportation fees", "taxas de transporte", "tasas de transporte"),
    (
        "textbook sales and rentals",
        "venda e aluguel de livros didáticos",
        "venta y alquiler de libros de texto",
    ),
    (
        "student activity receipts",
        "receitas de atividades estudantis",
        "ingresos de actividades estudiantiles",
    ),
    ("student fees", "taxas estudantis", "tasas estudiantiles"),
    (
        "other sales and services",
        "outras vendas e serviços",
        "otras ventas y servicios",
    ),
    ("cities and counties", "cidades e condados", "ciudades y condados"),
    (
        "other school systems",
        "outros sistemas escolares",
        "otros sistemas escolares",
    ),
    ("other school system", "outro sistema escolar", "otro sistema escolar"),
    ("parent government", "governo controlador", "gobierno controlador"),
    (
        "impact aid",
        "auxílio compensatório federal (impact aid)",
        "ayuda compensatoria federal (impact aid)",
    ),
    ("Indian education", "educação indígena", "educación indígena"),
    (
        "rural education achievement",
        "desempenho da educação rural",
        "desempeño de la educación rural",
    ),
    (
        "child nutrition programs",
        "programas de nutrição infantil",
        "programas de nutrición infantil",
    ),
    ("Child Nutrition Act", "Child Nutrition Act", "Child Nutrition Act"),
    (
        "math and science teaching",
        "ensino de matemática e ciências",
        "enseñanza de matemáticas y ciencias",
    ),
    (
        "drug-free schools",
        "escolas livres de drogas",
        "escuelas libres de drogas",
    ),
    (
        "21st Century Community Learning Centers",
        "21st Century Community Learning Centers",
        "21st Century Community Learning Centers",
    ),
    ("effective instruction", "ensino eficaz", "enseñanza eficaz"),
    (
        "rural and low-income schools",
        "escolas rurais e de baixa renda",
        "escuelas rurales y de baixa renta",
    ),
    (
        "supporting effective instruction",
        "apoio ao ensino eficaz",
        "apoyo a la enseñanza eficaz",
    ),
    (
        "student support and academic enrichment",
        "apoio ao estudante e enriquecimento acadêmico",
        "apoyo al estudiante y enriquecimiento académico",
    ),
    (
        "technology-related supplies and services",
        "insumos e serviços de tecnologia",
        "insumos y servicios de tecnología",
    ),
    (
        "technology-related equipment",
        "equipamentos de tecnologia",
        "equipos de tecnología",
    ),
    (
        "instructional equipment",
        "equipamentos de ensino",
        "equipos de enseñanza",
    ),
    (
        "regional education service agencies",
        "agências regionais de serviços educacionais",
        "agencias regionales de servicios educativos",
    ),
    (
        "land and existing structures",
        "terrenos e estruturas existentes",
        "terrenos y estructuras existentes",
    ),
    (
        "state and local funds",
        "recursos estaduais e locais",
        "fondos estatales y locales",
    ),
    ("federal funds", "recursos federais", "fondos federales"),
    (
        "utilities and energy",
        "serviços públicos e energia",
        "servicios públicos y energía",
    ),
    ("private schools", "escolas privadas", "escuelas privadas"),
    ("charter schools", "escolas charter", "escuelas charter"),
    ("state governments", "governos estaduais", "gobiernos estatales"),
    ("local governments", "governos locais", "gobiernos locales"),
    (
        "regular education programs",
        "programas de educação regular",
        "programas de educación regular",
    ),
    (
        "other education programs",
        "outros programas educacionais",
        "otros programas educativos",
    ),
    ("textbooks", "livros didáticos", "libros de texto"),
    ("construction", "construção", "construcción"),
    ("transportation", "transporte", "transporte"),
    # staff nouns ------------------------------------------------------------
    (
        "prekindergarten teachers",
        "professores de pré-escola",
        "docentes de preescolar",
    ),
    (
        "kindergarten teachers",
        "professores de jardim de infância",
        "docentes de jardín de infancia",
    ),
    (
        "elementary school teachers",
        "professores do ensino primário",
        "docentes de primaria",
    ),
    (
        "secondary school teachers",
        "professores do ensino secundário",
        "docentes de secundaria",
    ),
    (
        "ungraded teachers",
        "professores de turmas sem seriação",
        "docentes de clases sin graduación",
    ),
    ("classroom teachers", "professores em sala de aula", "docentes de aula"),
    ("teachers", "professores", "docentes"),
    (
        "instructional aides or paraprofessionals",
        "auxiliares de ensino ou paraprofissionais",
        "auxiliares de enseñanza o paraprofesionales",
    ),
    (
        "instructional coordinators and supervisors",
        "coordenadores e supervisores pedagógicos",
        "coordinadores y supervisores pedagógicos",
    ),
    (
        "elementary school guidance counselors",
        "orientadores educacionais do ensino primário",
        "orientadores educativos de primaria",
    ),
    (
        "secondary school guidance counselors",
        "orientadores educacionais do ensino secundário",
        "orientadores educativos de secundaria",
    ),
    (
        "other guidance counselors",
        "outros orientadores educacionais",
        "otros orientadores educativos",
    ),
    (
        "guidance counselors",
        "orientadores educacionais",
        "orientadores educativos",
    ),
    ("school counselors", "conselheiros escolares", "consejeros escolares"),
    ("school psychologists", "psicólogos escolares", "psicólogos escolares"),
    (
        "librarians or media specialists",
        "bibliotecários ou especialistas em mídia",
        "bibliotecarios o especialistas en medios",
    ),
    (
        "library or media support staff",
        "pessoal de apoio de biblioteca ou mídia",
        "personal de apoyo de biblioteca o medios",
    ),
    (
        "local education agency administrative support staff",
        "pessoal de apoio administrativo da agência local de educação",
        "personal de apoyo administrativo de la agencia local de educación",
    ),
    (
        "local education agency administrators",
        "administradores da agência local de educação",
        "administradores de la agencia local de educación",
    ),
    (
        "school administrative support staff",
        "pessoal de apoio administrativo da escola",
        "personal de apoyo administrativo de la escuela",
    ),
    (
        "school administrators",
        "administradores escolares",
        "administradores escolares",
    ),
    (
        "school staff",
        "pessoal lotado nas escolas",
        "personal ubicado en las escuelas",
    ),
    (
        "LEA staff",
        "pessoal lotado na agência local de educação",
        "personal ubicado en la agencia local de educación",
    ),
    (
        "other support services staff",
        "outros profissionais de serviços de apoio",
        "otro personal de servicios de apoyo",
    ),
    ("staff", "profissionais", "personal"),
    # articles before a masked proper noun; must precede the bare connectives
    # below, or " for " fires first and strands the "the"
    (" for the ", " para o ", " para el "),
    (" from the ", " do ", " del "),
    (" to the ", " ao ", " al "),
    (" of the ", " do ", " del "),
    # connectives ------------------------------------------------------------
    (
        " through the state",
        " repassad@ pelo estado",
        " transferid@ por el estado",
    ),
    (" for ", " para ", " para "),
    (" from ", " de ", " de "),
    (" to ", " a ", " a "),
    (" on ", " sobre ", " sobre "),
    (" in ", " em ", " en "),
    (
        " not elsewhere classified",
        ", não classificad@ em outra rubrica",
        ", no clasificad@ en otro rubro",
    ),
    ("Total", "Total de", "Total de"),
    ("Other", "Outr@s", "Otr@s"),
    ("other", "outr@s", "otr@s"),
    (
        "Non-elementary-secondary",
        "Não primária-secundária",
        "No primaria-secundaria",
    ),
    ("(dollars)", "", ""),
    ("ARRA", "ARRA", "ARRA"),
]


#: Phrases that must survive the glossary untouched. A self-mapping glossary
#: entry is not enough: it replaces the phrase with itself, and a later generic
#: rule (" to " -> " a ") then fires inside the result, which is how
#: "Project School Emergency Response to Violence" became "... Response a
#: Violence". They are masked out before translation and restored after.
_PROTECTED = (
    "Education Stabilization Fund - Rethink K12 Models",
    "Education Stabilization Fund - Reimagine Workforce Preparation",
    "Project School Emergency Response to Violence",
    "Coronavirus Relief Fund",
    "Individuals with Disabilities Education Act",
    "American Recovery and Reinvestment Act",
    "21st Century Community Learning Centers",
    "Child Nutrition Act",
)


def _translate(label: str, idx: int) -> str:
    """Compose a Portuguese (idx=1) or Spanish (idx=2) label from the glossary."""
    out = label
    masks: dict[str, str] = {}
    for i, phrase in enumerate(_PROTECTED):
        if phrase in out:
            token = f"\x00{i}\x00"
            masks[token] = phrase
            out = out.replace(phrase, token)

    for entry in _GLOSSARY:
        if entry[0] in out:
            out = out.replace(entry[0], entry[idx])

    # gender agreement placeholders, resolved by the surrounding noun
    fem = any(
        w in out.lower()
        for w in ("receita", "despesa", "dívida", "ingresos", "deuda")
    )
    out = out.replace("@", "a" if fem else "o")

    for token, phrase in masks.items():
        out = out.replace(token, phrase)
    return " ".join(out.split()).strip(" ,")


# --------------------------------------------------------------------------
# school_district (agency directory, staff FTE moved out to `staff`)
# --------------------------------------------------------------------------

#: The 27 staff FTE columns of the agency directory, reshaped into `staff`.
#: Tuples are (category code, source column, English, Portuguese, Spanish).
#: Order fixes the dictionary order of `staff_category`.
STAFF_CATEGORIES: list[tuple[str, str, str, str, str]] = [
    (
        "teachers_prek",
        "teachers_prek_fte",
        "Prekindergarten teachers",
        "Professores de pré-escola",
        "Docentes de preescolar",
    ),
    (
        "teachers_kindergarten",
        "teachers_kindergarten_fte",
        "Kindergarten teachers",
        "Professores de jardim de infância",
        "Docentes de jardín de infancia",
    ),
    (
        "teachers_elementary",
        "teachers_elementary_fte",
        "Elementary school teachers",
        "Professores do ensino primário",
        "Docentes de primaria",
    ),
    (
        "teachers_secondary",
        "teachers_secondary_fte",
        "Secondary school teachers",
        "Professores do ensino secundário",
        "Docentes de secundaria",
    ),
    (
        "teachers_ungraded",
        "teachers_ungraded_fte",
        "Ungraded teachers",
        "Professores de turmas sem seriação",
        "Docentes de clases sin graduación",
    ),
    (
        "teachers_total",
        "teachers_total_fte",
        "All teachers",
        "Todos os professores",
        "Todos los docentes",
    ),
    (
        "instructional_aides",
        "instructional_aides_fte",
        "Instructional aides and paraprofessionals",
        "Auxiliares de ensino e paraprofissionais",
        "Auxiliares de enseñanza y paraprofesionales",
    ),
    (
        "coordinators",
        "coordinators_fte",
        "Instructional coordinators and supervisors",
        "Coordenadores e supervisores pedagógicos",
        "Coordinadores y supervisores pedagógicos",
    ),
    (
        "guidance_counselors_elementary",
        "guidance_counselors_elem_fte",
        "Elementary school guidance counselors",
        "Orientadores educacionais do ensino primário",
        "Orientadores educativos de primaria",
    ),
    (
        "guidance_counselors_secondary",
        "guidance_counselors_sec_fte",
        "Secondary school guidance counselors",
        "Orientadores educacionais do ensino secundário",
        "Orientadores educativos de secundaria",
    ),
    (
        "guidance_counselors_other",
        "guidance_counselors_other_fte",
        "Other guidance counselors",
        "Outros orientadores educacionais",
        "Otros orientadores educativos",
    ),
    (
        "guidance_counselors_total",
        "guidance_counselors_total_fte",
        "All guidance counselors",
        "Todos os orientadores educacionais",
        "Todos los orientadores educativos",
    ),
    (
        "school_counselors",
        "school_counselors_fte",
        "School counselors",
        "Conselheiros escolares",
        "Consejeros escolares",
    ),
    (
        "school_psychologists",
        "school_psychologists_fte",
        "School psychologists",
        "Psicólogos escolares",
        "Psicólogos escolares",
    ),
    (
        "librarian_specialists",
        "librarian_specialists_fte",
        "Librarians and media specialists",
        "Bibliotecários e especialistas em mídia",
        "Bibliotecarios y especialistas en medios",
    ),
    (
        "librarian_support_staff",
        "librarian_support_staff_fte",
        "Library and media support staff",
        "Pessoal de apoio de biblioteca e mídia",
        "Personal de apoyo de biblioteca y medios",
    ),
    (
        "lea_administrators",
        "lea_administrators_fte",
        "Local education agency administrators",
        "Administradores da agência local de educação",
        "Administradores de la agencia local de educación",
    ),
    (
        "lea_admin_support_staff",
        "lea_admin_support_staff_fte",
        "Local education agency administrative support staff",
        "Pessoal de apoio administrativo da agência local de educação",
        "Personal de apoyo administrativo de la agencia local de educación",
    ),
    (
        "lea_staff_total",
        "lea_staff_total_fte",
        "All staff based at the local education agency",
        "Todo o pessoal lotado na agência local de educação",
        "Todo el personal ubicado en la agencia local de educación",
    ),
    (
        "school_administrators",
        "school_administrators_fte",
        "School administrators",
        "Administradores escolares",
        "Administradores escolares",
    ),
    (
        "school_admin_support_staff",
        "school_admin_support_staff_fte",
        "School administrative support staff",
        "Pessoal de apoio administrativo da escola",
        "Personal de apoyo administrativo de la escuela",
    ),
    (
        "school_staff_total",
        "school_staff_total_fte",
        "All staff based at schools",
        "Todo o pessoal lotado nas escolas",
        "Todo el personal ubicado en las escuelas",
    ),
    (
        "support_staff_students",
        "support_staff_students_fte",
        "Student support services staff",
        "Profissionais de serviços de apoio ao estudante",
        "Personal de servicios de apoyo al estudiante",
    ),
    (
        "support_staff_students_without_psychologists",
        "support_staff_stu_wo_psych_fte",
        "Student support services staff, excluding psychologists",
        "Profissionais de serviços de apoio ao estudante, exceto psicólogos",
        "Personal de servicios de apoyo al estudiante, excepto psicólogos",
    ),
    (
        "support_staff_other",
        "support_staff_other_fte",
        "Other support services staff",
        "Outros profissionais de serviços de apoio",
        "Otro personal de servicios de apoyo",
    ),
    (
        "other_staff",
        "other_staff_fte",
        "Other staff",
        "Outros profissionais",
        "Otro personal",
    ),
    (
        "staff_total",
        "staff_total_fte",
        "All staff",
        "Todo o pessoal",
        "Todo el personal",
    ),
]

STAFF_SOURCE_COLUMNS = [src for _, src, *_ in STAFF_CATEGORIES]

DISTRICT_COLUMNS: list[Col] = [
    YEAR,
    AGENCY_ID,
    STATE_ID,
    Col(
        "county_id",
        "county_code",
        "STRING",
        "County FIPS code (5 digits) of the agency's location",
        "Código FIPS do condado (5 dígitos) da localização da agência",
        "Código FIPS del condado (5 dígitos) de la ubicación de la agencia",
        directory="br_bd_diretorios_us.county:id_county",
    ),
    Col(
        "agency_id_state",
        "state_leaid",
        "STRING",
        "Local education agency identification number assigned by the state",
        "Código da agência local de educação atribuído pelo estado",
        "Código de la agencia local de educación asignado por el estado",
    ),
    Col(
        "agency_name",
        "lea_name",
        "STRING",
        "Local education agency (school district) name",
        "Nome da agência local de educação (distrito escolar)",
        "Nombre de la agencia local de educación (distrito escolar)",
    ),
    Col(
        "county_name",
        "county_name",
        "STRING",
        "County name",
        "Nome do condado",
        "Nombre del condado",
    ),
    Col(
        "cbsa_id",
        "cbsa",
        "STRING",
        "OMB core-based statistical area code of the vintage in force in the school year",
        "Código OMB da área estatística baseada em núcleo (CBSA) da versão vigente no ano letivo",
        "Código OMB del área estadística basada en núcleo (CBSA) de la versión vigente en el año escolar",
    ),
    Col(
        "cbsa_type",
        "cbsa_type",
        "STRING",
        "Whether the core-based statistical area is metropolitan or micropolitan",
        "Indica se a área estatística baseada em núcleo é metropolitana ou micropolitana",
        "Indica si el área estadística basada en núcleo es metropolitana o micropolitana",
        dictionary=True,
    ),
    Col(
        "csa_id",
        "csa",
        "STRING",
        "OMB combined statistical area code of the vintage in force in the school year",
        "Código OMB da área estatística combinada (CSA) da versão vigente no ano letivo",
        "Código OMB del área estadística combinada (CSA) de la versión vigente en el año escolar",
    ),
    Col(
        "cmsa_id",
        "cmsa",
        "STRING",
        "Consolidated metropolitan statistical area code (discontinued after 2003)",
        "Código da área estatística metropolitana consolidada (descontinuado após 2003)",
        "Código del área estadística metropolitana consolidada (descontinuado después de 2003)",
    ),
    Col(
        "necta_id",
        "necta",
        "STRING",
        "New England city and town area code",
        "Código da área de cidades e municípios da Nova Inglaterra",
        "Código del área de ciudades y municipios de Nueva Inglaterra",
    ),
    Col(
        "congressional_district_id",
        "congress_district_id",
        "STRING",
        "State and congressional district identification number",
        "Código do estado e do distrito congressional",
        "Código del estado y del distrito congresional",
    ),
    Col(
        "state_leg_district_lower",
        "state_leg_district_lower",
        "STRING",
        "State legislative district, lower chamber",
        "Distrito legislativo estadual, câmara baixa",
        "Distrito legislativo estatal, cámara baja",
    ),
    Col(
        "state_leg_district_upper",
        "state_leg_district_upper",
        "STRING",
        "State legislative district, upper chamber",
        "Distrito legislativo estadual, câmara alta",
        "Distrito legislativo estatal, cámara alta",
    ),
    Col(
        "supervisory_union_number",
        "supervisory_union_number",
        "STRING",
        "Supervisory union identification number, where the agency belongs to one",
        "Código da união supervisora, quando a agência integra uma",
        "Código de la unión supervisora, cuando la agencia integra una",
    ),
    *_geo_cols("location"),
    Col(
        "zip4_location",
        "zip4_location",
        "STRING",
        "Four-digit ZIP code extension of the location address",
        "Extensão de quatro dígitos do CEP (ZIP+4) do endereço de localização",
        "Extensión de cuatro dígitos del código postal (ZIP+4) de la dirección de ubicación",
    ),
    *_geo_cols("mailing"),
    Col(
        "zip4_mailing",
        "zip4_mailing",
        "STRING",
        "Four-digit ZIP code extension of the mailing address",
        "Extensão de quatro dígitos do CEP (ZIP+4) do endereço de correspondência",
        "Extensión de cuatro dígitos del código postal (ZIP+4) de la dirección postal",
    ),
    Col(
        "phone",
        "phone",
        "STRING",
        "Agency telephone number",
        "Telefone da agência",
        "Teléfono de la agencia",
    ),
    Col(
        "latitude",
        "latitude",
        "FLOAT64",
        "Latitude of the agency in decimal degrees",
        "Latitude da agência em graus decimais",
        "Latitud de la agencia en grados decimales",
        unit="",
    ),
    Col(
        "longitude",
        "longitude",
        "FLOAT64",
        "Longitude of the agency in decimal degrees",
        "Longitude da agência em graus decimais",
        "Longitud de la agencia en grados decimales",
        unit="",
    ),
    Col(
        "urban_centric_locale",
        "urban_centric_locale",
        "STRING",
        "Degree of urbanization (urban-centric locale) of the agency location",
        "Grau de urbanização (localidade urbano-cêntrica) da localização da agência",
        "Grado de urbanización (localidad urbano-céntrica) de la ubicación de la agencia",
        dictionary=True,
    ),
    Col(
        "agency_type",
        "agency_type",
        "STRING",
        "Type of local education agency",
        "Tipo de agência local de educação",
        "Tipo de agencia local de educación",
        dictionary=True,
    ),
    Col(
        "agency_level",
        "agency_level",
        "STRING",
        "Grade span served by the agency",
        "Faixa de séries atendida pela agência",
        "Rango de grados atendido por la agencia",
        dictionary=True,
    ),
    Col(
        "agency_charter_indicator",
        "agency_charter_indicator",
        "STRING",
        "Whether the agency operates charter schools",
        "Indica se a agência opera escolas charter",
        "Indica si la agencia opera escuelas charter",
        dictionary=True,
    ),
    Col(
        "boundary_change_indicator",
        "boundary_change_indicator",
        "STRING",
        "Whether the agency's boundary changed since the previous school year",
        "Indica se o limite territorial da agência mudou em relação ao ano letivo anterior",
        "Indica si el límite territorial de la agencia cambió respecto al año escolar anterior",
        dictionary=True,
    ),
    Col(
        "bureau_indian_education",
        "bureau_indian_education",
        "STRING",
        "Whether the agency is funded or operated by the Bureau of Indian Education",
        "Indica se a agência é financiada ou operada pelo Bureau of Indian Education",
        "Indica si la agencia es financiada u operada por el Bureau of Indian Education",
        dictionary=True,
    ),
    Col(
        "lowest_grade_offered",
        "lowest_grade_offered",
        "STRING",
        "Lowest grade offered by the agency",
        "Menor série oferecida pela agência",
        "Grado más bajo ofrecido por la agencia",
        dictionary=True,
    ),
    Col(
        "highest_grade_offered",
        "highest_grade_offered",
        "STRING",
        "Highest grade offered by the agency",
        "Maior série oferecida pela agência",
        "Grado más alto ofrecido por la agencia",
        dictionary=True,
    ),
    Col(
        "number_of_schools",
        "number_of_schools",
        "INT64",
        "Schools operated by the agency",
        "Escolas operadas pela agência",
        "Escuelas operadas por la agencia",
        unit="",
    ),
    Col(
        "enrollment",
        "enrollment",
        "INT64",
        "Total student enrollment reported in the agency directory",
        "Matrícula total de estudantes informada no diretório de agências",
        "Matrícula total de estudiantes informada en el directorio de agencias",
        unit="person",
    ),
    Col(
        "spec_ed_students",
        "spec_ed_students",
        "INT64",
        "Students served under the Individuals with Disabilities Education Act",
        "Estudantes atendidos sob o Individuals with Disabilities Education Act",
        "Estudiantes atendidos bajo el Individuals with Disabilities Education Act",
        unit="person",
    ),
    Col(
        "english_language_learners",
        "english_language_learners",
        "INT64",
        "Students identified as English language learners",
        "Estudantes identificados como aprendizes de inglês",
        "Estudiantes identificados como aprendices de inglés",
        unit="person",
    ),
    Col(
        "migrant_students",
        "migrant_students",
        "INT64",
        "Students identified as migrant",
        "Estudantes identificados como migrantes",
        "Estudiantes identificados como migrantes",
        unit="person",
    ),
]


# --------------------------------------------------------------------------
# school_enrollment (long) and staff (long)
# --------------------------------------------------------------------------

ENROLLMENT_COLUMNS: list[Col] = [
    YEAR,
    SCHOOL_ID,
    AGENCY_ID,
    STATE_ID,
    Col(
        "grade",
        "grade",
        "STRING",
        "Grade the enrollment count refers to",
        "Série a que se refere a contagem de matrículas",
        "Grado al que se refiere el conteo de matrículas",
        dictionary=True,
        observations=(
            "-1 is prekindergarten and is a real category, not a missing code. "
            "99 is the total across grades."
        ),
    ),
    Col(
        "race",
        "race",
        "STRING",
        "Race or ethnicity the enrollment count refers to",
        "Raça ou etnia a que se refere a contagem de matrículas",
        "Raza o etnia a la que se refiere el conteo de matrículas",
        dictionary=True,
        observations=(
            "99 is the total across race and ethnicity groups. The CCD moved from five "
            "race categories to the current seven in 2008-09; category 7 (two or more "
            "races) does not appear before then."
        ),
    ),
    Col(
        "sex",
        "sex",
        "STRING",
        "Sex the enrollment count refers to",
        "Sexo a que se refere a contagem de matrículas",
        "Sexo al que se refiere el conteo de matrículas",
        dictionary=True,
        observations="99 is the total across sexes.",
    ),
    Col(
        "enrollment",
        "enrollment",
        "INT64",
        "Students enrolled in the school in the given grade, race and sex cell",
        "Estudantes matriculados na escola na célula de série, raça e sexo indicada",
        "Estudiantes matriculados en la escuela en la celda de grado, raza y sexo indicada",
        unit="person",
    ),
]

STAFF_COLUMNS: list[Col] = [
    YEAR,
    AGENCY_ID,
    STATE_ID,
    Col(
        "staff_category",
        None,
        "STRING",
        "Staff category the full-time equivalent count refers to",
        "Categoria de pessoal a que se refere a contagem em equivalente de tempo integral",
        "Categoría de personal a la que se refiere el conteo en equivalente de tiempo completo",
        dictionary=True,
        observations=(
            "Reshaped from the wide staff columns of the CCD agency universe file. "
            "Totals (teachers_total, guidance_counselors_total, lea_staff_total, "
            "school_staff_total, staff_total) are reported alongside their components "
            "and must not be summed together with them."
        ),
    ),
    Col(
        "staff_fte",
        None,
        "FLOAT64",
        "Full-time equivalent staff in the category",
        "Pessoal em equivalente de tempo integral na categoria",
        "Personal en equivalente de tiempo completo en la categoría",
        unit="",
    ),
]


# --------------------------------------------------------------------------
# district_finance (F-33)
# --------------------------------------------------------------------------

#: Source labels that contradict their own column name. Urban labels
#: `debt_shortterm_outstand_beg_FY` "at end of fiscal year" and
#: `debt_shortterm_outstand_end_FY` "at beginning of" -- the two are swapped
#: relative to the names, and relative to their long-term counterparts, which
#: are labelled correctly. The names are followed and the discrepancy is
#: recorded on the column so a user can check against the F-33 form.
FINANCE_LABEL_FIXUPS = {
    "debt_shortterm_outstand_beg_FY": "Short-term debt outstanding at beginning of fiscal year",
    "debt_shortterm_outstand_end_FY": "Short-term debt outstanding at end of fiscal year",
}

_SWAPPED_LABEL_NOTE = (
    "The source variable list labels this column with the opposite end of the "
    "fiscal year, contradicting both the column name and the long-term debt "
    "columns. The name is followed here; check against the F-33 form before "
    "relying on it.",
    "A lista de variáveis da fonte rotula esta coluna com o extremo oposto do "
    "exercício fiscal, contradizendo o nome da coluna e as colunas de dívida de "
    "longo prazo. O nome foi seguido aqui; confira no formulário F-33 antes de "
    "usá-la.",
    "La lista de variables de la fuente etiqueta esta columna con el extremo "
    "opuesto del ejercicio fiscal, contradiciendo el nombre de la columna y las "
    "columnas de deuda de largo plazo. Aquí se siguió el nombre; verifique en el "
    "formulario F-33 antes de usarla.",
)

#: Columns of the finance file that are not money amounts.
FINANCE_NON_MONEY = {
    "leaid",
    "censusid",
    "year",
    "fips",
    "enrollment_fall_responsible",
    "enrollment_fall_school",
}


def _finance_money_columns(
    header: list[str], labels: dict[str, str]
) -> list[Col]:
    """Build the ~157 USD columns of the F-33 table from the source header."""
    cols: list[Col] = []
    for src in header:
        if src in FINANCE_NON_MONEY:
            continue
        label = (
            FINANCE_LABEL_FIXUPS.get(src)
            or labels.get(src)
            or src.replace("_", " ").capitalize()
        )
        note = (
            _SWAPPED_LABEL_NOTE
            if src in FINANCE_LABEL_FIXUPS
            else ("", "", "")
        )
        cols.append(
            Col(
                src.lower(),
                src,
                "FLOAT64",
                label,
                _translate(label, 1),
                _translate(label, 2),
                unit="usd",
                observations=note[0],
                observations_pt=note[1],
                observations_es=note[2],
            )
        )
    return cols


FINANCE_HEAD_COLUMNS: list[Col] = [
    YEAR,
    AGENCY_ID,
    STATE_ID,
    Col(
        "census_id",
        "censusid",
        "STRING",
        "US Census Bureau 14-digit government identification number",
        "Código de 14 dígitos do governo atribuído pelo US Census Bureau",
        "Código de 14 dígitos del gobierno asignado por el US Census Bureau",
    ),
    Col(
        "enrollment_fall_responsible",
        "enrollment_fall_responsible",
        "INT64",
        "Students for which the reporting local education agency is financially responsible",
        "Estudantes pelos quais a agência local de educação declarante é financeiramente responsável",
        "Estudiantes por los cuales la agencia local de educación declarante es financieramente responsable",
        unit="person",
    ),
    Col(
        "enrollment_fall_school",
        "enrollment_fall_school",
        "INT64",
        "Students enrolled in schools operated by the reporting local education agency",
        "Estudantes matriculados em escolas operadas pela agência local de educação declarante",
        "Estudiantes matriculados en escuelas operadas por la agencia local de educación declarante",
        unit="person",
    ),
]


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------

DICIONARIO_COLUMNS: list[Col] = [
    Col(
        "id_tabela",
        None,
        "STRING",
        "Slug of the us_ed_nces_ccd table the dictionary entry describes",
        "Slug da tabela do conjunto us_ed_nces_ccd que a entrada do dicionário descreve",
        "Slug de la tabla del conjunto us_ed_nces_ccd que describe la entrada del diccionario",
    ),
    Col(
        "nome_coluna",
        None,
        "STRING",
        "Name of the column the dictionary entry describes",
        "Nome da coluna que a entrada do dicionário descreve",
        "Nombre de la columna que describe la entrada del diccionario",
    ),
    Col(
        "chave",
        None,
        "STRING",
        "Code stored in the column",
        "Código armazenado na coluna",
        "Código almacenado en la columna",
    ),
    Col(
        "cobertura_temporal",
        None,
        "STRING",
        "Temporal coverage of the dictionary entry",
        "Cobertura temporal da entrada do dicionário",
        "Cobertura temporal de la entrada del diccionario",
    ),
    Col(
        "valor",
        None,
        "STRING",
        "Label the code stands for",
        "Rótulo que o código representa",
        "Etiqueta que representa el código",
    ),
]


# --------------------------------------------------------------------------
# Tables
# --------------------------------------------------------------------------

_SCHOOL_DESC_EN = (
    "Annual directory of every public elementary and secondary school in the United States, "
    "from the NCES Common Core of Data school universe survey. One row per NCES school "
    "identifier and school year, with location, geography codes, school type and level, "
    "charter, magnet and virtual indicators, Title I status, grade span, full-time equivalent "
    "teachers, total enrollment and free and reduced-price lunch eligibility counts."
)
_SCHOOL_DESC_PT = (
    "Diretório anual de todas as escolas públicas de ensino primário e secundário dos Estados "
    "Unidos, a partir do censo de escolas do NCES Common Core of Data. Uma linha por "
    "identificador NCES de escola e ano letivo, com localização, códigos geográficos, tipo e "
    "nível de ensino, indicadores de charter, magnet e ensino virtual, situação quanto ao Title I, "
    "faixa de séries, professores em equivalente de tempo integral, matrícula total e contagens "
    "de elegibilidade a almoço gratuito ou a preço reduzido."
)
_SCHOOL_DESC_ES = (
    "Directorio anual de todas las escuelas públicas de enseñanza primaria y secundaria de "
    "Estados Unidos, a partir del censo de escuelas del NCES Common Core of Data. Una fila por "
    "identificador NCES de escuela y año escolar, con ubicación, códigos geográficos, tipo y "
    "nivel de enseñanza, indicadores de charter, magnet y enseñanza virtual, situación respecto "
    "al Title I, rango de grados, docentes en equivalente de tiempo completo, matrícula total y "
    "conteos de elegibilidad para almuerzo gratuito o a precio reducido."
)

TABLE_SCHOOL = Table(
    "school",
    "School",
    "Escola",
    "Escuela",
    _SCHOOL_DESC_EN,
    _SCHOOL_DESC_PT,
    _SCHOOL_DESC_ES,
    SCHOOL_COLUMNS,
    cluster=["state_id", "school_id"],
    entities=["year", "school"],
    entity_columns={"year": "year", "school_id": "school"},
)

TABLE_DISTRICT = Table(
    "school_district",
    "School district",
    "Distrito escolar",
    "Distrito escolar",
    "Annual directory of every public school district (local education agency) in the United "
    "States, from the NCES Common Core of Data agency universe survey. One row per NCES LEAID "
    "and school year, with location, geography codes, agency type and level, grade span, number "
    "of schools operated, total enrollment, and counts of special education, English learner and "
    "migrant students. Staff counts are published separately in the staff table.",
    "Diretório anual de todos os distritos escolares públicos (agências locais de educação) dos "
    "Estados Unidos, a partir do censo de agências do NCES Common Core of Data. Uma linha por "
    "LEAID do NCES e ano letivo, com localização, códigos geográficos, tipo e nível da agência, "
    "faixa de séries, número de escolas operadas, matrícula total e contagens de estudantes de "
    "educação especial, aprendizes de inglês e migrantes. As contagens de pessoal são publicadas "
    "separadamente na tabela staff.",
    "Directorio anual de todos los distritos escolares públicos (agencias locales de educación) "
    "de Estados Unidos, a partir del censo de agencias del NCES Common Core of Data. Una fila por "
    "LEAID del NCES y año escolar, con ubicación, códigos geográficos, tipo y nivel de la agencia, "
    "rango de grados, número de escuelas operadas, matrícula total y conteos de estudiantes de "
    "educación especial, aprendices de inglés y migrantes. Los conteos de personal se publican "
    "por separado en la tabla staff.",
    DISTRICT_COLUMNS,
    cluster=["state_id", "agency_id"],
    entities=["year", "school_district"],
    entity_columns={"year": "year", "agency_id": "school_district"},
)

TABLE_ENROLLMENT = Table(
    "school_enrollment",
    "School enrollment",
    "Matrícula escolar",
    "Matrícula escolar",
    "Public school enrollment counts in long format, one row per school, school year, grade, "
    "race or ethnicity and sex, from the NCES Common Core of Data school membership file. The "
    "source ships roughly 200 wide membership columns whose composition changes across years; "
    "they are reshaped here into a single count column. Marginal totals are retained and carry "
    "the code 99 on grade, race and sex, so the cells and their totals must not be summed "
    "together. Race and sex detail begins in 1998; earlier years carry totals only.",
    "Contagens de matrícula em escolas públicas em formato longo, uma linha por escola, ano "
    "letivo, série, raça ou etnia e sexo, a partir do arquivo de matrículas do NCES Common Core "
    "of Data. A fonte publica cerca de 200 colunas largas de matrícula cuja composição muda ao "
    "longo dos anos; elas são reorganizadas aqui em uma única coluna de contagem. Os totais "
    "marginais são mantidos e recebem o código 99 em série, raça e sexo, de modo que as células e "
    "seus totais não devem ser somados em conjunto. O detalhamento por raça e sexo começa em "
    "1998; os anos anteriores trazem apenas totais.",
    "Conteos de matrícula en escuelas públicas en formato largo, una fila por escuela, año "
    "escolar, grado, raza o etnia y sexo, a partir del archivo de matrículas del NCES Common Core "
    "of Data. La fuente publica alrededor de 200 columnas anchas de matrícula cuya composición "
    "cambia a lo largo de los años; aquí se reorganizan en una única columna de conteo. Los "
    "totales marginales se conservan y llevan el código 99 en grado, raza y sexo, por lo que las "
    "celdas y sus totales no deben sumarse en conjunto. El detalle por raza y sexo comienza en "
    "1998; los años anteriores traen solo totales.",
    ENROLLMENT_COLUMNS,
    cluster=["state_id", "school_id"],
    entities=["year", "school", "race", "sex", "grade"],
    entity_columns={
        "year": "year",
        "school_id": "school",
        "race": "race",
        "sex": "sex",
        "grade": "grade",
    },
)

TABLE_STAFF = Table(
    "staff",
    "Staff",
    "Pessoal",
    "Personal",
    "Full-time equivalent staff of public school districts in long format, one row per school "
    "district, school year and staff category, from the NCES Common Core of Data agency universe "
    "survey. Categories cover teachers by grade span, instructional aides, coordinators, guidance "
    "counselors, psychologists, librarians and administrative and support staff. Category totals "
    "are reported alongside their components and must not be summed together with them. Rows "
    "with no reported count are omitted.",
    "Pessoal em equivalente de tempo integral dos distritos escolares públicos em formato longo, "
    "uma linha por distrito escolar, ano letivo e categoria de pessoal, a partir do censo de "
    "agências do NCES Common Core of Data. As categorias cobrem professores por faixa de séries, "
    "auxiliares de ensino, coordenadores, orientadores educacionais, psicólogos, bibliotecários e "
    "pessoal administrativo e de apoio. Os totais por categoria são informados junto com seus "
    "componentes e não devem ser somados em conjunto com eles. Linhas sem contagem informada são "
    "omitidas.",
    "Personal en equivalente de tiempo completo de los distritos escolares públicos en formato "
    "largo, una fila por distrito escolar, año escolar y categoría de personal, a partir del censo "
    "de agencias del NCES Common Core of Data. Las categorías cubren docentes por rango de grados, "
    "auxiliares de enseñanza, coordinadores, orientadores educativos, psicólogos, bibliotecarios y "
    "personal administrativo y de apoyo. Los totales por categoría se informan junto con sus "
    "componentes y no deben sumarse en conjunto con ellos. Se omiten las filas sin conteo "
    "informado.",
    STAFF_COLUMNS,
    cluster=["state_id", "agency_id"],
    entities=["year", "school_district"],
    entity_columns={"year": "year", "agency_id": "school_district"},
)

TABLE_DICIONARIO = Table(
    "dicionario",
    "Dictionary",
    "Dicionário",
    "Diccionario",
    "Dictionary of the codes stored in the coded columns of the us_ed_nces_ccd tables, with the "
    "label each code stands for.",
    "Dicionário dos códigos armazenados nas colunas codificadas das tabelas do conjunto "
    "us_ed_nces_ccd, com o rótulo que cada código representa.",
    "Diccionario de los códigos almacenados en las columnas codificadas de las tablas del "
    "conjunto us_ed_nces_ccd, con la etiqueta que representa cada código.",
    DICIONARIO_COLUMNS,
    partition="",
    entities=[],
)


def finance_table(header: list[str], labels: dict[str, str]) -> Table:
    return Table(
        "district_finance",
        "School district finance",
        "Finanças do distrito escolar",
        "Finanzas del distrito escolar",
        "Revenues, expenditures, salaries, employee benefits, debt and assets of public school "
        "districts, from the Census Bureau and NCES Annual Survey of School System Finances "
        "(form F-33). One row per school district and fiscal year, in nominal US dollars. "
        "Categories that a given year's form did not collect are null; the CARES Act, CRRSA and "
        "ARP relief items exist only from 2019 onward.",
        "Receitas, despesas, salários, benefícios a empregados, dívida e ativos dos distritos "
        "escolares públicos, a partir do Annual Survey of School System Finances (formulário "
        "F-33) do Census Bureau e do NCES. Uma linha por distrito escolar e exercício fiscal, em "
        "dólares nominais. Categorias não coletadas pelo formulário de um dado ano são nulas; os "
        "itens de auxílio do CARES Act, CRRSA e ARP existem apenas a partir de 2019.",
        "Ingresos, gastos, salarios, prestaciones a empleados, deuda y activos de los distritos "
        "escolares públicos, a partir de la Annual Survey of School System Finances (formulario "
        "F-33) del Census Bureau y del NCES. Una fila por distrito escolar y ejercicio fiscal, en "
        "dólares nominales. Las categorías no recolectadas por el formulario de un año dado son "
        "nulas; los ítems de ayuda del CARES Act, CRRSA y ARP existen solo a partir de 2019.",
        FINANCE_HEAD_COLUMNS + _finance_money_columns(header, labels),
        cluster=["state_id", "agency_id"],
        year_min=1989,
        year_max=2020,
        entities=["year", "school_district"],
        entity_columns={"year": "year", "agency_id": "school_district"},
    )


#: Tables that do not need the source header to be built.
STATIC_TABLES = [TABLE_SCHOOL, TABLE_DISTRICT, TABLE_ENROLLMENT, TABLE_STAFF]


def _unescape(text: str) -> str:
    """Urban's labels carry HTML entities (&ndash;, &mdash;, &amp;)."""
    import html

    return html.unescape(text).replace("-", "-").replace("—", " - ").strip()


# --------------------------------------------------------------------------
# Portuguese and Spanish renderings of the ten distinct column notes
# --------------------------------------------------------------------------
#
# Attached after construction so the note text stays next to its English
# original above rather than being repeated three times inline.

_OBSERVATION_TRANSLATIONS: dict[str, tuple[str, str]] = {
    SCHOOL_ID.observations: (
        "Preenchido com zeros à esquerda até 12 caracteres na carga. O único valor de 11 "
        "caracteres da fonte (25000031636) é corrigido para 250000301636.",
        "Rellenado con ceros a la izquierda hasta 12 caracteres en la carga. El único valor "
        "de 11 caracteres de la fuente (25000031636) se corrige a 250000301636.",
    ),
    AGENCY_ID.observations: (
        "Preenchido com zeros à esquerda até 7 caracteres na carga; a fonte remove os zeros "
        "à esquerda nos extratos de matrícula.",
        "Rellenado con ceros a la izquierda hasta 7 caracteres en la carga; la fuente elimina "
        "los ceros a la izquierda en los extractos de matrícula.",
    ),
    STATE_ID.observations: (
        "Preenchido com zeros à esquerda até 2 caracteres. O CCD amplia a lista de códigos "
        "FIPS estaduais com códigos para jurisdições que não são estados (58 escolas do "
        "Departamento de Defesa no exterior, 59 Bureau of Indian Education, 61 e 63 áreas do "
        "Departamento de Defesa no país e no exterior), de modo que esta coluna não é uma "
        "chave estrangeira estrita para o diretório de estados.",
        "Rellenado con ceros a la izquierda hasta 2 caracteres. El CCD amplía la lista de "
        "códigos FIPS estatales con códigos para jurisdicciones que no son estados (58 "
        "escuelas del Departamento de Defensa en el exterior, 59 Bureau of Indian Education, "
        "61 y 63 áreas del Departamento de Defensa en el país y en el exterior), por lo que "
        "esta columna no es una clave foránea estricta hacia el directorio de estados.",
    ),
    "Not the 12-character NCES id; see school_id for that.": (
        "Não é o código NCES de 12 caracteres; para esse, veja school_id.",
        "No es el código NCES de 12 caracteres; para ese, véase school_id.",
    ),
    "CBSA delineations are revised periodically; codes are not comparable across all years.": (
        "As delimitações de CBSA são revisadas periodicamente; os códigos não são comparáveis "
        "ao longo de todos os anos.",
        "Las delimitaciones de CBSA se revisan periódicamente; los códigos no son comparables "
        "a lo largo de todos los años.",
    ),
    (
        "Nine categories before 2005 and twelve from 2005 onward; both code sets appear in "
        "the dictionary."
    ): (
        "Nove categorias antes de 2005 e doze a partir de 2005; ambos os conjuntos de códigos "
        "constam do dicionário.",
        "Nueve categorías antes de 2005 y doce a partir de 2005; ambos conjuntos de códigos "
        "constan en el diccionario.",
    ),
    (
        "-1 is prekindergarten and is a real category, not a missing code. 99 is the total "
        "across grades."
    ): (
        "-1 é pré-escola e constitui uma categoria real, não um código de ausência. 99 é o "
        "total entre as séries.",
        "-1 es preescolar y constituye una categoría real, no un código de ausencia. 99 es el "
        "total entre los grados.",
    ),
    (
        "99 is the total across race and ethnicity groups. The CCD moved from five race "
        "categories to the current seven in 2008-09; category 7 (two or more races) does not "
        "appear before then."
    ): (
        "99 é o total entre os grupos de raça e etnia. O CCD passou de cinco categorias de "
        "raça para as sete atuais em 2008-09; a categoria 7 (duas ou mais raças) não aparece "
        "antes disso.",
        "99 es el total entre los grupos de raza y etnia. El CCD pasó de cinco categorías de "
        "raza a las siete actuales en 2008-09; la categoría 7 (dos o más razas) no aparece "
        "antes de eso.",
    ),
    "99 is the total across sexes.": (
        "99 é o total entre os sexos.",
        "99 es el total entre los sexos.",
    ),
    (
        "Reshaped from the wide staff columns of the CCD agency universe file. Totals "
        "(teachers_total, guidance_counselors_total, lea_staff_total, school_staff_total, "
        "staff_total) are reported alongside their components and must not be summed together "
        "with them."
    ): (
        "Reorganizado a partir das colunas largas de pessoal do censo de agências do CCD. Os "
        "totais (teachers_total, guidance_counselors_total, lea_staff_total, "
        "school_staff_total, staff_total) são informados junto com seus componentes e não "
        "devem ser somados em conjunto com eles.",
        "Reorganizado a partir de las columnas anchas de personal del censo de agencias del "
        "CCD. Los totales (teachers_total, guidance_counselors_total, lea_staff_total, "
        "school_staff_total, staff_total) se informan junto con sus componentes y no deben "
        "sumarse en conjunto con ellos.",
    ),
}


def apply_observation_translations(columns: list[Col]) -> None:
    """Fill observations_pt / observations_es from the table above."""
    for col in columns:
        if not col.observations or col.observations_pt:
            continue
        pair = _OBSERVATION_TRANSLATIONS.get(col.observations)
        if pair is None:
            raise KeyError(f"no translation for note on column {col.name!r}")
        col.observations_pt, col.observations_es = pair


for _table in (TABLE_SCHOOL, TABLE_DISTRICT, TABLE_ENROLLMENT, TABLE_STAFF):
    apply_observation_translations(_table.columns)


# --------------------------------------------------------------------------
# Translation validation
# --------------------------------------------------------------------------
#
# The glossary composes; it does not understand. A phrase it has no entry for
# passes through untouched and produces a half-English label that reads
# plausibly enough to survive a skim ("Receita estadual para educação especial
# programs"). This check fails the build instead.

#: Words that legitimately read the same in English and in Portuguese or
#: Spanish. Acronyms and any token containing a digit are exempted
#: automatically, so this list only carries genuine cognates and loanwords.
_COGNATES = frozenset(
    [
        "capital",
        "central",
        "charter",
        "congressional",
        "federal",
        "fiscal",
        "formula",
        "general",
        "grade",
        "impact",
        "index",
        "instituto",
        "latitude",
        "local",
        "longitude",
        "magnet",
        "national",
        "percentil",
        "regular",
        "royalties",
        "slug",
        "social",
        "temporal",
        "title",
        "total",
        "us_ed_nces_ccd",
        "virtual",
    ]
)

#: Proper nouns kept in English on purpose. Checked as whole phrases before the
#: word scan, so the words inside them are not flagged.
_PROPER_NOUNS = (
    "Individuals with Disabilities Education Act",
    "Child Nutrition Act",
    "American Recovery and Reinvestment Act",
    "National School Lunch Act",
    "National School Lunch Program",
    "Bureau of Indian Education",
    "Center on Education Data and Policy",
    "Common Core of Data",
    "Education Data Portal",
    "Urban Institute",
    "US Census Bureau",
    "Census Bureau",
    "Title I",
    "Indian Education",
    "CARES Act",
    "CRRSA Act",
    "ARP Act",
    "21st Century Community Learning Centers",
    "impact aid",
    # Loanwords kept verbatim in the translations.
    "charter school",
    "magnet school",
    "ZIP code",
    # Federal programme names, kept in English.
    "Education Stabilization Fund - Rethink K12 Models",
    "Education Stabilization Fund - Reimagine Workforce Preparation",
    "Project School Emergency Response to Violence",
    "Coronavirus Relief Fund",
)


def _acronyms(text: str) -> set[str]:
    """Tokens the English label writes in capitals -- NCES, FIPS, LEAID, OMB."""
    return {
        w.strip(".,;:()-").lower()
        for w in text.split()
        if w.strip(".,;:()-").isupper() and len(w.strip(".,;:()-")) > 1
    }


def _words(text: str) -> set[str]:
    stripped = text
    for phrase in _PROPER_NOUNS:
        stripped = stripped.replace(phrase, " ")
    out = set()
    for raw in stripped.split():
        w = raw.strip(".,;:()-").lower()
        if len(w) > 2 and not any(ch.isdigit() for ch in w):
            out.add(w)
    return out


def untranslated_words(english: str, translated: str) -> list[str]:
    """Words the translation shares with the English original, minus cognates.

    A denylist of English words was tried first and kept missing new ones
    ("practices", "programs", "public", "all", "lunches"), each of which shipped
    a half-translated label that read plausibly. Any word common to both strings
    is suspect unless it is an acronym, carries a digit, sits inside a proper
    noun, or is a listed cognate.
    """
    shared = _words(english) & _words(translated)
    exempt = _COGNATES | _acronyms(english)
    return sorted(w for w in shared if w not in exempt)


#: Words that may legitimately precede a trailing "serviços de apoio".
_HEAD_PREPOSITIONS = frozenset(
    [
        "de",
        "com",
        "para",
        "à",
        "ao",
        "e",
        "os",
        "dos",
        "a",
        "la",
        "las",
        "los",
        "del",
        "em",
        "en",
        "y",
    ]
)

#: (tail, language) pairs whose head must be a preposition.
_SUPPORT_TAILS = (("serviços de apoio", "pt"), ("servicios de apoyo", "es"))


def _stranded_head(text: str) -> str | None:
    """A noun left dangling before a trailing support-services phrase.

    The glossary composes noun phrases by substitution, which in English word
    order yields "para transporte escolar serviços de apoio" -- every word
    translated, and still wrong. No residue check sees this; the shape does.
    """
    for tail, _lang in _SUPPORT_TAILS:
        if text.endswith(tail):
            head = text[: -len(tail)].split()
            if head and head[-1].lower() not in _HEAD_PREPOSITIONS:
                return head[-1]
    return None


def assert_translated(columns: list[Col], table: str) -> None:
    """Raise if any description is not fully or not idiomatically translated."""
    problems: list[str] = []
    for col in columns:
        for lang, text in (("pt", col.desc_pt), ("es", col.desc_es)):
            leaks = untranslated_words(col.desc_en, text)
            if leaks:
                problems.append(
                    f"{table}.{col.name} [{lang}] {text!r} -> {leaks}"
                )
            stranded = _stranded_head(text)
            if stranded:
                problems.append(
                    f"{table}.{col.name} [{lang}] {text!r} -> "
                    f"stranded head noun {stranded!r} before support services"
                )
    if problems:
        raise ValueError(
            f"{len(problems)} bad description(s):\n  " + "\n  ".join(problems)
        )
