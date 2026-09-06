"""Generate the us_census_lodes architecture CSVs.

The architecture CSVs under ``architecture/`` are the schema source of truth for
the cleaning code, the dbt models and the backend column registration. This
script writes them from the column maps in
``pipelines/datasets/us_census_lodes/constants.py`` so the four artefacts cannot
drift apart.

    uv run python models/us_census_lodes/code/gen_architecture.py

Descriptions are written in Portuguese, English and Spanish. Per the house style
a column description never ends with a period, and always starts with a capital.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_lodes.constants import (
    ARCHITECTURE_DIR,
    RAC_COUNTS,
    WAC_COUNTS,
    XWALK_COLUMNS,
)

FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

DIR_STATE = "diretorios_us.state:id_state"
DIR_COUNTY = "diretorios_us.county:id_county"
DIR_TRACT = "diretorios_us.census_tract_2020:id_census_tract"
DIR_YEAR = "diretorios_data_tempo.ano:ano"


def row(
    name,
    btype,
    pt,
    en,
    es,
    *,
    original="",
    unit="",
    directory="",
    dictionary="no",
    observations="",
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": btype,
        "description": pt,
        "description_en": en,
        "description_es": es,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": observations,
        "original_name": original,
    }


# --------------------------------------------------------------------------
# Trilingual text for every count column, keyed by destination name.
# The phrasing mirrors LODESTechDoc8.4 rather than paraphrasing it.
# --------------------------------------------------------------------------
NAICS_SECTORS = {
    "11": (
        "Agricultura, silvicultura, pesca e caça",
        "Agriculture, Forestry, Fishing and Hunting",
        "Agricultura, silvicultura, pesca y caza",
    ),
    "21": (
        "Indústria extrativa e extração de petróleo e gás",
        "Mining, Quarrying, and Oil and Gas Extraction",
        "Minería, canteras y extracción de petróleo y gas",
    ),
    "22": ("Utilidades públicas", "Utilities", "Servicios públicos"),
    "23": ("Construção", "Construction", "Construcción"),
    "31_33": ("Indústria de transformação", "Manufacturing", "Manufactura"),
    "42": ("Comércio atacadista", "Wholesale Trade", "Comercio al por mayor"),
    "44_45": ("Comércio varejista", "Retail Trade", "Comercio al por menor"),
    "48_49": (
        "Transporte e armazenagem",
        "Transportation and Warehousing",
        "Transporte y almacenamiento",
    ),
    "51": ("Informação", "Information", "Información"),
    "52": (
        "Atividades financeiras e de seguros",
        "Finance and Insurance",
        "Finanzas y seguros",
    ),
    "53": (
        "Atividades imobiliárias e de aluguel",
        "Real Estate and Rental and Leasing",
        "Actividades inmobiliarias y de alquiler",
    ),
    "54": (
        "Serviços profissionais, científicos e técnicos",
        "Professional, Scientific, and Technical Services",
        "Servicios profesionales, científicos y técnicos",
    ),
    "55": (
        "Gestão de empresas e empreendimentos",
        "Management of Companies and Enterprises",
        "Gestión de empresas y sociedades",
    ),
    "56": (
        "Serviços administrativos, de apoio e de gestão de resíduos",
        "Administrative and Support and Waste Management and Remediation Services",
        "Servicios administrativos, de apoyo y de gestión de residuos",
    ),
    "61": (
        "Serviços educacionais",
        "Educational Services",
        "Servicios educativos",
    ),
    "62": (
        "Saúde e assistência social",
        "Health Care and Social Assistance",
        "Atención de la salud y asistencia social",
    ),
    "71": (
        "Artes, entretenimento e recreação",
        "Arts, Entertainment, and Recreation",
        "Artes, entretenimiento y recreación",
    ),
    "72": (
        "Alojamento e alimentação",
        "Accommodation and Food Services",
        "Alojamiento y servicios de comida",
    ),
    "81": (
        "Outros serviços, exceto administração pública",
        "Other Services (except Public Administration)",
        "Otros servicios, excepto administración pública",
    ),
    "92": (
        "Administração pública",
        "Public Administration",
        "Administración pública",
    ),
}

# Note appended to every column the source only publishes from a given year.
NOTE_DEMOG = (
    "Disponível apenas para os anos de referência de 2009 em diante; zero nos anos "
    "anteriores. | Available only for data years 2009 onward; zero in earlier years. | "
    "Disponible solo para los años de referencia desde 2009; cero en los años anteriores."
)
NOTE_EDU = (
    "Disponível apenas para os anos de referência de 2009 em diante e restrito a "
    "trabalhadores de 30 anos ou mais; zero nos demais casos. | Available only for data "
    "years 2009 onward and restricted to workers aged 30 and over; zero otherwise. | "
    "Disponible solo desde 2009 y restringido a trabajadores de 30 años o más; cero en "
    "los demás casos."
)
NOTE_FIRM = (
    "Disponível apenas para os anos de referência de 2011 em diante e apenas para o tipo "
    "de vínculo JT02 (todos os empregos privados); zero nos demais casos. | Available "
    "only for data years 2011 onward and only for job type JT02 (All Private Jobs); zero "
    "otherwise. | Disponible solo desde 2011 y solo para el tipo de empleo JT02 (todos "
    "los empleos privados); cero en los demás casos."
)

COUNT_TEXT: dict[str, tuple[str, str, str, str]] = {
    "jobs_total": (
        "Número total de empregos",
        "Total number of jobs",
        "Número total de empleos",
        "",
    ),
    "jobs_age_29_or_younger": (
        "Número de empregos de trabalhadores de 29 anos ou menos",
        "Number of jobs for workers age 29 or younger",
        "Número de empleos de trabajadores de 29 años o menos",
        "A partir do ano de referência de 2012 o universo é restrito a trabalhadores de "
        "14 a 99 anos, de modo que esta faixa passa a ser de 14 a 29 anos. | From data "
        "year 2012 the job frame is restricted to workers aged 14 to 99, so this band "
        "becomes ages 14 to 29. | Desde 2012 el universo se restringe a trabajadores de "
        "14 a 99 años, por lo que esta franja pasa a ser de 14 a 29 años.",
    ),
    "jobs_age_30_to_54": (
        "Número de empregos de trabalhadores de 30 a 54 anos",
        "Number of jobs for workers age 30 to 54",
        "Número de empleos de trabajadores de 30 a 54 años",
        "",
    ),
    "jobs_age_55_or_older": (
        "Número de empregos de trabalhadores de 55 anos ou mais",
        "Number of jobs for workers age 55 or older",
        "Número de empleos de trabajadores de 55 años o más",
        "A partir do ano de referência de 2012 esta faixa é de 55 a 99 anos. | From data "
        "year 2012 this band is ages 55 to 99. | Desde 2012 esta franja es de 55 a 99 años.",
    ),
    "jobs_earnings_1250_or_less": (
        "Número de empregos com rendimento mensal de até 1.250 dólares",
        "Number of jobs with earnings of 1,250 US dollars per month or less",
        "Número de empleos con ingresos mensuales de hasta 1.250 dólares",
        "",
    ),
    "jobs_earnings_1251_to_3333": (
        "Número de empregos com rendimento mensal de 1.251 a 3.333 dólares",
        "Number of jobs with earnings of 1,251 to 3,333 US dollars per month",
        "Número de empleos con ingresos mensuales de 1.251 a 3.333 dólares",
        "",
    ),
    "jobs_earnings_above_3333": (
        "Número de empregos com rendimento mensal superior a 3.333 dólares",
        "Number of jobs with earnings greater than 3,333 US dollars per month",
        "Número de empleos con ingresos mensuales superiores a 3.333 dólares",
        "",
    ),
    "jobs_race_white": (
        "Número de empregos de trabalhadores de raça branca, isoladamente",
        "Number of jobs for workers with race White, Alone",
        "Número de empleos de trabajadores de raza blanca, sola",
        NOTE_DEMOG,
    ),
    "jobs_race_black": (
        "Número de empregos de trabalhadores negros ou afro-americanos, isoladamente",
        "Number of jobs for workers with race Black or African American, Alone",
        "Número de empleos de trabajadores negros o afroamericanos, solos",
        NOTE_DEMOG,
    ),
    "jobs_race_american_indian_alaska_native": (
        "Número de empregos de trabalhadores indígenas americanos ou nativos do Alasca, "
        "isoladamente",
        "Number of jobs for workers with race American Indian or Alaska Native, Alone",
        "Número de empleos de trabajadores indígenas americanos o nativos de Alaska, solos",
        NOTE_DEMOG,
    ),
    "jobs_race_asian": (
        "Número de empregos de trabalhadores asiáticos, isoladamente",
        "Number of jobs for workers with race Asian, Alone",
        "Número de empleos de trabajadores asiáticos, solos",
        NOTE_DEMOG,
    ),
    "jobs_race_native_hawaiian_pacific_islander": (
        "Número de empregos de trabalhadores nativos do Havaí ou de outras ilhas do "
        "Pacífico, isoladamente",
        "Number of jobs for workers with race Native Hawaiian or Other Pacific Islander, "
        "Alone",
        "Número de empleos de trabajadores nativos de Hawái u otras islas del Pacífico, "
        "solos",
        NOTE_DEMOG,
    ),
    "jobs_race_two_or_more": (
        "Número de empregos de trabalhadores de dois ou mais grupos raciais",
        "Number of jobs for workers with two or more race groups",
        "Número de empleos de trabajadores de dos o más grupos raciales",
        NOTE_DEMOG,
    ),
    "jobs_ethnicity_not_hispanic": (
        "Número de empregos de trabalhadores não hispânicos ou latinos",
        "Number of jobs for workers with ethnicity Not Hispanic or Latino",
        "Número de empleos de trabajadores no hispanos o latinos",
        NOTE_DEMOG,
    ),
    "jobs_ethnicity_hispanic": (
        "Número de empregos de trabalhadores hispânicos ou latinos",
        "Number of jobs for workers with ethnicity Hispanic or Latino",
        "Número de empleos de trabajadores hispanos o latinos",
        NOTE_DEMOG,
    ),
    "jobs_education_less_than_high_school": (
        "Número de empregos de trabalhadores sem ensino médio completo",
        "Number of jobs for workers with educational attainment less than high school",
        "Número de empleos de trabajadores sin educación secundaria completa",
        NOTE_EDU,
    ),
    "jobs_education_high_school": (
        "Número de empregos de trabalhadores com ensino médio completo e sem ensino "
        "superior",
        "Number of jobs for workers with educational attainment high school or "
        "equivalent, no college",
        "Número de empleos de trabajadores con secundaria completa y sin estudios "
        "superiores",
        NOTE_EDU,
    ),
    "jobs_education_some_college": (
        "Número de empregos de trabalhadores com ensino superior incompleto ou curso "
        "de nível associado",
        "Number of jobs for workers with educational attainment some college or "
        "Associate degree",
        "Número de empleos de trabajadores con estudios superiores incompletos o título "
        "de nivel asociado",
        NOTE_EDU,
    ),
    "jobs_education_bachelors_or_higher": (
        "Número de empregos de trabalhadores com bacharelado ou pós-graduação",
        "Number of jobs for workers with educational attainment Bachelor's degree or "
        "advanced degree",
        "Número de empleos de trabajadores con licenciatura o posgrado",
        NOTE_EDU,
    ),
    "jobs_sex_male": (
        "Número de empregos de trabalhadores do sexo masculino",
        "Number of jobs for male workers",
        "Número de empleos de trabajadores de sexo masculino",
        NOTE_DEMOG,
    ),
    "jobs_sex_female": (
        "Número de empregos de trabalhadoras do sexo feminino",
        "Number of jobs for female workers",
        "Número de empleos de trabajadoras de sexo femenino",
        NOTE_DEMOG,
    ),
    "jobs_firm_age_0_to_1": (
        "Número de empregos em firmas com 0 a 1 ano de idade",
        "Number of jobs at firms aged 0 to 1 years",
        "Número de empleos en empresas de 0 a 1 año de antigüedad",
        NOTE_FIRM,
    ),
    "jobs_firm_age_2_to_3": (
        "Número de empregos em firmas com 2 a 3 anos de idade",
        "Number of jobs at firms aged 2 to 3 years",
        "Número de empleos en empresas de 2 a 3 años de antigüedad",
        NOTE_FIRM,
    ),
    "jobs_firm_age_4_to_5": (
        "Número de empregos em firmas com 4 a 5 anos de idade",
        "Number of jobs at firms aged 4 to 5 years",
        "Número de empleos en empresas de 4 a 5 años de antigüedad",
        NOTE_FIRM,
    ),
    "jobs_firm_age_6_to_10": (
        "Número de empregos em firmas com 6 a 10 anos de idade",
        "Number of jobs at firms aged 6 to 10 years",
        "Número de empleos en empresas de 6 a 10 años de antigüedad",
        NOTE_FIRM,
    ),
    "jobs_firm_age_11_or_more": (
        "Número de empregos em firmas com 11 anos de idade ou mais",
        "Number of jobs at firms aged 11 or more years",
        "Número de empleos en empresas de 11 o más años de antigüedad",
        NOTE_FIRM,
    ),
    "jobs_firm_size_0_to_19": (
        "Número de empregos em firmas com 0 a 19 empregados",
        "Number of jobs at firms with 0 to 19 employees",
        "Número de empleos en empresas con 0 a 19 empleados",
        NOTE_FIRM,
    ),
    "jobs_firm_size_20_to_49": (
        "Número de empregos em firmas com 20 a 49 empregados",
        "Number of jobs at firms with 20 to 49 employees",
        "Número de empleos en empresas con 20 a 49 empleados",
        NOTE_FIRM,
    ),
    "jobs_firm_size_50_to_249": (
        "Número de empregos em firmas com 50 a 249 empregados",
        "Number of jobs at firms with 50 to 249 employees",
        "Número de empleos en empresas con 50 a 249 empleados",
        NOTE_FIRM,
    ),
    "jobs_firm_size_250_to_499": (
        "Número de empregos em firmas com 250 a 499 empregados",
        "Number of jobs at firms with 250 to 499 employees",
        "Número de empleos en empresas con 250 a 499 empleados",
        NOTE_FIRM,
    ),
    "jobs_firm_size_500_or_more": (
        "Número de empregos em firmas com 500 empregados ou mais",
        "Number of jobs at firms with 500 or more employees",
        "Número de empleos en empresas con 500 o más empleados",
        NOTE_FIRM,
    ),
}

for _code, (_pt, _en, _es) in NAICS_SECTORS.items():
    COUNT_TEXT[f"jobs_naics_{_code}"] = (
        f"Número de empregos no setor NAICS {_code.replace('_', '-')} ({_pt})",
        f"Number of jobs in NAICS sector {_code.replace('_', '-')} ({_en})",
        f"Número de empleos en el sector NAICS {_code.replace('_', '-')} ({_es})",
        "",
    )


def geo_key_rows(geo_kind: str) -> list[dict]:
    """Key columns. ``geo_kind`` is 'residence' or 'workplace'."""
    if geo_kind == "residence":
        pt, en, es = "residência", "residence", "residencia"
        original = "h_geocode"
    else:
        pt, en, es = "trabalho", "workplace", "trabajo"
        original = "w_geocode"
    return [
        row(
            "year",
            "INT64",
            "Ano de referência dos dados",
            "Reference year of the data",
            "Año de referencia de los datos",
            directory=DIR_YEAR,
            unit="year",
            original=original,
            observations="Derivado do nome do arquivo de origem. | Derived from the "
            "source file name. | Derivado del nombre del archivo de origen.",
        ),
        row(
            "state_id",
            "STRING",
            f"Código FIPS do estado do bloco censitário de {pt}",
            f"State FIPS code of the {en} census block",
            f"Código FIPS del estado del bloque censal de {es}",
            directory=DIR_STATE,
            original=original,
            observations="Dois primeiros dígitos do código do bloco. | First two digits "
            "of the block code. | Dos primeros dígitos del código del bloque.",
        ),
        row(
            "county_id",
            "STRING",
            f"Código FIPS do condado do bloco censitário de {pt}",
            f"County FIPS code of the {en} census block",
            f"Código FIPS del condado del bloque censal de {es}",
            directory=DIR_COUNTY,
            original=original,
            observations="Cinco primeiros dígitos do código do bloco. | First five "
            "digits of the block code. | Cinco primeros dígitos del código del bloque.",
        ),
        row(
            "census_tract_id",
            "STRING",
            f"Código do setor censitário de 2020 do bloco de {pt}",
            f"2020 census tract code of the {en} block",
            f"Código del sector censal de 2020 del bloque de {es}",
            directory=DIR_TRACT,
            original=original,
            observations="Onze primeiros dígitos do código do bloco. | First eleven "
            "digits of the block code. | Once primeros dígitos del código del bloque.",
        ),
        row(
            "block_id",
            "STRING",
            f"Código do bloco censitário de tabulação de 2020 de {pt}",
            f"2020 census tabulation block code of {en}",
            f"Código del bloque censal de tabulación de 2020 de {es}",
            original=original,
            observations="Código de 15 dígitos. Não há diretório de blocos censitários "
            "em br_bd_diretorios_us, que vai apenas até o setor censitário, portanto "
            "esta coluna não tem chave estrangeira. | 15-digit code. br_bd_diretorios_us "
            "has no census block table (it stops at the census tract), so this column "
            "carries no directory foreign key. | Código de 15 dígitos. No existe "
            "directorio de bloques censales en br_bd_diretorios_us, que llega solo hasta "
            "el sector censal, por lo que esta columna no tiene clave foránea.",
        ),
        row(
            "job_type",
            "STRING",
            "Tipo de vínculo empregatício considerado na tabulação",
            "Job type covered by the tabulation",
            "Tipo de empleo considerado en la tabulación",
            dictionary="yes",
            original="[TYPE]",
            observations="Derivado do nome do arquivo de origem. JT04 e JT05 (empregos "
            "federais) existem apenas a partir de 2010. | Derived from the source file "
            "name. JT04 and JT05 (federal jobs) exist only from 2010 onward. | Derivado "
            "del nombre del archivo de origen. JT04 y JT05 existen solo desde 2010.",
        ),
    ]


def count_rows(counts) -> list[dict]:
    out = []
    for original, name in counts:
        pt, en, es, obs = COUNT_TEXT[name]
        out.append(
            row(
                name,
                "INT64",
                pt,
                en,
                es,
                original=original,
                unit="job",
                observations=obs,
            )
        )
    return out


DATE_CREATED = row(
    "date_created",
    "DATE",
    "Data de criação do arquivo de origem pelo Census Bureau",
    "Date on which the source file was created by the Census Bureau",
    "Fecha de creación del archivo de origen por el Census Bureau",
    original="createdate",
    observations="Marca um ponto do processamento interno do Census Bureau, e não a "
    "data de divulgação (data vintage) da versão do LODES. | Marks a point in the Census "
    "Bureau's internal processing, not the release date (data vintage) of the LODES "
    "version. | Marca un punto del procesamiento interno del Census Bureau, no la fecha "
    "de publicación de la versión de LODES.",
)


# --------------------------------------------------------------------------
# geography_crosswalk
# --------------------------------------------------------------------------
XWALK_TEXT: dict[str, tuple[str, str, str, str, str]] = {
    # name: (pt, en, es, directory, observations)
    "block_id": (
        "Código do bloco censitário de tabulação de 2020",
        "2020 census tabulation block code",
        "Código del bloque censal de tabulación de 2020",
        "",
        "Chave primária da tabela. | Primary key of the table. | Clave primaria de la tabla.",
    ),
    "state_id": (
        "Código FIPS do estado",
        "State FIPS code",
        "Código FIPS del estado",
        DIR_STATE,
        "",
    ),
    "state_abbreviation": (
        "Sigla USPS do estado",
        "USPS state abbreviation",
        "Sigla USPS del estado",
        "",
        "O diretório de estados é chaveado pelo código FIPS, e o backend só aceita "
        "vínculo com a chave primária do diretório, portanto esta coluna não tem chave "
        "estrangeira. | The state directory is keyed on the FIPS code and the backend "
        "only accepts a link to a directory's primary key, so this column carries no "
        "foreign key. | El directorio de estados usa el código FIPS como clave, por lo "
        "que esta columna no tiene clave foránea.",
    ),
    "state_name": (
        "Nome do estado",
        "State name",
        "Nombre del estado",
        "",
        "",
    ),
    "county_id": (
        "Código FIPS do condado",
        "County FIPS code",
        "Código FIPS del condado",
        DIR_COUNTY,
        "",
    ),
    "county_name": (
        "Nome do condado ou equivalente",
        "County or county equivalent name",
        "Nombre del condado o equivalente",
        "",
        "",
    ),
    "census_tract_id": (
        "Código do setor censitário de 2020",
        "2020 census tract code",
        "Código del sector censal de 2020",
        DIR_TRACT,
        "",
    ),
    "census_tract_name": (
        "Nome do setor censitário, com condado e estado",
        "Census tract name, formatted with county and state",
        "Nombre del sector censal, con condado y estado",
        "",
        "",
    ),
    "block_group_id": (
        "Código do grupo de blocos censitários",
        "Census block group code",
        "Código del grupo de bloques censales",
        "",
        "Não há diretório de grupos de blocos em br_bd_diretorios_us. | "
        "br_bd_diretorios_us has no block group table. | No existe directorio de grupos "
        "de bloques en br_bd_diretorios_us.",
    ),
    "block_group_name": (
        "Nome do grupo de blocos, com setor censitário, condado e estado",
        "Census block group name, formatted with tract, county and state",
        "Nombre del grupo de bloques, con sector censal, condado y estado",
        "",
        "",
    ),
    "cbsa_id": (
        "Código da área estatística baseada em núcleo (CBSA)",
        "Core Based Statistical Area (CBSA) code",
        "Código del área estadística basada en núcleo (CBSA)",
        "",
        "O diretório cbsa_2023 usa a delimitação de 2023, enquanto o LODES traz a "
        "delimitação vigente na divulgação; o vínculo não é declarado para evitar falsos "
        "negativos. | The cbsa_2023 directory uses the 2023 delineation while LODES "
        "carries the delineation current at release, so no foreign key is declared. | El "
        "directorio cbsa_2023 usa la delimitación de 2023 mientras LODES trae la vigente "
        "en la publicación, por lo que no se declara clave foránea.",
    ),
    "cbsa_name": (
        "Nome da área estatística baseada em núcleo (CBSA)",
        "Core Based Statistical Area (CBSA) name",
        "Nombre del área estadística basada en núcleo (CBSA)",
        "",
        "",
    ),
    "zcta_id": (
        "Código da área de tabulação de código postal (ZCTA)",
        "ZIP Code Tabulation Area (ZCTA) code",
        "Código del área de tabulación de código postal (ZCTA)",
        "",
        "ZCTA não é o mesmo que código postal ZIP. | A ZCTA is not the same as a ZIP "
        "code. | Una ZCTA no es lo mismo que un código postal ZIP.",
    ),
    "zcta_name": (
        "Nome da área de tabulação de código postal (ZCTA)",
        "ZIP Code Tabulation Area (ZCTA) name",
        "Nombre del área de tabulación de código postal (ZCTA)",
        "",
        "",
    ),
    "place_id": (
        "Código nacional único do lugar (FIPS de estado + FIPS de lugar)",
        "Nationally unique place code (state FIPS + place FIPS)",
        "Código nacional único del lugar (FIPS de estado + FIPS de lugar)",
        "",
        "",
    ),
    "place_name": ("Nome do lugar", "Place name", "Nombre del lugar", "", ""),
    "county_subdivision_id": (
        "Código nacional único da subdivisão de condado",
        "Nationally unique county subdivision code",
        "Código nacional único de la subdivisión de condado",
        "",
        "",
    ),
    "county_subdivision_name": (
        "Nome da subdivisão de condado",
        "County subdivision name",
        "Nombre de la subdivisión de condado",
        "",
        "",
    ),
    "congressional_district_id": (
        "Código nacional único do distrito eleitoral do 119º Congresso",
        "Nationally unique 119th Congressional District code",
        "Código nacional único del distrito electoral del 119º Congreso",
        "",
        "Estados com distrito único recebem o número 00 e territórios com delegado ou "
        "comissário residente recebem 98. | States with an At Large district get district "
        "number 00 and state equivalents with a Delegate or Resident Commissioner get 98. "
        "| Los estados con distrito único reciben 00 y los territorios con delegado "
        "reciben 98.",
    ),
    "congressional_district_name": (
        "Nome do distrito eleitoral do 119º Congresso",
        "119th Congressional District name",
        "Nombre del distrito electoral del 119º Congreso",
        "",
        "",
    ),
    "state_legislative_district_lower_id": (
        "Código nacional único do distrito legislativo estadual da câmara baixa",
        "Nationally unique state legislative district code, lower chamber",
        "Código nacional único del distrito legislativo estatal de la cámara baja",
        "",
        "",
    ),
    "state_legislative_district_lower_name": (
        "Nome do distrito legislativo estadual da câmara baixa",
        "State legislative district name, lower chamber",
        "Nombre del distrito legislativo estatal de la cámara baja",
        "",
        "",
    ),
    "state_legislative_district_upper_id": (
        "Código nacional único do distrito legislativo estadual da câmara alta",
        "Nationally unique state legislative district code, upper chamber",
        "Código nacional único del distrito legislativo estatal de la cámara alta",
        "",
        "",
    ),
    "state_legislative_district_upper_name": (
        "Nome do distrito legislativo estadual da câmara alta",
        "State legislative district name, upper chamber",
        "Nombre del distrito legislativo estatal de la cámara alta",
        "",
        "",
    ),
    "school_district_id": (
        "Código nacional único do distrito escolar unificado ou de ensino fundamental",
        "Nationally unique unified or elementary school district code",
        "Código nacional único del distrito escolar unificado o de enseñanza primaria",
        "",
        "Composto pelo FIPS do estado e pelo código de cinco dígitos da agência local de "
        "educação (LEA). | State FIPS plus the five-digit Local Education Agency (LEA) "
        "code. | FIPS del estado más el código de cinco dígitos de la agencia local de "
        "educación (LEA).",
    ),
    "school_district_name": (
        "Nome do distrito escolar unificado ou de ensino fundamental",
        "Unified or elementary school district name",
        "Nombre del distrito escolar unificado o de enseñanza primaria",
        "",
        "",
    ),
    "secondary_school_district_id": (
        "Código nacional único do distrito escolar de ensino médio",
        "Nationally unique secondary school district code",
        "Código nacional único del distrito escolar de enseñanza secundaria",
        "",
        "",
    ),
    "secondary_school_district_name": (
        "Nome do distrito escolar de ensino médio",
        "Secondary school district name",
        "Nombre del distrito escolar de enseñanza secundaria",
        "",
        "",
    ),
    "tribal_area_id": (
        "Código da área indígena americana, nativa do Alasca ou nativa do Havaí",
        "American Indian, Alaska Native or Native Hawaiian area census code",
        "Código del área indígena americana, nativa de Alaska o nativa de Hawái",
        "",
        "",
    ),
    "tribal_area_name": (
        "Nome da área indígena americana, nativa do Alasca ou nativa do Havaí",
        "American Indian, Alaska Native or Native Hawaiian area name",
        "Nombre del área indígena americana, nativa de Alaska o nativa de Hawái",
        "",
        "",
    ),
    "tribal_subdivision_id": (
        "Código da subdivisão tribal indígena americana",
        "American Indian tribal subdivision code",
        "Código de la subdivisión tribal indígena americana",
        "",
        "",
    ),
    "tribal_subdivision_name": (
        "Nome da subdivisão tribal indígena americana",
        "American Indian tribal subdivision name",
        "Nombre de la subdivisión tribal indígena americana",
        "",
        "",
    ),
    "alaska_native_corporation_id": (
        "Código nacional único da corporação regional nativa do Alasca (ANRC)",
        "Nationally unique Alaska Native Regional Corporation (ANRC) code",
        "Código nacional único de la corporación regional nativa de Alaska (ANRC)",
        "",
        "",
    ),
    "alaska_native_corporation_name": (
        "Nome da corporação regional nativa do Alasca",
        "Alaska Native Regional Corporation name",
        "Nombre de la corporación regional nativa de Alaska",
        "",
        "",
    ),
    "military_installation_id": (
        "Código do marco de instalação militar",
        "Military installation landmark code",
        "Código del hito de instalación militar",
        "",
        "",
    ),
    "military_installation_name": (
        "Nome da instalação militar",
        "Military installation name",
        "Nombre de la instalación militar",
        "",
        "",
    ),
    "workforce_board_id": (
        "Código nacional único da área do conselho de inovação da força de trabalho (WIB)",
        "Nationally unique Workforce Innovation Board (WIB) area code",
        "Código nacional único del área del consejo de innovación laboral (WIB)",
        "",
        "",
    ),
    "workforce_board_name": (
        "Nome da área do conselho de inovação da força de trabalho",
        "Workforce Innovation Board area name",
        "Nombre del área del consejo de innovación laboral",
        "",
        "",
    ),
}


def xwalk_rows() -> list[dict]:
    out = []
    for original, name in XWALK_COLUMNS:
        if name == "latitude":
            out.append(
                row(
                    "latitude",
                    "FLOAT64",
                    "Latitude do ponto interno do bloco censitário, em graus decimais",
                    "Latitude of the census block internal point, in decimal degrees",
                    "Latitud del punto interno del bloque censal, en grados decimales",
                    original=original,
                    unit="degree",
                    observations="O ponto interno não é o centroide; a única garantia é "
                    "que ele está dentro do bloco. | The internal point is not a "
                    "centroid; the only guarantee is that it lies inside the block. | El "
                    "punto interno no es el centroide; la única garantía es que está "
                    "dentro del bloque.",
                )
            )
            continue
        if name == "longitude":
            out.append(
                row(
                    "longitude",
                    "FLOAT64",
                    "Longitude do ponto interno do bloco censitário, em graus decimais",
                    "Longitude of the census block internal point, in decimal degrees",
                    "Longitud del punto interno del bloque censal, en grados decimales",
                    original=original,
                    unit="degree",
                )
            )
            continue
        if name == "date_created":
            d = dict(DATE_CREATED)
            out.append(d)
            continue
        pt, en, es, directory, obs = XWALK_TEXT[name]
        out.append(
            row(
                name,
                "STRING",
                pt,
                en,
                es,
                original=original,
                directory=directory,
                observations=obs,
            )
        )
    return out


DICIONARIO_ROWS = [
    row(
        "id_tabela",
        "STRING",
        "Nome da tabela",
        "Table name",
        "Nombre de la tabla",
    ),
    row(
        "nome_coluna",
        "STRING",
        "Nome da coluna",
        "Column name",
        "Nombre de la columna",
    ),
    row("chave", "STRING", "Chave do valor", "Value key", "Clave del valor"),
    row(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal do valor",
        "Temporal coverage of the value",
        "Cobertura temporal del valor",
    ),
    row(
        "valor",
        "STRING",
        "Valor traduzido",
        "Translated value",
        "Valor traducido",
    ),
]


TABLES = {
    "residence_jobs": lambda: (
        geo_key_rows("residence") + count_rows(RAC_COUNTS) + [DATE_CREATED]
    ),
    "workplace_jobs": lambda: (
        geo_key_rows("workplace") + count_rows(WAC_COUNTS) + [DATE_CREATED]
    ),
    "geography_crosswalk": xwalk_rows,
    "dicionario": lambda: DICIONARIO_ROWS,
}


def main() -> None:
    ARCHITECTURE_DIR.mkdir(parents=True, exist_ok=True)
    for table, builder in TABLES.items():
        rows = builder()
        path = ARCHITECTURE_DIR / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        bad = [
            r["name"] for r in rows if r["description"].rstrip().endswith(".")
        ]
        if bad:
            raise SystemExit(
                f"{table}: descriptions must not end with a period: {bad}"
            )
        numeric_no_unit = [
            r["name"]
            for r in rows
            if r["bigquery_type"] in ("INT64", "FLOAT64")
            and not r["measurement_unit"]
        ]
        if numeric_no_unit:
            raise SystemExit(
                f"{table}: numeric columns without measurement_unit: {numeric_no_unit}"
            )
        print(f"{table}: {len(rows)} columns -> {path}")


if __name__ == "__main__":
    main()
