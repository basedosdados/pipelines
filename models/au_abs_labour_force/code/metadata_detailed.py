#!/usr/bin/env python3
"""Backend metadata spec for the 12 Detailed-release tables.

Names, descriptions, observation levels, coverage and update cadence, in one
place. Consumed by `register_detailed.py`; column-level metadata comes from
`code/columns_json_detailed/`, generated from the architecture CSVs.

Coverage bounds were measured from the cleaned parquet, not read off the ABS
landing page: a monthly or quarterly table needs month-granular bounds, and
year-only bounds understate both endpoints.
"""

DATASET_SLUG = "au_abs_labour_force"

# ── raw data source ──────────────────────────────────────────────────────────
RAW_SOURCE = {
    "name_pt": "Cubos de dados — Força de Trabalho, Austrália, Detalhada (ABS)",
    "name_en": "Data cubes — Labour Force, Australia, Detailed (ABS)",
    "name_es": "Cubos de datos — Fuerza Laboral, Australia, Detallada (ABS)",
    "description_pt": (
        "Cubos de dados trimestrais e mensais da publicação Labour Force, "
        "Australia, Detailed (catálogo 6291.0.55.001) do Australian Bureau of "
        "Statistics, em planilhas Excel. A publicação foi encerrada no período "
        "de referência de março de 2026; as tabelas remanescentes passam a "
        "integrar Labour Force, Australia a partir de setembro de 2026."
    ),
    "description_en": (
        "Quarterly and monthly data cubes of the Australian Bureau of "
        "Statistics release Labour Force, Australia, Detailed (catalogue "
        "6291.0.55.001), published as Excel spreadsheets. The release ceased "
        "with the March 2026 reference period; the surviving tables move into "
        "Labour Force, Australia from September 2026."
    ),
    "description_es": (
        "Cubos de datos trimestrales y mensuales de la publicación Labour "
        "Force, Australia, Detailed (catálogo 6291.0.55.001) de la Oficina "
        "Australiana de Estadística, en hojas de cálculo Excel. La publicación "
        "cesó en el período de referencia de marzo de 2026; las tablas "
        "restantes pasan a Labour Force, Australia desde septiembre de 2026."
    ),
    "url": (
        "https://www.abs.gov.au/statistics/labour/employment-and-unemployment/"
        "labour-force-australia-detailed/mar-2026"
    ),
    "license_slug": "cc_by",
    "availability_slug": "online",
}

# ── dataset description, refreshed for the extended coverage ─────────────────
DATASET_DESCRIPTION = {
    "pt": (
        "Estatísticas da força de trabalho australiana produzidas pelo "
        "Australian Bureau of Statistics, reunindo a publicação mensal Labour "
        "Force, Australia (catálogo 6202.0) desde fevereiro de 1978 e os cubos "
        "trimestrais e mensais da publicação encerrada Labour Force, "
        "Australia, Detailed (catálogo 6291.0.55.001) desde novembro de 1984. "
        "Abrange emprego, desemprego, participação, horas trabalhadas, posição "
        "na ocupação, subutilização da força de trabalho, indústria (ANZSIC "
        "2006), ocupação (ANZSCO 2013), tempo no trabalho atual e duração da "
        "procura por trabalho, para a Austrália e os estados e territórios."
    ),
    "en": (
        "Australian labour force statistics produced by the Australian Bureau "
        "of Statistics, bringing together the monthly Labour Force, Australia "
        "release (catalogue 6202.0) since February 1978 and the quarterly and "
        "monthly data cubes of the ceased Labour Force, Australia, Detailed "
        "release (catalogue 6291.0.55.001) since November 1984. It covers "
        "employment, unemployment, participation, hours worked, status in "
        "employment, labour underutilisation, industry (ANZSIC 2006), "
        "occupation (ANZSCO 2013), job tenure and duration of job search, for "
        "Australia and the states and territories."
    ),
    "es": (
        "Estadísticas de la fuerza laboral australiana producidas por la "
        "Oficina Australiana de Estadística, que reúnen la publicación mensual "
        "Labour Force, Australia (catálogo 6202.0) desde febrero de 1978 y los "
        "cubos trimestrales y mensuales de la publicación cesada Labour Force, "
        "Australia, Detailed (catálogo 6291.0.55.001) desde noviembre de 1984. "
        "Abarca empleo, desempleo, participación, horas trabajadas, situación "
        "en el empleo, subutilización de la fuerza laboral, industria (ANZSIC "
        "2006), ocupación (ANZSCO 2013), antigüedad en el empleo y duración de "
        "la búsqueda de trabajo, para Australia y los estados y territorios."
    ),
}

# Subject-matter tags only: no place names, no theme restatements, no
# organization names (metadata-schema, "Choosing tags").
# Verified against the live staging vocabulary (877 tags): six already exist
# with Portuguese slugs, and there is no tag for unemployment, so one is created
# with an English slug per the naming rule.
DATASET_TAGS = [
    "emprego",
    "unemployment",
    "trabalho",
    "ocupacao",
    "atividade_economica",
    "carga_horaria",
    "pesquisa",
]

# ── per table ────────────────────────────────────────────────────────────────
# slug -> names, descriptions, observation levels, coverage, update entity
TABLE_META: dict[str, dict] = {
    "employment_industry": {
        "name_pt": "Emprego por grupo de indústria, estado e sexo",
        "name_en": "Employment by industry group, state and sex",
        "name_es": "Empleo por grupo de industria, estado y sexo",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos, por trimestre, grupo de indústria do trabalho principal "
            "(ANZSIC 2006), estado ou território e sexo."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs, by "
            "quarter, industry group of the main job (ANZSIC 2006), state or "
            "territory and sex."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos, por trimestre, grupo de industria del trabajo principal "
            "(ANZSIC 2006), estado o territorio y sexo."
        ),
        "observation_levels": {
            "state": ["geography"],
            "industry": ["industry_group_code", "industry_group"],
            "sex": ["sex"],
        },
        "coverage": (1984, 11, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_industry_region": {
        "name_pt": "Emprego por divisão de indústria, região de grande capital e sexo",
        "name_en": "Employment by industry division, greater capital city area and sex",
        "name_es": "Empleo por división de industria, área de gran capital y sexo",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos, por trimestre, divisão de indústria do trabalho "
            "principal (ANZSIC 2006), grande capital ou restante do estado e "
            "sexo."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs, by "
            "quarter, industry division of the main job (ANZSIC 2006), greater "
            "capital city or rest of state area and sex."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos, por trimestre, división de industria del trabajo "
            "principal (ANZSIC 2006), gran capital o resto del estado y sexo."
        ),
        "observation_levels": {
            "region": ["gccsa"],
            "industry": ["industry_division_code", "industry_division"],
            "sex": ["sex"],
        },
        "coverage": (1984, 11, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_industry_age": {
        "name_pt": "Emprego por divisão de indústria e faixa etária",
        "name_en": "Employment by industry division and age group",
        "name_es": "Empleo por división de industria y grupo de edad",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos na Austrália, por trimestre, divisão de indústria do "
            "trabalho principal (ANZSIC 2006) e faixa etária quinquenal."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs in "
            "Australia, by quarter, industry division of the main job (ANZSIC "
            "2006) and five-year age group."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos en Australia, por trimestre, división de industria del "
            "trabajo principal (ANZSIC 2006) y grupo de edad quinquenal."
        ),
        "observation_levels": {
            "industry": ["industry_division_code", "industry_division"],
            "age": ["age_group"],
        },
        "coverage": (1984, 11, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_industry_hours": {
        "name_pt": "Emprego por divisão de indústria e horas trabalhadas",
        "name_en": "Employment by industry division and hours worked",
        "name_es": "Empleo por división de industria y horas trabajadas",
        "description_pt": (
            "Pessoas ocupadas e horas trabalhadas na Austrália, por trimestre, "
            "divisão de indústria do trabalho principal (ANZSIC 2006) e faixa "
            "de horas, em três conceitos de horas: habitualmente trabalhadas "
            "em todos os empregos, efetivamente trabalhadas no trabalho "
            "principal e habitualmente trabalhadas no trabalho principal."
        ),
        "description_en": (
            "Employed persons and hours worked in Australia, by quarter, "
            "industry division of the main job (ANZSIC 2006) and band of hours "
            "worked, on three hours concepts: hours usually worked in all "
            "jobs, hours actually worked in main job and hours usually worked "
            "in main job."
        ),
        "description_es": (
            "Personas ocupadas y horas trabajadas en Australia, por trimestre, "
            "división de industria del trabajo principal (ANZSIC 2006) y rango "
            "de horas, en tres conceptos de horas: habitualmente trabajadas en "
            "todos los empleos, efectivamente trabajadas en el trabajo "
            "principal y habitualmente trabajadas en el trabajo principal."
        ),
        "observation_levels": {
            "industry": ["industry_division_code", "industry_division"],
        },
        "coverage": (2001, 5, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_industry_status": {
        "name_pt": "Emprego por divisão de indústria e posição na ocupação",
        "name_en": "Employment by industry division and status in employment",
        "name_es": "Empleo por división de industria y situación en el empleo",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos na Austrália, por trimestre, divisão de indústria do "
            "trabalho principal (ANZSIC 2006) e posição na ocupação."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs in "
            "Australia, by quarter, industry division of the main job (ANZSIC "
            "2006) and status in employment."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos en Australia, por trimestre, división de industria del "
            "trabajo principal (ANZSIC 2006) y situación en el empleo."
        ),
        "observation_levels": {
            "industry": ["industry_division_code", "industry_division"],
        },
        "coverage": (1991, 2, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_industry_occupation": {
        "name_pt": "Emprego por divisão de indústria, grande grupo ocupacional e sexo",
        "name_en": "Employment by industry division, occupation major group and sex",
        "name_es": "Empleo por división de industria, gran grupo ocupacional y sexo",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos na Austrália, por trimestre, divisão de indústria "
            "(ANZSIC 2006), grande grupo ocupacional do trabalho principal "
            "(ANZSCO 2013) e sexo."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs in "
            "Australia, by quarter, industry division (ANZSIC 2006), "
            "occupation major group of the main job (ANZSCO 2013) and sex."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos en Australia, por trimestre, división de industria "
            "(ANZSIC 2006), gran grupo ocupacional del trabajo principal "
            "(ANZSCO 2013) y sexo."
        ),
        "observation_levels": {
            "industry": ["industry_division_code", "industry_division"],
            "occupation": ["occupation_major_code", "occupation_major"],
            "sex": ["sex"],
        },
        "coverage": (1986, 8, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_occupation": {
        "name_pt": "Emprego por subgrupo principal ocupacional, sexo e faixa etária",
        "name_en": "Employment by occupation sub-major group, sex and age group",
        "name_es": "Empleo por subgrupo principal ocupacional, sexo y grupo de edad",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos na Austrália, por trimestre, subgrupo principal "
            "ocupacional do trabalho principal (ANZSCO 2013), sexo e faixa "
            "etária."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs in "
            "Australia, by quarter, occupation sub-major group of the main job "
            "(ANZSCO 2013), sex and age group."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos en Australia, por trimestre, subgrupo principal "
            "ocupacional del trabajo principal (ANZSCO 2013), sexo y grupo de "
            "edad."
        ),
        "observation_levels": {
            "occupation": ["occupation_sub_major"],
            "sex": ["sex"],
            "age": ["age_group"],
        },
        "coverage": (1986, 8, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_occupation_age": {
        "name_pt": "Emprego por grande grupo ocupacional e faixa etária",
        "name_en": "Employment by occupation major group and age group",
        "name_es": "Empleo por gran grupo ocupacional y grupo de edad",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos na Austrália, por trimestre, grande grupo ocupacional do "
            "trabalho principal (ANZSCO 2013) e faixa etária quinquenal."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs in "
            "Australia, by quarter, occupation major group of the main job "
            "(ANZSCO 2013) and five-year age group."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos en Australia, por trimestre, gran grupo ocupacional del "
            "trabajo principal (ANZSCO 2013) y grupo de edad quinquenal."
        ),
        "observation_levels": {
            "occupation": ["occupation_major_code", "occupation_major"],
            "age": ["age_group"],
        },
        "coverage": (1986, 8, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_occupation_status": {
        "name_pt": "Emprego por grande grupo ocupacional e posição na ocupação",
        "name_en": "Employment by occupation major group and status in employment",
        "name_es": "Empleo por gran grupo ocupacional y situación en el empleo",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos na Austrália, por trimestre, grande grupo ocupacional do "
            "trabalho principal (ANZSCO 2013) e posição na ocupação."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs in "
            "Australia, by quarter, occupation major group of the main job "
            "(ANZSCO 2013) and status in employment."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos en Australia, por trimestre, gran grupo ocupacional del "
            "trabajo principal (ANZSCO 2013) y situación en el empleo."
        ),
        "observation_levels": {
            "occupation": ["occupation_major_code", "occupation_major"],
        },
        "coverage": (1991, 2, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_status_hours": {
        "name_pt": "Emprego por posição na ocupação, horas trabalhadas e sexo",
        "name_en": "Employment by status in employment, hours worked and sex",
        "name_es": "Empleo por situación en el empleo, horas trabajadas y sexo",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos na Austrália, por trimestre, posição na ocupação do "
            "trabalho principal, faixa de horas efetivamente trabalhadas e "
            "sexo."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs in "
            "Australia, by quarter, status in employment of the main job, band "
            "of hours actually worked in all jobs and sex."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos en Australia, por trimestre, situación en el empleo del "
            "trabajo principal, rango de horas efectivamente trabajadas y sexo."
        ),
        "observation_levels": {"sex": ["sex"]},
        "coverage": (1991, 2, 2026, 2),
        "update_entity": "quarter",
    },
    "employment_job_tenure": {
        "name_pt": "Emprego por tempo no trabalho atual, estado e sexo",
        "name_en": "Employment by job tenure, state and sex",
        "name_es": "Empleo por antigüedad en el empleo, estado y sexo",
        "description_pt": (
            "Pessoas ocupadas e horas efetivamente trabalhadas em todos os "
            "empregos, por trimestre, tempo com o empregador atual ou no "
            "negócio próprio atual, estado ou território e sexo."
        ),
        "description_en": (
            "Employed persons and hours actually worked in all jobs, by "
            "quarter, time spent with the current employer or in the current "
            "business, state or territory and sex."
        ),
        "description_es": (
            "Personas ocupadas y horas efectivamente trabajadas en todos los "
            "empleos, por trimestre, tiempo con el empleador actual o en el "
            "negocio propio actual, estado o territorio y sexo."
        ),
        "observation_levels": {"state": ["geography"], "sex": ["sex"]},
        "coverage": (2001, 5, 2026, 2),
        "update_entity": "quarter",
    },
    "unemployment_duration": {
        "name_pt": "Desemprego por duração da procura por trabalho",
        "name_en": "Unemployment by duration of job search",
        "name_es": "Desempleo por duración de la búsqueda de trabajo",
        "description_pt": (
            "Pessoas desempregadas e semanas de procura por trabalho, por mês "
            "e duração da procura, desagregadas por estado ou território e, no "
            "caso da Austrália, por faixa etária."
        ),
        "description_en": (
            "Unemployed persons and weeks spent searching for a job, by month "
            "and duration of job search, broken down by state or territory "
            "and, for Australia, by age group."
        ),
        "description_es": (
            "Personas desempleadas y semanas de búsqueda de trabajo, por mes y "
            "duración de la búsqueda, desagregadas por estado o territorio y, "
            "en el caso de Australia, por grupo de edad."
        ),
        "observation_levels": {"state": ["geography"], "age": ["age_group"]},
        "coverage": (1991, 1, 2026, 3),
        "update_entity": "month",
    },
}

# Order the 12 new tables take after the four Tier-1 tables.
TABLE_ORDER = [
    "labour_force_status",
    "underutilisation",
    "hours_worked",
    "status_in_employment",
    "employment_industry",
    "employment_industry_region",
    "employment_industry_age",
    "employment_industry_hours",
    "employment_industry_status",
    "employment_industry_occupation",
    "employment_occupation",
    "employment_occupation_age",
    "employment_occupation_status",
    "employment_status_hours",
    "employment_job_tenure",
    "unemployment_duration",
]
