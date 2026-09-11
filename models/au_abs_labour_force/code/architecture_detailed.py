#!/usr/bin/env python3
"""Emit the architecture CSVs for the 12 Detailed-release tables.

Phase 2 of `au_abs_labour_force` (ABS cat. 6291.0.55.001, ceased March 2026).
The design is fixed by `models/au_abs_labour_force/ONBOARDING_PLAN_DETAILED.md`;
this module is its machine-readable form and the single source of truth for
column order and types, consumed by `clean_detailed.py`, `gen_dbt_detailed.py`
and `build_columns_json_detailed.py`.

Writes `code/architecture_detailed/<table>.csv`. The existing four Tier-1 tables
live in `code/architecture/` and are not touched.

Usage:
    uv run python models/au_abs_labour_force/code/architecture_detailed.py
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "code" / "architecture_detailed"

ARCH_HEADER = [
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
]

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_MONTH = "br_bd_diretorios_data_tempo.mes:mes"

GEO_OBS = (
    "States and territories follow the ASGS (2011) state and territory "
    "structure. To be linked to br_bd_diretorios_au once that directory "
    "carries a state table."
)
SEX_OBS = (
    "The Detailed cubes publish males and females only. A persons total is "
    "not derived by summing the two, because ABS benchmarks and rounds its "
    "estimates independently, so the sum is not the published total."
)
STATUS_OBS = (
    "Finer than the status_in_employment column of the sibling monthly table, "
    "which carries a single rolled-up employee category; joining the two "
    "requires rolling the three employee categories up first."
)
ANZSIC_OBS = (
    "ANZSIC (2006) Rev.2.0, carried by ABS across the whole history of the "
    "cube. A dedicated ANZSIC directory does not exist yet in "
    "br_bd_diretorios_au and is future work."
)
ANZSCO_OBS = (
    "ANZSCO (2013) v1.2, carried by ABS across the whole history of the cube. "
    "A dedicated ANZSCO directory does not exist yet in br_bd_diretorios_au "
    "and is future work."
)

# name -> (bigquery_type, description_en, description_pt, description_es,
#          directory_column, measurement_unit, observations, original_name)
C: dict[str, tuple[str, str, str, str, str, str, str, str]] = {
    "year": (
        "INT64",
        "Reference year of the observation",
        "Ano de referência da observação",
        "Año de referencia de la observación",
        DIR_YEAR,
        "year",
        "Partition column",
        "Mid-quarter month",
    ),
    "quarter": (
        "INT64",
        "Reference quarter of the observation, from 1 to 4",
        "Trimestre de referência da observação, de 1 a 4",
        "Trimestre de referencia de la observación, de 1 a 4",
        "",
        "quarter",
        "Derived from the ABS mid-quarter month: February is quarter 1, May "
        "is 2, August is 3 and November is 4.",
        "Mid-quarter month",
    ),
    "month": (
        "INT64",
        "Reference month of the observation, from 1 to 12",
        "Mês de referência da observação, de 1 a 12",
        "Mes de referencia de la observación, de 1 a 12",
        DIR_MONTH,
        "month",
        "The ABS mid-quarter month, always February, May, August or November.",
        "Mid-quarter month",
    ),
    "month_monthly": (
        "INT64",
        "Reference month of the observation, from 1 to 12",
        "Mês de referência da observação, de 1 a 12",
        "Mes de referencia de la observación, de 1 a 12",
        DIR_MONTH,
        "month",
        "Derived from the monthly reference period.",
        "Month",
    ),
    "geography": (
        "STRING",
        "Geographic area: one of the eight Australian states and territories",
        "Área geográfica: um dos oito estados e territórios australianos",
        "Área geográfica: uno de los ocho estados y territorios australianos",
        "",
        "",
        GEO_OBS,
        "State and territory (STT): ASGS (2011)",
    ),
    "geography_with_national": (
        "STRING",
        "Geographic area: Australia (national) or one of the eight states and "
        "territories",
        "Área geográfica: Austrália (nacional) ou um dos oito estados e "
        "territórios",
        "Área geográfica: Australia (nacional) o uno de los ocho estados y "
        "territorios",
        "",
        "",
        GEO_OBS
        + " Australia carries the rows broken down by age group, which ABS "
        "publishes for the nation only.",
        "State and territory (STT): ASGS (2011)",
    ),
    "gccsa": (
        "STRING",
        "Greater capital city or rest of state area under the ASGS",
        "Grande capital ou restante do estado segundo a ASGS",
        "Gran capital o resto del estado según la ASGS",
        "",
        "",
        "Labels suffixed (old standards) [1960-1991] are the pre-2011 "
        "boundaries that ABS reports alongside the current ones; they are an "
        "ABS series-break marker and are kept verbatim. Twelve of the 26 "
        "values carry it.",
        "Greater capital city and rest of state (GCCSA): ASGS (2011)",
    ),
    "sex": (
        "STRING",
        "Sex: males or females",
        "Sexo: homens ou mulheres",
        "Sexo: hombres o mujeres",
        "",
        "",
        SEX_OBS,
        "Sex",
    ),
    "age_group": (
        "STRING",
        "Age group in years",
        "Faixa etária em anos",
        "Grupo de edad en años",
        "",
        "",
        "ABS standard age groupings. The band set differs across cubes: six "
        "broad bands in some, eleven five-year bands in others.",
        "Age",
    ),
    "status_in_employment": (
        "STRING",
        "Status in employment of the main job, such as employee with paid "
        "leave entitlements or owner manager of an incorporated enterprise",
        "Posição na ocupação do trabalho principal, como empregado com direito "
        "a licença remunerada ou proprietário-gerente de empresa constituída",
        "Situación en el empleo del trabajo principal, como asalariado con "
        "derecho a licencia remunerada o propietario-gerente de empresa "
        "constituida",
        "",
        "",
        STATUS_OBS,
        "Status in employment of main job",
    ),
    "hours_band": (
        "STRING",
        "Range of hours worked in the reference week",
        "Faixa de horas trabalhadas na semana de referência",
        "Rango de horas trabajadas en la semana de referencia",
        "",
        "",
        "The hours concept the band applies to is given by the accompanying "
        "hours measure, where present.",
        "Hours actually worked in all jobs",
    ),
    "hours_measure": (
        "STRING",
        "Hours concept the band applies to: hours usually worked in all jobs, "
        "hours actually worked in main job, or hours usually worked in main job",
        "Conceito de horas ao qual a faixa se aplica: horas habitualmente "
        "trabalhadas em todos os empregos, horas efetivamente trabalhadas no "
        "trabalho principal ou horas habitualmente trabalhadas no trabalho "
        "principal",
        "Concepto de horas al que se aplica el rango: horas habitualmente "
        "trabajadas en todos los empleos, horas efectivamente trabajadas en el "
        "trabajo principal u horas habitualmente trabajadas en el trabajo "
        "principal",
        "",
        "",
        "Carries the concept that distinguished ABS cubes EQ10, EQ11 and EQ14, "
        "which share an identical grain and measure block. The four "
        "cube-specific zero labels are normalised to a single Did not work (0 "
        "hours) because this column now carries the concept.",
        "(cube identity: EQ10, EQ11, EQ14)",
    ),
    "industry_division_code": (
        "STRING",
        "ANZSIC 2006 division letter, from A to S",
        "Letra da divisão ANZSIC 2006, de A a S",
        "Letra de la división ANZSIC 2006, de A a S",
        "",
        "",
        "Not published in the cube: attached from the ANZSIC (2006) Rev.2.0 "
        "division list, asserted to cover every observed label. In "
        "employment_industry, where the cube publishes group-level codes, the "
        "division is derived from the group code's ANZSIC subdivision (its "
        "first two digits), and directly from the letter for the 19 "
        "division-level nfd residuals.",
        "Industry division of main job: ANZSIC (2006) Rev.2.0",
    ),
    "industry_division": (
        "STRING",
        "ANZSIC 2006 division of the main job",
        "Divisão ANZSIC 2006 do trabalho principal",
        "División ANZSIC 2006 del trabajo principal",
        "",
        "",
        ANZSIC_OBS,
        "Industry division of main job: ANZSIC (2006) Rev.2.0",
    ),
    "industry_group_code": (
        "STRING",
        "ANZSIC 2006 group code of the main job",
        "Código do grupo ANZSIC 2006 do trabalho principal",
        "Código del grupo ANZSIC 2006 del trabajo principal",
        "",
        "",
        "Split from the code-prefixed label ABS publishes. Stored as a string "
        "because the leading zero is significant. 272 of the 291 values are a "
        "real three-digit ANZSIC group code; the other 19 are the "
        "division-level not further defined residuals, which ABS codes as the "
        "division letter followed by 00, one per ANZSIC division.",
        "Industry group of main job: ANZSIC (2006) Rev.2.0",
    ),
    "industry_group": (
        "STRING",
        "ANZSIC 2006 group of the main job",
        "Grupo ANZSIC 2006 do trabalho principal",
        "Grupo ANZSIC 2006 del trabajo principal",
        "",
        "",
        ANZSIC_OBS
        + " The three-character code prefix ABS ships on the label is split "
        "out into industry_group_code. Labels ending in nfd are ABS not "
        "further defined residuals.",
        "Industry group of main job: ANZSIC (2006) Rev.2.0",
    ),
    "occupation_major_code": (
        "STRING",
        "ANZSCO major group code, from 1 to 8",
        "Código do grande grupo ANZSCO, de 1 a 8",
        "Código del gran grupo ANZSCO, de 1 a 8",
        "",
        "",
        "Not published in the cube: attached from the ANZSCO (2013) v1.2 major "
        "group list, asserted to cover every observed label.",
        "Occupation major group of main job: ANZSCO (2013) v1.2",
    ),
    "occupation_major": (
        "STRING",
        "ANZSCO major group of the main job",
        "Grande grupo ANZSCO do trabalho principal",
        "Gran grupo ANZSCO del trabajo principal",
        "",
        "",
        ANZSCO_OBS,
        "Occupation major group of main job: ANZSCO (2013) v1.2",
    ),
    "occupation_sub_major": (
        "STRING",
        "ANZSCO sub-major group of the main job",
        "Subgrupo principal ANZSCO do trabalho principal",
        "Subgrupo principal ANZSCO del trabajo principal",
        "",
        "",
        ANZSCO_OBS
        + " No code is attached: the two-digit correspondence includes nfd "
        "residuals that are not derivable from the label.",
        "Occupation sub-major group of main job: ANZSCO (2013) v1.2",
    ),
    "occupation_classification": (
        "STRING",
        "Occupation classification the labels belong to",
        "Classificação ocupacional a que os rótulos pertencem",
        "Clasificación ocupacional a la que pertenecen las etiquetas",
        "",
        "",
        "Constant at ANZSCO (2013) v1.2 across the whole history of this "
        "ceased release. Carried explicitly so that the OSCA-coded rows ABS "
        "begins publishing from the September 2026 reference period remain "
        "distinguishable in the same column.",
        "(constant)",
    ),
    "tenure_band": (
        "STRING",
        "Time spent with the current employer or in the current business",
        "Tempo com o empregador atual ou no negócio próprio atual",
        "Tiempo con el empleador actual o en el negocio propio actual",
        "",
        "",
        "The label 12 months or more [2001-2014] is the coarse band ABS "
        "published before the finer bands were introduced in 2014; it is an "
        "ABS series-break marker and is kept verbatim.",
        "Number of months or years with current employer or business",
    ),
    "duration_band": (
        "STRING",
        "Duration of job search",
        "Duração da procura por trabalho",
        "Duración de la búsqueda de trabajo",
        "",
        "",
        "",
        "Duration of job search",
    ),
    "employed_full_time": (
        "FLOAT64",
        "Number of persons employed full-time",
        "Número de pessoas ocupadas em tempo integral",
        "Número de personas ocupadas a tiempo completo",
        "",
        "person",
        "Converted from ABS thousands to absolute persons",
        "Employed full-time ('000)",
    ),
    "employed_part_time": (
        "FLOAT64",
        "Number of persons employed part-time",
        "Número de pessoas ocupadas em tempo parcial",
        "Número de personas ocupadas a tiempo parcial",
        "",
        "person",
        "Converted from ABS thousands to absolute persons",
        "Employed part-time ('000)",
    ),
    "hours_worked_full_time": (
        "FLOAT64",
        "Total hours actually worked in all jobs by persons employed full-time",
        "Total de horas efetivamente trabalhadas em todos os empregos por "
        "pessoas ocupadas em tempo integral",
        "Total de horas efectivamente trabajadas en todos los empleos por "
        "personas ocupadas a tiempo completo",
        "",
        "hour",
        "Converted from ABS thousands of hours to hours",
        "Number of hours actually worked in all jobs (employed full-time) "
        "('000 Hours)",
    ),
    "hours_worked_part_time": (
        "FLOAT64",
        "Total hours actually worked in all jobs by persons employed part-time",
        "Total de horas efetivamente trabalhadas em todos os empregos por "
        "pessoas ocupadas em tempo parcial",
        "Total de horas efectivamente trabajadas en todos los empleos por "
        "personas ocupadas a tiempo parcial",
        "",
        "hour",
        "Converted from ABS thousands of hours to hours",
        "Number of hours actually worked in all jobs (employed part-time) "
        "('000 Hours)",
    ),
    "unemployed_total": (
        "FLOAT64",
        "Number of unemployed persons",
        "Número de pessoas desempregadas",
        "Número de personas desempleadas",
        "",
        "person",
        "Converted from ABS thousands to absolute persons",
        "Unemployed total ('000)",
    ),
    "unemployed_looked_for_full_time": (
        "FLOAT64",
        "Number of unemployed persons who looked for full-time work",
        "Número de pessoas desempregadas que procuraram trabalho em tempo "
        "integral",
        "Número de personas desempleadas que buscaron trabajo a tiempo completo",
        "",
        "person",
        "Converted from ABS thousands to absolute persons",
        "Unemployed looked for full-time work ('000)",
    ),
    "unemployed_looked_for_part_time": (
        "FLOAT64",
        "Number of unemployed persons who looked for only part-time work",
        "Número de pessoas desempregadas que procuraram apenas trabalho em "
        "tempo parcial",
        "Número de personas desempleadas que buscaron solo trabajo a tiempo "
        "parcial",
        "",
        "person",
        "Converted from ABS thousands to absolute persons",
        "Unemployed looked for only part-time work ('000)",
    ),
    "weeks_searching": (
        "FLOAT64",
        "Total weeks spent searching for a job by unemployed persons",
        "Total de semanas de procura por trabalho das pessoas desempregadas",
        "Total de semanas de búsqueda de trabajo de las personas desempleadas",
        "",
        "week",
        "Converted from ABS thousands of weeks to weeks",
        "Number of weeks searching for job ('000 Weeks)",
    ),
}

EMP_MEASURES = [
    "employed_full_time",
    "employed_part_time",
    "hours_worked_full_time",
    "hours_worked_part_time",
]

# Per-table overrides: (table, emitted column name) -> {field: value}
OVERRIDE: dict[tuple[str, str], dict[str, str]] = {
    ("employment_industry_hours", "hours_worked_full_time"): {
        "description": "Total hours worked by persons employed full-time, on "
        "the hours concept given by hours_measure",
        "description_pt": "Total de horas trabalhadas por pessoas ocupadas em "
        "tempo integral, no conceito de horas indicado por hours_measure",
        "description_es": "Total de horas trabajadas por personas ocupadas a "
        "tiempo completo, en el concepto de horas indicado por hours_measure",
        "original_name": "Number of hours ... (employed full-time) ('000 Hours)",
    },
    ("employment_industry_hours", "hours_worked_part_time"): {
        "description": "Total hours worked by persons employed part-time, on "
        "the hours concept given by hours_measure",
        "description_pt": "Total de horas trabalhadas por pessoas ocupadas em "
        "tempo parcial, no conceito de horas indicado por hours_measure",
        "description_es": "Total de horas trabajadas por personas ocupadas a "
        "tiempo parcial, en el concepto de horas indicado por hours_measure",
        "original_name": "Number of hours ... (employed part-time) ('000 Hours)",
    },
    ("employment_industry_hours", "hours_band"): {
        "original_name": "Hours usually worked in all jobs / Hours actually "
        "worked in main job / Hours usually worked in main job",
    },
    ("employment_status_hours", "hours_band"): {
        "description": "Range of hours actually worked in all jobs during the "
        "reference week",
        "description_pt": "Faixa de horas efetivamente trabalhadas em todos os "
        "empregos na semana de referência",
        "description_es": "Rango de horas efectivamente trabajadas en todos los "
        "empleos en la semana de referencia",
        "observations": "",
    },
    ("employment_occupation", "age_group"): {
        "observations": "ABS standard age groupings, six broad bands from "
        "15-24 years to 65 years and over.",
    },
    ("unemployment_duration", "age_group"): {
        "description": "Age group in years; total covers persons aged 15 years "
        "and over",
        "description_pt": "Faixa etária em anos; total abrange pessoas com 15 "
        "anos ou mais",
        "description_es": "Grupo de edad en años; total abarca personas de 15 "
        "años o más",
        "observations": "Six broad bands from 15-24 years to 65 years and "
        "over, published for Australia only. The sentinel total carries the "
        "rows broken down by state and territory instead, which ABS publishes "
        "without an age breakdown.",
    },
    ("employment_industry_age", "age_group"): {
        "observations": "ABS standard age groupings, eleven five-year bands "
        "from 15-19 years to 65 years and over.",
    },
    ("employment_occupation_age", "age_group"): {
        "observations": "ABS standard age groupings, eleven five-year bands "
        "from 15-19 years to 65 years and over.",
    },
}

# table -> ordered registry keys
TABLES: dict[str, list[str]] = {
    "employment_industry": [
        "year",
        "quarter",
        "month",
        "geography",
        "industry_division_code",
        "industry_division",
        "industry_group_code",
        "industry_group",
        "sex",
        *EMP_MEASURES,
    ],
    "employment_industry_region": [
        "year",
        "quarter",
        "month",
        "gccsa",
        "industry_division_code",
        "industry_division",
        "sex",
        *EMP_MEASURES,
    ],
    "employment_industry_age": [
        "year",
        "quarter",
        "month",
        "industry_division_code",
        "industry_division",
        "age_group",
        *EMP_MEASURES,
    ],
    "employment_industry_hours": [
        "year",
        "quarter",
        "month",
        "industry_division_code",
        "industry_division",
        "hours_measure",
        "hours_band",
        *EMP_MEASURES,
    ],
    "employment_industry_status": [
        "year",
        "quarter",
        "month",
        "industry_division_code",
        "industry_division",
        "status_in_employment",
        *EMP_MEASURES,
    ],
    "employment_industry_occupation": [
        "year",
        "quarter",
        "month",
        "industry_division_code",
        "industry_division",
        "occupation_major_code",
        "occupation_major",
        "occupation_classification",
        "sex",
        *EMP_MEASURES,
    ],
    "employment_occupation": [
        "year",
        "quarter",
        "month",
        "occupation_sub_major",
        "occupation_classification",
        "sex",
        "age_group",
        *EMP_MEASURES,
    ],
    "employment_occupation_age": [
        "year",
        "quarter",
        "month",
        "occupation_major_code",
        "occupation_major",
        "occupation_classification",
        "age_group",
        *EMP_MEASURES,
    ],
    "employment_occupation_status": [
        "year",
        "quarter",
        "month",
        "occupation_major_code",
        "occupation_major",
        "occupation_classification",
        "status_in_employment",
        *EMP_MEASURES,
    ],
    "employment_status_hours": [
        "year",
        "quarter",
        "month",
        "status_in_employment",
        "hours_band",
        "sex",
        *EMP_MEASURES,
    ],
    "employment_job_tenure": [
        "year",
        "quarter",
        "month",
        "geography",
        "tenure_band",
        "sex",
        *EMP_MEASURES,
    ],
    "unemployment_duration": [
        "year",
        "month_monthly",
        "geography_with_national",
        "duration_band",
        "age_group",
        "unemployed_total",
        "unemployed_looked_for_full_time",
        "unemployed_looked_for_part_time",
        "weeks_searching",
    ],
}

# registry key -> emitted column name
ALIAS = {
    "month_monthly": "month",
    "geography_with_national": "geography",
}

SOURCE_CUBES = {
    "employment_industry": ["EQ06"],
    "employment_industry_region": ["EQ03"],
    "employment_industry_age": ["EQ12"],
    "employment_industry_hours": ["EQ10", "EQ11", "EQ14"],
    "employment_industry_status": ["EQ05"],
    "employment_industry_occupation": ["EQ09"],
    "employment_occupation": ["EQ07a"],
    "employment_occupation_age": ["EQ13"],
    "employment_occupation_status": ["EQ07b"],
    "employment_status_hours": ["EQ04"],
    "employment_job_tenure": ["EQ02"],
    "unemployment_duration": ["UM2", "UM3"],
}

# Grain for dbt_utils.unique_combination_of_columns (emitted names).
GRAIN = {
    "employment_industry": [
        "year",
        "month",
        "geography",
        "industry_group_code",
        "sex",
    ],
    "employment_industry_region": [
        "year",
        "month",
        "gccsa",
        "industry_division_code",
        "sex",
    ],
    "employment_industry_age": [
        "year",
        "month",
        "industry_division_code",
        "age_group",
    ],
    "employment_industry_hours": [
        "year",
        "month",
        "industry_division_code",
        "hours_measure",
        "hours_band",
    ],
    "employment_industry_status": [
        "year",
        "month",
        "industry_division_code",
        "status_in_employment",
    ],
    "employment_industry_occupation": [
        "year",
        "month",
        "industry_division_code",
        "occupation_major_code",
        "sex",
    ],
    "employment_occupation": [
        "year",
        "month",
        "occupation_sub_major",
        "sex",
        "age_group",
    ],
    "employment_occupation_age": [
        "year",
        "month",
        "occupation_major_code",
        "age_group",
    ],
    "employment_occupation_status": [
        "year",
        "month",
        "occupation_major_code",
        "status_in_employment",
    ],
    "employment_status_hours": [
        "year",
        "month",
        "status_in_employment",
        "hours_band",
        "sex",
    ],
    "employment_job_tenure": [
        "year",
        "month",
        "geography",
        "tenure_band",
        "sex",
    ],
    "unemployment_duration": [
        "year",
        "month",
        "geography",
        "duration_band",
        "age_group",
    ],
}


def field(table: str, key: str, which: str) -> str:
    """Resolve one architecture field, applying per-table overrides."""
    btype, d_en, d_pt, d_es, directory, unit, obs, original = C[key]
    name = ALIAS.get(key, key)
    base = {
        "name": name,
        "bigquery_type": btype,
        "description": d_en,
        "description_pt": d_pt,
        "description_es": d_es,
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations": obs,
        "original_name": original,
    }
    base.update(OVERRIDE.get((table, name), {}))
    return base[which]


def rows(table: str) -> list[dict[str, str]]:
    """Architecture rows for one table, in emitted column order."""
    return [
        {h: field(table, key, h) for h in ARCH_HEADER} for key in TABLES[table]
    ]


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        path = OUT / f"{table}.csv"
        with path.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=ARCH_HEADER)
            w.writeheader()
            w.writerows(rows(table))
        print(f"wrote {path}  ({len(TABLES[table])} columns)")


if __name__ == "__main__":
    main()
