"""Generate the architecture CSVs for au_abs_migration.

The architecture table is the source of truth for column names, types, order
and descriptions. Types follow arithmetic meaning: only genuine quantities
(counts of people) are numeric; every code and identifier is STRING.

Phase 1 covers the Overseas Migration release (ABS cat. 3407.0) and the
interstate migration series from National, state and territory population
(cat. 3101.0). Sub-state (regional) migration is a later phase.
"""

from __future__ import annotations

import csv
from pathlib import Path

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
    name: str,
    btype: str,
    pt: str,
    en: str,
    es: str,
    *,
    coverage: str = "",
    dictionary: str = "no",
    directory: str = "",
    unit: str = "",
    sensitive: str = "no",
    observations: str = "",
    original: str = "",
) -> dict[str, str]:
    return {
        "name": name,
        "bigquery_type": btype,
        "description": pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": sensitive,
        "observations": observations,
        "original_name": original,
        "description_en": en,
        "description_es": es,
    }


# --------------------------------------------------------------------------
# shared columns
# --------------------------------------------------------------------------


def year_financial() -> dict[str, str]:
    return col(
        "year",
        "INT64",
        "Ano inicial do exercício fiscal australiano a que se referem os dados; "
        "2004 corresponde ao exercício 2004-05, encerrado em 30 de junho de 2005",
        "Start year of the Australian financial year the data refer to; 2004 denotes "
        "the 2004-05 year, ending 30 June 2005",
        "Año inicial del ejercicio fiscal australiano al que se refieren los datos; "
        "2004 corresponde al ejercicio 2004-05, cerrado el 30 de junio de 2005",
        directory="br_bd_diretorios_data_tempo.ano:ano",
        unit="year",
        observations="Partition column",
        original="TIME_PERIOD",
    )


def year_calendar() -> dict[str, str]:
    return col(
        "year",
        "INT64",
        "Ano civil de referência dos dados",
        "Reference calendar year of the data",
        "Año calendario de referencia de los datos",
        directory="br_bd_diretorios_data_tempo.ano:ano",
        unit="year",
        observations="Partition column",
        original="TIME_PERIOD",
    )


def quarter() -> dict[str, str]:
    return col(
        "quarter",
        "INT64",
        "Trimestre civil de referência, de 1 a 4",
        "Reference calendar quarter, 1 to 4",
        "Trimestre calendario de referencia, de 1 a 4",
        unit="quarter",
        observations="Derived from the SDMX period label; 2006-Q3 is the July-September 2006 quarter",
        original="TIME_PERIOD",
    )


def state_id() -> dict[str, str]:
    return col(
        "state_id",
        "STRING",
        "Código do estado ou território australiano de residência",
        "Code of the Australian state or territory of residence",
        "Código del estado o territorio australiano de residencia",
        directory="diretorios_au.state:id_state",
        observations=(
            "ASGS state and territory code (1 New South Wales to 8 Australian Capital "
            "Territory). The national aggregate lives in the companion _australia table, "
            "because ABS rounds every value to the nearest 10 and the states therefore do "
            "not sum to the published Australia figure"
        ),
        original="REGION",
    )


def country_of_birth_id() -> dict[str, str]:
    return col(
        "country_of_birth_id",
        "STRING",
        "Código do país de nascimento na Standard Australian Classification of "
        "Countries (SACC) 2016, com quatro dígitos",
        "Four-digit country of birth code in the Standard Australian Classification of "
        "Countries (SACC) 2016",
        "Código de cuatro dígitos del país de nacimiento en la Standard Australian "
        "Classification of Countries (SACC) 2016",
        dictionary="yes",
        observations=(
            "Codes ending in 99 are residual 'not elsewhere classified' categories and "
            "0000 is 'Inadequately Described'; neither is a country"
        ),
        original="SACC code",
    )


def country_iso3_code() -> dict[str, str]:
    return col(
        "country_iso3_code",
        "STRING",
        "Código ISO 3166-1 alfa-3 do país de nascimento",
        "ISO 3166-1 alpha-3 code of the country of birth",
        "Código ISO 3166-1 alfa-3 del país de nacimiento",
        directory="diretorios_mundo.pais:sigla_pais_iso3",
        observations=(
            "Links to the country directory, whose backend key is spelled "
            "sigla_pais_iso3 although the BigQuery column is sigla_iso3. Resolved "
            "from the SACC code during cleaning. Null for the residual SACC "
            "categories, for Kosovo and Spanish North Africa (no ISO 3166-1 code), and "
            "for SACC 2100, which spans the United Kingdom together with the Channel "
            "Islands and the Isle of Man. The seven Antarctic territorial claims all "
            "resolve to ATA"
        ),
    )


def age_group(*, nom: bool = True) -> dict[str, str]:
    extra = (
        "ABS five-year age groups; TOT is all ages. Beware the code A59, which denotes "
        "5-9 years, not 59"
    )
    if not nom:
        extra += ". The interstate series carries finer groups at the top of the distribution"
    return col(
        "age_group",
        "STRING",
        "Código do grupo etário do migrante",
        "Code of the migrant's age group",
        "Código del grupo de edad del migrante",
        dictionary="yes",
        observations=extra,
        original="AGE",
    )


def sex() -> dict[str, str]:
    return col(
        "sex",
        "STRING",
        "Código do sexo do migrante",
        "Code of the migrant's sex",
        "Código del sexo del migrante",
        dictionary="yes",
        observations="1 males, 2 females, 3 persons (the total)",
        original="SEX",
    )


def visa_group_id() -> dict[str, str]:
    return col(
        "visa_group_id",
        "STRING",
        "Código do grupo de visto ou de cidadania do migrante no momento da migração",
        "Code of the migrant's visa or citizenship group at the time of migration",
        "Código del grupo de visado o de ciudadanía del migrante en el momento de la migración",
        dictionary="yes",
        observations=(
            "ABS visa and citizenship groupings. Codes 1020, 1040 and 1041 are aggregates "
            "(temporary total, permanent total and overall total). These are counts of "
            "migrations by visa held, not counts of visas granted by the Department of "
            "Home Affairs"
        ),
        original="VISA",
    )


def measure(
    name: str, pt: str, en: str, es: str, observations: str
) -> dict[str, str]:
    return col(
        name, "INT64", pt, en, es, unit="person", observations=observations
    )


ROUNDED = (
    "Rounded by ABS to the nearest 10 to preserve confidentiality, so components do not "
    "always add to totals"
)

OVERSEAS_ARRIVALS = measure(
    "arrivals",
    "Número de chegadas de migrantes do exterior",
    "Number of overseas migrant arrivals",
    "Número de llegadas de migrantes del exterior",
    ROUNDED,
)
OVERSEAS_DEPARTURES = measure(
    "departures",
    "Número de partidas de migrantes para o exterior",
    "Number of overseas migrant departures",
    "Número de salidas de migrantes hacia el exterior",
    ROUNDED,
)
OVERSEAS_NET = measure(
    "net",
    "Migração líquida internacional: chegadas menos partidas",
    "Net overseas migration: arrivals minus departures",
    "Migración neta internacional: llegadas menos salidas",
    ROUNDED + ". Can be negative",
)
INTERSTATE_ARRIVALS = measure(
    "arrivals",
    "Número de chegadas de migrantes de outros estados ou territórios",
    "Number of interstate migrant arrivals",
    "Número de llegadas de migrantes de otros estados o territorios",
    ROUNDED,
)
INTERSTATE_DEPARTURES = measure(
    "departures",
    "Número de partidas de migrantes para outros estados ou territórios",
    "Number of interstate migrant departures",
    "Número de salidas de migrantes hacia otros estados o territorios",
    ROUNDED,
)
INTERSTATE_NET = measure(
    "net",
    "Migração líquida interestadual: chegadas menos partidas",
    "Net interstate migration: arrivals minus departures",
    "Migración neta interestatal: llegadas menos salidas",
    ROUNDED + ". Can be negative",
)


def dicionario() -> list[dict[str, str]]:
    return [
        col(
            "id_tabela",
            "STRING",
            "Slug da tabela de au_abs_migration descrita pela entrada",
            "Slug of the au_abs_migration table the entry describes",
            "Slug de la tabla de au_abs_migration que describe la entrada",
        ),
        col(
            "nome_coluna",
            "STRING",
            "Nome da coluna descrita pela entrada",
            "Name of the column the entry describes",
            "Nombre de la columna que describe la entrada",
        ),
        col(
            "chave",
            "STRING",
            "Valor da chave, conforme armazenado na coluna",
            "Key value, as stored in the column",
            "Valor de la clave, tal como se almacena en la columna",
        ),
        col(
            "cobertura_temporal",
            "STRING",
            "Cobertura temporal da entrada",
            "Temporal coverage of the entry",
            "Cobertura temporal de la entrada",
        ),
        col(
            "valor",
            "STRING",
            "Descrição da chave",
            "Description of the key",
            "Descripción de la clave",
        ),
    ]


TABLES: dict[str, list[dict[str, str]]] = {
    # --- overseas migration, country of birth (spreadsheets 001-003) -------
    "overseas_country_of_birth_australia": [
        year_financial(),
        country_of_birth_id(),
        country_iso3_code(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
        OVERSEAS_NET,
    ],
    "overseas_country_of_birth_state": [
        year_financial(),
        state_id(),
        country_of_birth_id(),
        country_iso3_code(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
        OVERSEAS_NET,
    ],
    # --- overseas migration, age and sex (NOM_FY / NOM_CY) ----------------
    "overseas_age_sex_australia": [
        year_financial(),
        age_group(),
        sex(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
        OVERSEAS_NET,
    ],
    "overseas_age_sex_state": [
        year_financial(),
        state_id(),
        age_group(),
        sex(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
        OVERSEAS_NET,
    ],
    "overseas_age_sex_australia_calendar_year": [
        year_calendar(),
        age_group(),
        sex(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
        OVERSEAS_NET,
    ],
    "overseas_age_sex_state_calendar_year": [
        year_calendar(),
        state_id(),
        age_group(),
        sex(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
        OVERSEAS_NET,
    ],
    # --- overseas migration, visa groups (spreadsheet 004 / OMAD_VISA) ----
    "overseas_visa_australia": [
        year_financial(),
        visa_group_id(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
    ],
    "overseas_visa_state": [
        year_financial(),
        state_id(),
        visa_group_id(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
    ],
    "overseas_visa_quarter_australia": [
        year_calendar(),
        quarter(),
        visa_group_id(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
    ],
    "overseas_visa_quarter_state": [
        year_calendar(),
        quarter(),
        state_id(),
        visa_group_id(),
        OVERSEAS_ARRIVALS,
        OVERSEAS_DEPARTURES,
    ],
    # --- interstate migration (NIM_FY / NIM_CY) ---------------------------
    "interstate_age_sex_australia": [
        year_financial(),
        age_group(nom=False),
        sex(),
        INTERSTATE_ARRIVALS,
        INTERSTATE_DEPARTURES,
        INTERSTATE_NET,
    ],
    "interstate_age_sex_state": [
        year_financial(),
        state_id(),
        age_group(nom=False),
        sex(),
        INTERSTATE_ARRIVALS,
        INTERSTATE_DEPARTURES,
        INTERSTATE_NET,
    ],
    "interstate_age_sex_australia_calendar_year": [
        year_calendar(),
        age_group(nom=False),
        sex(),
        INTERSTATE_ARRIVALS,
        INTERSTATE_DEPARTURES,
        INTERSTATE_NET,
    ],
    "interstate_age_sex_state_calendar_year": [
        year_calendar(),
        state_id(),
        age_group(nom=False),
        sex(),
        INTERSTATE_ARRIVALS,
        INTERSTATE_DEPARTURES,
        INTERSTATE_NET,
    ],
    "dicionario": dicionario(),
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for table, columns in TABLES.items():
        path = OUT / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=FIELDS, lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(columns)
        print(f"{path.name}: {len(columns)} columns")


if __name__ == "__main__":
    main()
