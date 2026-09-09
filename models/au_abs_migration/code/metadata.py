"""Register au_abs_migration metadata in the Data Basis backend.

Idempotent by construction: every record is looked up before it is written and
its id passed back, because create_update_* duplicates observation levels,
cloud tables, coverages and updates when called without one.

The dataset is created as ``under_review``; publishing is a separate,
post-merge action.

Run:  python metadata.py [--env staging]
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)

import server

CODE = Path(__file__).resolve().parent
ARCH = CODE / "architecture"

DATASET_SLUG = "au_abs_migration"
GCP_DATASET_ID = "au_abs_migration"
GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}

DATASET = {
    "name_pt": "Migração Internacional e Interestadual (Austrália)",
    "name_en": "Overseas and Interstate Migration (Australia)",
    "name_es": "Migración Internacional e Interestatal (Australia)",
    "description_pt": (
        "Estatísticas de migração da Austrália produzidas pelo Australian Bureau of "
        "Statistics: migração internacional por país de nascimento, idade, sexo e grupo "
        "de visto, e migração interestadual por idade e sexo. Cobre exercícios fiscais "
        "e anos civis desde 1996-97 (interestadual) e 2004-05 (internacional), além de "
        "uma série trimestral por grupo de visto desde o terceiro trimestre de 2006. "
        "As tabelas separam o agregado nacional dos estados e territórios, porque o ABS "
        "arredonda cada valor para a dezena mais próxima e as unidades não somam o total "
        "publicado."
    ),
    "description_en": (
        "Australian migration statistics produced by the Australian Bureau of "
        "Statistics: overseas migration by country of birth, age, sex and visa group, "
        "and interstate migration by age and sex. Covers financial and calendar years "
        "from 1996-97 (interstate) and 2004-05 (overseas), plus a quarterly visa series "
        "from the September quarter of 2006. National and state figures sit in separate "
        "tables, because ABS rounds every value to the nearest 10 and the states do not "
        "sum to the published national total."
    ),
    "description_es": (
        "Estadísticas de migración de Australia producidas por la Australian Bureau of "
        "Statistics: migración internacional por país de nacimiento, edad, sexo y grupo "
        "de visado, y migración interestatal por edad y sexo. Cubre ejercicios fiscales "
        "y años calendario desde 1996-97 (interestatal) y 2004-05 (internacional), más "
        "una serie trimestral por grupo de visado desde el tercer trimestre de 2006. "
        "Las tablas separan el agregado nacional de los estados y territorios, porque la "
        "ABS redondea cada valor a la decena más próxima y las unidades no suman el total "
        "publicado."
    ),
    "organization_slugs": ["abs"],
    "theme_slugs": ["population", "government"],
    # One entry per concept, English slug first. The vocabularies diverge by
    # backend — prod carries migration/demographics/citizenship/mobility, while
    # the older staging clone still has the Portuguese slugs — so each concept
    # lists its known spellings and the first that exists wins.
    "tag_slugs": [
        ("migration", "migracao"),
        ("demographics", "demografia"),
        ("citizenship", "cidadania"),
        ("mobility", "mobilidade"),
        ("visa",),
    ],
}

# Trilingual names for a tag that has to be created, keyed by its English slug.
NEW_TAGS = {
    "migration": ("migração", "migration", "migración"),
    "demographics": ("demografia", "demographics", "demografía"),
    "citizenship": ("cidadania", "citizenship", "ciudadanía"),
    "mobility": ("mobilidade", "mobility", "movilidad"),
    "visa": ("visto", "visa", "visado"),
}

SPREADSHEET_SOURCE = "spreadsheets"
API_SOURCE = "api"

RAW_SOURCES = {
    SPREADSHEET_SOURCE: {
        "name_pt": "Migração Internacional — planilhas de séries temporais",
        "name_en": "Overseas Migration — time series spreadsheets",
        "name_es": "Migración Internacional — planillas de series temporales",
        "url": "https://www.abs.gov.au/statistics/people/population/overseas-migration/latest-release",
        "description_pt": (
            "Planilhas anuais da divulgação Overseas Migration (cat. 3407.0), com "
            "migração líquida, chegadas e partidas por país de nascimento e por grupo de "
            "visto, para a Austrália e cada estado ou território."
        ),
        "description_en": (
            "Annual spreadsheets from the Overseas Migration release (cat. 3407.0), "
            "carrying net migration, arrivals and departures by country of birth and by "
            "visa group, for Australia and each state and territory."
        ),
        "description_es": (
            "Planillas anuales de la publicación Overseas Migration (cat. 3407.0), con "
            "migración neta, llegadas y salidas por país de nacimiento y por grupo de "
            "visado, para Australia y cada estado o territorio."
        ),
        "contains_api": False,
    },
    API_SOURCE: {
        "name_pt": "ABS Data API (SDMX)",
        "name_en": "ABS Data API (SDMX)",
        "name_es": "ABS Data API (SDMX)",
        "url": "https://data.api.abs.gov.au/rest",
        "description_pt": (
            "API SDMX do ABS. Fornece os fluxos NOM_FY, NOM_CY, OMAD_VISA, NIM_FY e "
            "NIM_CY, com o detalhe por idade, sexo e trimestre ausente das planilhas."
        ),
        "description_en": (
            "The ABS SDMX API. Serves the NOM_FY, NOM_CY, OMAD_VISA, NIM_FY and NIM_CY "
            "dataflows, with the age, sex and quarterly detail the spreadsheets omit."
        ),
        "description_es": (
            "API SDMX de la ABS. Entrega los flujos NOM_FY, NOM_CY, OMAD_VISA, NIM_FY y "
            "NIM_CY, con el detalle por edad, sexo y trimestre ausente de las planillas."
        ),
        "contains_api": True,
    },
}

ROUNDED_PT = (
    "Os valores são arredondados pelo ABS para a dezena mais próxima, de modo que os "
    "componentes nem sempre somam os totais."
)
ROUNDED_EN = "ABS rounds every value to the nearest 10, so components do not always add to totals."
ROUNDED_ES = (
    "La ABS redondea cada valor a la decena más próxima, de modo que los componentes no "
    "siempre suman los totales."
)


def table(
    slug: str,
    names: tuple[str, str, str],
    descriptions: tuple[str, str, str],
    observation_levels: list[str],
    coverage: tuple[int, int | None, int, int | None],
    source: str,
    update: tuple[str, int, int | None],
) -> dict:
    return {
        "slug": slug,
        "name_pt": names[0],
        "name_en": names[1],
        "name_es": names[2],
        "description_pt": f"{descriptions[0]} {ROUNDED_PT}",
        "description_en": f"{descriptions[1]} {ROUNDED_EN}",
        "description_es": f"{descriptions[2]} {ROUNDED_ES}",
        "observation_levels": observation_levels,
        "coverage": coverage,
        "source": source,
        "update": update,
    }


ANNUAL = ("year", 1, 1)
QUARTERLY = ("quarter", 1, 1)

TABLES = [
    table(
        "overseas_country_of_birth_australia",
        (
            "Migração internacional por país de nascimento, Austrália",
            "Overseas migration by country of birth, Australia",
            "Migración internacional por país de nacimiento, Australia",
        ),
        (
            "Chegadas, partidas e migração líquida internacional da Austrália, por país "
            "de nascimento e exercício fiscal.",
            "Overseas arrivals, departures and net migration for Australia, by country "
            "of birth and financial year.",
            "Llegadas, salidas y migración neta internacional de Australia, por país de "
            "nacimiento y ejercicio fiscal.",
        ),
        ["year", "country"],
        (2004, None, 2024, None),
        SPREADSHEET_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_country_of_birth_state",
        (
            "Migração internacional por país de nascimento e estado",
            "Overseas migration by country of birth and state",
            "Migración internacional por país de nacimiento y estado",
        ),
        (
            "Chegadas, partidas e migração líquida internacional por país de nascimento, "
            "estado ou território de residência e exercício fiscal.",
            "Overseas arrivals, departures and net migration by country of birth, state "
            "or territory of residence and financial year.",
            "Llegadas, salidas y migración neta internacional por país de nacimiento, "
            "estado o territorio de residencia y ejercicio fiscal.",
        ),
        ["year", "state", "country"],
        (2004, None, 2024, None),
        SPREADSHEET_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_age_sex_australia",
        (
            "Migração internacional por idade e sexo, Austrália",
            "Overseas migration by age and sex, Australia",
            "Migración internacional por edad y sexo, Australia",
        ),
        (
            "Chegadas, partidas e migração líquida internacional da Austrália, por grupo "
            "etário, sexo e exercício fiscal.",
            "Overseas arrivals, departures and net migration for Australia, by age "
            "group, sex and financial year.",
            "Llegadas, salidas y migración neta internacional de Australia, por grupo de "
            "edad, sexo y ejercicio fiscal.",
        ),
        ["year", "age", "sex"],
        (2004, None, 2024, None),
        API_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_age_sex_state",
        (
            "Migração internacional por idade, sexo e estado",
            "Overseas migration by age, sex and state",
            "Migración internacional por edad, sexo y estado",
        ),
        (
            "Chegadas, partidas e migração líquida internacional por grupo etário, sexo, "
            "estado ou território de residência e exercício fiscal.",
            "Overseas arrivals, departures and net migration by age group, sex, state or "
            "territory of residence and financial year.",
            "Llegadas, salidas y migración neta internacional por grupo de edad, sexo, "
            "estado o territorio de residencia y ejercicio fiscal.",
        ),
        ["year", "state", "age", "sex"],
        (2004, None, 2024, None),
        API_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_age_sex_australia_calendar_year",
        (
            "Migração internacional por idade e sexo, Austrália, anos civis",
            "Overseas migration by age and sex, Australia, calendar years",
            "Migración internacional por edad y sexo, Australia, años civiles",
        ),
        (
            "Mesma estatística da tabela por exercício fiscal, recortada por ano civil.",
            "The same statistic as the financial-year table, cut by calendar year.",
            "La misma estadística de la tabla por ejercicio fiscal, recortada por año "
            "calendario.",
        ),
        ["year", "age", "sex"],
        (2004, None, 2024, None),
        API_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_age_sex_state_calendar_year",
        (
            "Migração internacional por idade, sexo e estado, anos civis",
            "Overseas migration by age, sex and state, calendar years",
            "Migración internacional por edad, sexo y estado, años civiles",
        ),
        (
            "Mesma estatística da tabela estadual por exercício fiscal, recortada por "
            "ano civil.",
            "The same statistic as the state financial-year table, cut by calendar year.",
            "La misma estadística de la tabla estatal por ejercicio fiscal, recortada por "
            "año calendario.",
        ),
        ["year", "state", "age", "sex"],
        (2004, None, 2024, None),
        API_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_visa_australia",
        (
            "Migração internacional por grupo de visto, Austrália",
            "Overseas migration by visa group, Australia",
            "Migración internacional por grupo de visado, Australia",
        ),
        (
            "Chegadas e partidas de migrantes internacionais da Austrália por grupo de "
            "visto ou de cidadania e exercício fiscal. Contabiliza migrações segundo o "
            "visto detido no momento do deslocamento, e não vistos concedidos.",
            "Overseas migrant arrivals and departures for Australia by visa or "
            "citizenship group and financial year. Counts migrations by the visa held at "
            "the time of travel, not visas granted.",
            "Llegadas y salidas de migrantes internacionales de Australia por grupo de "
            "visado o de ciudadanía y ejercicio fiscal. Contabiliza migraciones según el "
            "visado vigente al momento del desplazamiento, no visados otorgados.",
        ),
        ["year"],
        (2004, None, 2024, None),
        SPREADSHEET_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_visa_state",
        (
            "Migração internacional por grupo de visto e estado",
            "Overseas migration by visa group and state",
            "Migración internacional por grupo de visado y estado",
        ),
        (
            "Chegadas e partidas de migrantes internacionais por grupo de visto ou de "
            "cidadania, estado ou território de residência e exercício fiscal.",
            "Overseas migrant arrivals and departures by visa or citizenship group, "
            "state or territory of residence and financial year.",
            "Llegadas y salidas de migrantes internacionales por grupo de visado o de "
            "ciudadanía, estado o territorio de residencia y ejercicio fiscal.",
        ),
        ["year", "state"],
        (2004, None, 2024, None),
        SPREADSHEET_SOURCE,
        ANNUAL,
    ),
    table(
        "overseas_visa_quarter_australia",
        (
            "Migração internacional por grupo de visto, trimestral, Austrália",
            "Overseas migration by visa group, quarterly, Australia",
            "Migración internacional por grupo de visado, trimestral, Australia",
        ),
        (
            "Chegadas e partidas de migrantes internacionais da Austrália por grupo de "
            "visto ou de cidadania, em trimestres civis. O ABS revisa esta série a cada "
            "divulgação trimestral da população, de modo que o ano preliminar pode "
            "divergir das tabelas anuais.",
            "Overseas migrant arrivals and departures for Australia by visa or "
            "citizenship group, in calendar quarters. ABS revises this series with each "
            "quarterly population release, so the preliminary year can differ from the "
            "annual tables.",
            "Llegadas y salidas de migrantes internacionales de Australia por grupo de "
            "visado o de ciudadanía, en trimestres calendario. La ABS revisa esta serie "
            "en cada publicación trimestral de población, por lo que el año preliminar "
            "puede diferir de las tablas anuales.",
        ),
        ["year", "quarter"],
        (2006, 7, 2025, 12),
        API_SOURCE,
        QUARTERLY,
    ),
    table(
        "overseas_visa_quarter_state",
        (
            "Migração internacional por grupo de visto e estado, trimestral",
            "Overseas migration by visa group and state, quarterly",
            "Migración internacional por grupo de visado y estado, trimestral",
        ),
        (
            "Chegadas e partidas de migrantes internacionais por grupo de visto ou de "
            "cidadania e estado ou território de residência, em trimestres civis.",
            "Overseas migrant arrivals and departures by visa or citizenship group and "
            "state or territory of residence, in calendar quarters.",
            "Llegadas y salidas de migrantes internacionales por grupo de visado o de "
            "ciudadanía y estado o territorio de residencia, en trimestres calendario.",
        ),
        ["year", "quarter", "state"],
        (2006, 7, 2025, 12),
        API_SOURCE,
        QUARTERLY,
    ),
    table(
        "interstate_age_sex_australia",
        (
            "Migração interestadual por idade e sexo, Austrália",
            "Interstate migration by age and sex, Australia",
            "Migración interestatal por edad y sexo, Australia",
        ),
        (
            "Chegadas, partidas e migração líquida interestadual da Austrália, por grupo "
            "etário, sexo e exercício fiscal. O agregado nacional corresponde ao total "
            "de deslocamentos entre estados e territórios.",
            "Interstate arrivals, departures and net migration for Australia, by age "
            "group, sex and financial year. The national aggregate is the total of moves "
            "between states and territories.",
            "Llegadas, salidas y migración neta interestatal de Australia, por grupo de "
            "edad, sexo y ejercicio fiscal. El agregado nacional corresponde al total de "
            "desplazamientos entre estados y territorios.",
        ),
        ["year", "age", "sex"],
        (1996, None, 2024, None),
        API_SOURCE,
        ANNUAL,
    ),
    table(
        "interstate_age_sex_state",
        (
            "Migração interestadual por idade, sexo e estado",
            "Interstate migration by age, sex and state",
            "Migración interestatal por edad, sexo y estado",
        ),
        (
            "Chegadas, partidas e migração líquida interestadual por grupo etário, sexo, "
            "estado ou território e exercício fiscal.",
            "Interstate arrivals, departures and net migration by age group, sex, state "
            "or territory and financial year.",
            "Llegadas, salidas y migración neta interestatal por grupo de edad, sexo, "
            "estado o territorio y ejercicio fiscal.",
        ),
        ["year", "state", "age", "sex"],
        (1996, None, 2024, None),
        API_SOURCE,
        ANNUAL,
    ),
    table(
        "interstate_age_sex_australia_calendar_year",
        (
            "Migração interestadual por idade e sexo, Austrália, anos civis",
            "Interstate migration by age and sex, Australia, calendar years",
            "Migración interestatal por edad y sexo, Australia, años civiles",
        ),
        (
            "Mesma estatística da tabela por exercício fiscal, recortada por ano civil.",
            "The same statistic as the financial-year table, cut by calendar year.",
            "La misma estadística de la tabla por ejercicio fiscal, recortada por año "
            "calendario.",
        ),
        ["year", "age", "sex"],
        (1997, None, 2025, None),
        API_SOURCE,
        ANNUAL,
    ),
    table(
        "interstate_age_sex_state_calendar_year",
        (
            "Migração interestadual por idade, sexo e estado, anos civis",
            "Interstate migration by age, sex and state, calendar years",
            "Migración interestatal por edad, sexo y estado, años civiles",
        ),
        (
            "Mesma estatística da tabela estadual por exercício fiscal, recortada por "
            "ano civil. Inclui os Territórios Externos (código 9), ausentes da série por "
            "exercício fiscal.",
            "The same statistic as the state financial-year table, cut by calendar year. "
            "Includes Other Territories (code 9), which the financial-year series omits.",
            "La misma estadística de la tabla estatal por ejercicio fiscal, recortada por "
            "año calendario. Incluye los Territorios Externos (código 9), ausentes de la "
            "serie por ejercicio fiscal.",
        ),
        ["year", "state", "age", "sex"],
        (1997, None, 2025, None),
        API_SOURCE,
        ANNUAL,
    ),
]

DICIONARIO = {
    "slug": "dicionario",
    "name_pt": "Dicionário",
    "name_en": "Dictionary",
    "name_es": "Diccionario",
    "description_pt": (
        "Dicionário dos valores codificados usados nas tabelas de au_abs_migration: país "
        "de nascimento (SACC), grupo etário, sexo e grupo de visto."
    ),
    "description_en": (
        "Dictionary of the coded values used across the au_abs_migration tables: country "
        "of birth (SACC), age group, sex and visa group."
    ),
    "description_es": (
        "Diccionario de los valores codificados usados en las tablas de au_abs_migration: "
        "país de nacimiento (SACC), grupo de edad, sexo y grupo de visado."
    ),
    "observation_levels": [],
    "coverage": None,
    "source": None,
    "update": None,
}

# The country directory's key column is spelled differently on each backend:
# prod agrees with BigQuery (sigla_iso3), while the older staging clone still
# calls it sigla_pais_iso3. A link that names the wrong one is accepted with no
# error and leaves the foreign key null, so it has to be rewritten per env.
DIRECTORY_BY_ENV = {
    "staging": {
        "diretorios_mundo.pais:sigla_iso3": "diretorios_mundo.pais:sigla_pais_iso3",
    },
}

# column name -> observation level entity slug
COLUMN_ENTITY = {
    "year": "year",
    "quarter": "quarter",
    "state_id": "state",
    "country_of_birth_id": "country",
    "age_group": "age",
    "sex": "sex",
}


def architecture(table_slug: str) -> list[dict]:
    with (ARCH / f"{table_slug}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def columns_payload(table_slug: str, env: str) -> str:
    """Architecture rows as the bulk_upsert_columns payload.

    Directory links are rewritten to the backend dataset slug: the backend
    resolves `diretorios_au.state:id_state`, not the GCP dataset id.
    """
    payload = []
    for row in architecture(table_slug):
        directory = row["directory_column"].replace(
            "br_bd_diretorios_", "diretorios_"
        )
        payload.append(
            {
                "name": row["name"],
                "bigquery_type": row["bigquery_type"],
                "description_pt": row["description"],
                "description_en": row["description_en"],
                "description_es": row["description_es"],
                "covered_by_dictionary": row["covered_by_dictionary"] == "yes",
                "directory_column": directory,
                "measurement_unit": row["measurement_unit"],
                "has_sensitive_data": row["has_sensitive_data"] == "yes",
                "observations_en": row["observations"],
            }
        )
    return json.dumps(payload, ensure_ascii=False)


def lookup(category: str, slug: str, env: str) -> str:
    result = server.lookup_id(category=category, slug=slug, env=env)
    if isinstance(result, dict):
        for key in ("id", "value"):
            if key in result:
                return server._strip_id(str(result[key]))
        raise RuntimeError(
            f"{category}/{slug}: unexpected lookup response {result}"
        )
    return server._strip_id(str(result))


def existing_state(env: str) -> dict:
    """Everything already registered for this dataset, keyed for reuse."""
    query = """
    query($slug: String) {
      allDataset(slug: $slug) {
        edges { node { id
          rawDataSources { edges { node { id name url } } }
          tables { edges { node { id slug
            observationLevels { edges { node { id entity { slug } } } }
            cloudTables { edges { node { id } } }
            coverages { edges { node { id datetimeRanges { edges { node { id } } } } } }
            updates { edges { node { id } } }
          } } }
        } }
      }
    }
    """
    data = server._gql(query, {"slug": DATASET_SLUG}, env=env)
    edges = data["allDataset"]["edges"]
    if not edges:
        return {}
    node = edges[0]["node"]
    tables = {}
    for edge in node["tables"]["edges"]:
        item = edge["node"]
        tables[item["slug"]] = {
            "id": server._strip_id(item["id"]),
            "observation_levels": {
                level["node"]["entity"]["slug"]: server._strip_id(
                    level["node"]["id"]
                )
                for level in item["observationLevels"]["edges"]
                if level["node"]["entity"]
            },
            "cloud_table": next(
                (
                    server._strip_id(c["node"]["id"])
                    for c in item["cloudTables"]["edges"]
                ),
                None,
            ),
            "coverage": next(
                (
                    server._strip_id(c["node"]["id"])
                    for c in item["coverages"]["edges"]
                ),
                None,
            ),
            "datetime_range": next(
                (
                    server._strip_id(r["node"]["id"])
                    for c in item["coverages"]["edges"]
                    for r in c["node"]["datetimeRanges"]["edges"]
                ),
                None,
            ),
            "update": next(
                (
                    server._strip_id(u["node"]["id"])
                    for u in item["updates"]["edges"]
                ),
                None,
            ),
        }
    return {
        "id": server._strip_id(node["id"]),
        "raw_data_sources": {
            edge["node"]["url"]: server._strip_id(edge["node"]["id"])
            for edge in node["rawDataSources"]["edges"]
        },
        "tables": tables,
    }


def resolve_tags(env: str) -> list[str]:
    """One id per tag concept, creating the tag only when no spelling exists.

    The vocabularies diverge by backend, so each concept lists its known slugs
    and the first that resolves wins; a concept with no match is created under
    its English slug.
    """
    ids = []
    for candidates in DATASET["tag_slugs"]:
        found = None
        for slug in candidates:
            try:
                found = lookup("tag", slug, env)
                break
            except Exception:
                continue
        if found is None:
            slug = candidates[0]
            name_pt, name_en, name_es = NEW_TAGS[slug]
            created = server.create_update_tag(
                slug=slug,
                name_pt=name_pt,
                name_en=name_en,
                name_es=name_es,
                env=env,
            )
            print(f"  created tag {slug}")
            found = server._strip_id(str(created.get("id", created)))
        ids.append(found)
    return ids


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="staging")
    parser.add_argument("--tables", nargs="*", default=None)
    parser.add_argument(
        "--publish",
        action="store_true",
        help=(
            "Set the dataset status to published. Safe on dev/staging, whose frontend "
            "is not the public site; on prod, only after the PR has merged, the "
            "table-approve action has run and the live tables are verified."
        ),
    )
    args = parser.parse_args()
    env = args.env
    # The backend stores Update.latest as a DateTime; a bare date is rejected.
    today = dt.datetime.now().replace(microsecond=0).isoformat()

    state = existing_state(env)
    account = server.get_authenticated_account(env=env)
    account_id = server._strip_id(str(account.get("id", account)))
    entities = {
        slug: lookup("entity", slug, env)
        for slug in {"year", "quarter", "state", "country", "age", "sex"}
    }
    area_id = lookup("area", "au", env)

    dataset_id = server._strip_id(
        str(
            server.create_update_dataset(
                slug=DATASET_SLUG,
                name_pt=DATASET["name_pt"],
                name_en=DATASET["name_en"],
                name_es=DATASET["name_es"],
                description_pt=DATASET["description_pt"],
                description_en=DATASET["description_en"],
                description_es=DATASET["description_es"],
                organization_ids=[
                    lookup("organization", s, env)
                    for s in DATASET["organization_slugs"]
                ],
                theme_ids=[
                    lookup("theme", s, env) for s in DATASET["theme_slugs"]
                ],
                tag_ids=resolve_tags(env),
                status_id=lookup(
                    "status",
                    "published" if args.publish else "under_review",
                    env,
                ),
                id=state.get("id"),
                env=env,
            )["id"]
        )
    )
    print(f"dataset {DATASET_SLUG} = {dataset_id}")

    source_ids = {}
    for key, source in RAW_SOURCES.items():
        result = server.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=source["name_pt"],
            name_en=source["name_en"],
            name_es=source["name_es"],
            url=source["url"],
            license_id=lookup("license", "cc_by", env),
            availability_id=lookup("availability", "online", env),
            description_pt=source["description_pt"],
            description_en=source["description_en"],
            description_es=source["description_es"],
            contains_api=source["contains_api"],
            is_free=True,
            requires_registration=False,
            id=state.get("raw_data_sources", {}).get(source["url"]),
            env=env,
        )
        source_ids[key] = server._strip_id(str(result["id"]))
        print(f"raw source {key} = {source_ids[key]}")

    wanted = args.tables or [t["slug"] for t in [*TABLES, DICIONARIO]]
    for spec in [*TABLES, DICIONARIO]:
        if spec["slug"] not in wanted:
            continue
        slug = spec["slug"]
        known = state.get("tables", {}).get(slug, {})
        table_id = server._strip_id(
            str(
                server.create_update_table(
                    slug=slug,
                    name_pt=spec["name_pt"],
                    name_en=spec["name_en"],
                    name_es=spec["name_es"],
                    dataset_id=dataset_id,
                    status_id=lookup("status", "published", env),
                    published_by_ids=[account_id],
                    data_cleaned_by_ids=[account_id],
                    description_pt=spec["description_pt"],
                    description_en=spec["description_en"],
                    description_es=spec["description_es"],
                    raw_data_source_ids=(
                        [source_ids[spec["source"]]]
                        if spec["source"]
                        else None
                    ),
                    id=known.get("id"),
                    env=env,
                )["id"]
            )
        )

        level_ids = {}
        for entity_slug in spec["observation_levels"]:
            result = server.create_update_observation_level(
                table_id=table_id,
                entity_id=entities[entity_slug],
                id=known.get("observation_levels", {}).get(entity_slug),
                env=env,
            )
            level_ids[entity_slug] = server._strip_id(str(result["id"]))

        upsert = server.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_payload(slug, env), env=env
        )

        columns = {
            c["name"]: server._strip_id(c["id"])
            for c in server._fetch_table_columns(table_id, env)
        }
        for row in architecture(slug):
            name = row["name"]
            entity_slug = COLUMN_ENTITY.get(name)
            level_id = level_ids.get(entity_slug) if entity_slug else None
            is_partition = name == "year"
            if level_id is None and not is_partition:
                continue
            # update_column's booleans default to False, so the partition flag has
            # to be re-passed on every call that touches the column.
            server.update_column(
                column_id=columns[name],
                column_name=name,
                table_id=table_id,
                observation_level_id=level_id,
                is_partition=is_partition,
                env=env,
            )

        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT[env],
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=slug,
            id=known.get("cloud_table"),
            env=env,
        )

        if spec["coverage"]:
            coverage = server.create_update_coverage(
                table_id=table_id,
                area_id=area_id,
                id=known.get("coverage"),
                env=env,
            )
            coverage_id = server._strip_id(str(coverage["id"]))
            start_year, start_month, end_year, end_month = spec["coverage"]
            server.create_update_datetime_range(
                coverage_id=coverage_id,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                interval=1,
                id=known.get("datetime_range"),
                env=env,
            )

        if spec["update"]:
            entity_slug, frequency, lag = spec["update"]
            server.create_update_update(
                entity_id=entities[entity_slug],
                frequency=frequency,
                lag=lag,
                latest=today,
                table_id=table_id,
                id=known.get("update"),
                env=env,
            )

        print(
            f"  {slug}: id={table_id} columns created={upsert.get('created')} "
            f"updated={upsert.get('updated')} errors={upsert.get('errors')}"
        )

    if args.tables is None:
        # Ordering only makes sense once every table exists.
        server.reorder_tables(
            dataset_slug=DATASET_SLUG,
            table_slugs=[t["slug"] for t in [*TABLES, DICIONARIO]],
            env=env,
        )
    print("done")


if __name__ == "__main__":
    main()
