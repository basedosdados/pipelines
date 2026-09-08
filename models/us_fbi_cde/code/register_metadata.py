"""Register the us_fbi_cde metadata in the Data Basis backend.

Run with the shared venv's interpreter so the databasis MCP module imports:

    ~/.venvs/bd-pipelines/bin/python models/us_fbi_cde/code/register_metadata.py --env staging

The dataset record is an existing, empty shell (``u_s_crime_data_explorer_cde``)
that already carries the FBI organization and trilingual text, so it is updated
rather than created. Three further empty FBI crime shells exist alongside it and
are reported, not touched.

Reference ids differ between staging and prod, so every one is resolved per
environment. Nothing here uploads data: the prod tables are materialised by the
table-approve action when the onboarding PR merges.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(
    0, str(Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp")
)

import server  # noqa: E402

from pipelines.datasets.us_fbi_cde.spec import TABLES  # noqa: E402

# Backend slug is the part after <country>_<org>_, so org "fbi" plus "cde" gives
# the GCP dataset id us_fbi_cde — the same shape as us_bls_cpi -> cpi. The shell
# was created in 2023 under a slug scraped from the page title; it is renamed
# rather than replaced, so its id, raw sources and inbound links all survive.
DATASET_SLUG = "cde"
LEGACY_DATASET_SLUG = "u_s_crime_data_explorer_cde"
GCP_DATASET_ID = "us_fbi_cde"

# The convention is the prod bucket, but a local credential is dev-only and the
# prod bucket refuses the write (403). Registering a URL to an object that does
# not exist is worse than pointing at one that does, and most existing rows in
# production already point here. Both buckets are requester-pays, so either way
# the link returns HTTP 400 to an anonymous visitor until the migration to
# gs://basedosdados-public lands (PR #1928).
AUXILIARY_BUCKET = "basedosdados-dev"

# Other empty FBI crime shells in production. Reported to the user as
# consolidation candidates; never modified here.
CONSOLIDATION_CANDIDATES = [
    "crime_in_the_u_s",
    "hate_crime_in_the_u_s",
    "uniform_crime_reporting_ucr_program_data_arrests_by_age_sex_and_race",
]

TABLE_NAMES = {
    "agency": (
        "Agências policiais",
        "Law enforcement agencies",
        "Agencias policiales",
    ),
    "incident": ("Incidentes", "Incidents", "Incidentes"),
    "offense": ("Ofensas", "Offenses", "Delitos"),
    "offender": ("Agressores", "Offenders", "Agresores"),
    "victim": ("Vítimas", "Victims", "Víctimas"),
    "victim_offense": (
        "Vítimas por ofensa",
        "Victims by offense",
        "Víctimas por delito",
    ),
    "victim_offender_relationship": (
        "Relação entre vítima e agressor",
        "Victim-offender relationship",
        "Relación entre víctima y agresor",
    ),
    "arrestee": ("Pessoas presas", "Arrested persons", "Personas detenidas"),
    "property": ("Bens", "Property", "Bienes"),
    "hate_crime": ("Crimes de ódio", "Hate crimes", "Delitos de odio"),
    "ucr_summary": (
        "Resumo mensal do UCR",
        "UCR monthly summary",
        "Resumen mensual del UCR",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}

TABLE_DESCRIPTIONS = {
    "agency": (
        "Uma linha por agência policial e ano, de 1960 a 2025, com o nome, o "
        "tipo, a população coberta, os quadros de pessoal e os campos de "
        "cobertura necessários para ponderar as demais tabelas: quantos meses a "
        "agência reportou ao sistema resumido, quantos reportou ao NIBRS e qual "
        "agência a cobre quando ela não reporta diretamente.",
        "One row per law enforcement agency and year, 1960 to 2025, with the "
        "name, type, population covered, staffing counts and the coverage fields "
        "needed to weight the other tables: how many months the agency reported "
        "to the summary system, how many it reported to NIBRS, and which agency "
        "covers it when it does not report directly.",
        "Una fila por agencia policial y año, de 1960 a 2025, con el nombre, el "
        "tipo, la población cubierta, las plantillas de personal y los campos de "
        "cobertura necesarios para ponderar las demás tablas: cuántos meses la "
        "agencia informó al sistema resumido, cuántos informó al NIBRS y qué "
        "agencia la cubre cuando no informa directamente.",
    ),
    "incident": (
        "Uma linha por incidente registrado no NIBRS entre 1991 e 2025, com a "
        "data, a hora, a agência e o esclarecimento por meio excepcional. A "
        "cobertura do NIBRS é parcial e cresce ao longo do período, de três "
        "estados em 1991 para todos em 2020.",
        "One row per incident recorded in NIBRS between 1991 and 2025, with the "
        "date, hour, agency and exceptional clearance. NIBRS coverage is partial "
        "and grows over the period, from three states in 1991 to every state in "
        "2020.",
        "Una fila por incidente registrado en el NIBRS entre 1991 y 2025, con la "
        "fecha, la hora, la agencia y el esclarecimiento por medio excepcional. "
        "La cobertura del NIBRS es parcial y crece a lo largo del período, de "
        "tres estados en 1991 a todos en 2020.",
    ),
    "offense": (
        "Uma linha por ofensa dentro de um incidente do NIBRS. Um incidente "
        "admite até dez ofensas e a regra da hierarquia do sistema resumido não "
        "se aplica, de modo que todas são contadas. Traz o local, a arma "
        "principal e a motivação por preconceito principal.",
        "One row per offense within a NIBRS incident. An incident may carry up to "
        "ten offenses and the summary system's hierarchy rule does not apply, so "
        "all of them are counted. Includes the location, the primary weapon and "
        "the primary bias motivation.",
        "Una fila por delito dentro de un incidente del NIBRS. Un incidente "
        "admite hasta diez delitos y la regla de la jerarquía del sistema "
        "resumido no se aplica, por lo que todos se cuentan. Incluye el lugar, el "
        "arma principal y la motivación por prejuicio principal.",
    ),
    "offender": (
        "Uma linha por agressor identificado em um incidente do NIBRS, com "
        "idade, sexo, raça e etnia quando informados. Agressores desconhecidos "
        "aparecem com número de ordem 0 e atributos demográficos nulos.",
        "One row per offender identified in a NIBRS incident, with age, sex, race "
        "and ethnicity when reported. Unknown offenders appear with sequence "
        "number 0 and null demographic attributes.",
        "Una fila por agresor identificado en un incidente del NIBRS, con edad, "
        "sexo, raza y etnia cuando se informan. Los agresores desconocidos "
        "aparecen con número de orden 0 y atributos demográficos nulos.",
    ),
    "victim": (
        "Uma linha por vítima de um incidente do NIBRS. Nem toda vítima é uma "
        "pessoa: empresas, instituições financeiras, governos e a sociedade "
        "também são registrados como vítimas.",
        "One row per victim of a NIBRS incident. Not every victim is a person: "
        "businesses, financial institutions, governments and society are also "
        "recorded as victims.",
        "Una fila por víctima de un incidente del NIBRS. No toda víctima es una "
        "persona: empresas, instituciones financieras, gobiernos y la sociedad "
        "también se registran como víctimas.",
    ),
    "victim_offense": (
        "Liga cada vítima às ofensas específicas que sofreu dentro do incidente. "
        "É necessária para contar vítimas por tipo de ofensa: atribuir todas as "
        "ofensas de um incidente a todas as suas vítimas superestima as "
        "contagens.",
        "Links each victim to the specific offenses they suffered within the "
        "incident. It is required to count victims by offense type: attributing "
        "all of an incident's offenses to all of its victims overstates the "
        "counts.",
        "Vincula cada víctima con los delitos específicos que sufrió dentro del "
        "incidente. Es necesaria para contar víctimas por tipo de delito: "
        "atribuir todos los delitos de un incidente a todas sus víctimas "
        "sobrestima los conteos.",
    ),
    "victim_offender_relationship": (
        "Liga cada vítima aos agressores do incidente e registra a relação entre "
        "eles. O preenchimento é obrigatório apenas quando o incidente inclui um "
        "crime contra a pessoa ou um roubo.",
        "Links each victim to the incident's offenders and records the "
        "relationship between them. Reporting is mandatory only when the incident "
        "includes a crime against a person or a robbery.",
        "Vincula cada víctima con los agresores del incidente y registra la "
        "relación entre ellos. Su cumplimentación es obligatoria solo cuando el "
        "incidente incluye un delito contra la persona o un robo.",
    ),
    "arrestee": (
        "Uma linha por pessoa presa, reunindo as prisões do Grupo A, ligadas a um "
        "incidente, e as do Grupo B, que chegam sem vínculo com incidente ou "
        "agência nos arquivos publicados.",
        "One row per arrested person, combining Group A arrests, which are linked "
        "to an incident, with Group B arrests, which arrive with no incident or "
        "agency link in the published files.",
        "Una fila por persona detenida, que reúne los arrestos del Grupo A, "
        "vinculados a un incidente, y los del Grupo B, que llegan sin vínculo con "
        "incidente ni agencia en los archivos publicados.",
    ),
    "property": (
        "Uma linha por descrição de bem envolvido em um incidente do NIBRS, com o "
        "tipo de perda, o valor em dólares correntes e, nas ofensas de drogas, o "
        "tipo e a quantidade da substância apreendida.",
        "One row per description of property involved in a NIBRS incident, with "
        "the loss type, the value in current dollars and, for drug offenses, the "
        "type and quantity of the substance seized.",
        "Una fila por descripción de bien involucrado en un incidente del NIBRS, "
        "con el tipo de pérdida, el valor en dólares corrientes y, en los delitos "
        "de drogas, el tipo y la cantidad de la sustancia incautada.",
    ),
    "hate_crime": (
        "Uma linha por incidente de crime de ódio registrado pelo programa UCR "
        "entre 1991 e 2025, com a motivação por preconceito, as ofensas, o local "
        "e as contagens de vítimas e agressores. Cobre também os anos anteriores "
        "ao NIBRS.",
        "One row per hate crime incident recorded by the UCR program between 1991 "
        "and 2025, with the bias motivation, the offenses, the location and the "
        "victim and offender counts. It also covers the years before NIBRS.",
        "Una fila por incidente de delito de odio registrado por el programa UCR "
        "entre 1991 y 2025, con la motivación por prejuicio, los delitos, el "
        "lugar y los conteos de víctimas y agresores. Cubre también los años "
        "anteriores al NIBRS.",
    ),
    "ucr_summary": (
        "Contagens mensais do formulário resumido Return A por agência e item de "
        "ofensa, de 1985 a 2025: ofensas efetivas, infundadas, esclarecidas e "
        "esclarecidas apenas com menores de 18 anos. É a série longa que "
        "atravessa a transição para o NIBRS, já que o formulário continua a ser "
        "coletado das agências que não migraram.",
        "Monthly counts from the Return A summary form by agency and offense line "
        "item, 1985 to 2025: actual, unfounded, cleared and juvenile-only cleared "
        "offenses. This is the long series that spans the transition to NIBRS, "
        "since the form is still collected from agencies that did not migrate.",
        "Conteos mensuales del formulario resumido Return A por agencia e ítem de "
        "delito, de 1985 a 2025: delitos efectivos, infundados, esclarecidos y "
        "esclarecidos solo con menores de 18 años. Es la serie larga que atraviesa "
        "la transición al NIBRS, ya que el formulario se sigue recogiendo de las "
        "agencias que no migraron.",
    ),
    "dicionario": (
        "Dicionário de códigos das colunas categóricas de todas as tabelas do "
        "conjunto, com o rótulo em inglês, a língua da fonte.",
        "Dictionary of the codes used by the categorical columns of every table "
        "in this dataset, with the label in English, the language of the source.",
        "Diccionario de los códigos de las columnas categóricas de todas las "
        "tablas del conjunto, con la etiqueta en inglés, el idioma de la fuente.",
    ),
}

RAW_SOURCES = [
    {
        "key": "nibrs",
        "description_pt": "Pacotes zip por estado e ano com as tabelas normalizadas do NIBRS, um conjunto de CSVs ligados por identificador de incidente. Publicados na página Documents and Downloads do Crime Data Explorer, servidos por URL assinada de duração curta.",
        "description_en": "Per-state, per-year zip bundles of the normalised NIBRS tables, a set of CSVs joined by incident identifier. Published on the Crime Data Explorer's Documents and Downloads page and served through short-lived presigned URLs.",
        "description_es": "Paquetes zip por estado y año con las tablas normalizadas del NIBRS, un conjunto de CSV vinculados por identificador de incidente. Publicados en la página Documents and Downloads del Crime Data Explorer, servidos mediante URL firmada de corta duración.",
        "name_pt": "FBI Crime Data Explorer — arquivos NIBRS por estado e ano",
        "name_en": "FBI Crime Data Explorer — NIBRS state-year bulk downloads",
        "name_es": "FBI Crime Data Explorer — archivos NIBRS por estado y año",
        "url": "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads",
        "tables": [
            "incident",
            "offense",
            "offender",
            "victim",
            "victim_offense",
            "victim_offender_relationship",
            "arrestee",
            "property",
            "dicionario",
        ],
    },
    {
        "key": "reta",
        "description_pt": "Arquivos mestres anuais do formulário resumido Return A, em formato de largura fixa com registro de 7.385 caracteres: um cabeçalho de agência seguido de doze blocos mensais.",
        "description_en": "Annual master files of the Return A summary form, in a fixed-width format with a 7,385-character record: an agency header followed by twelve monthly blocks.",
        "description_es": "Archivos maestros anuales del formulario resumido Return A, en formato de ancho fijo con registro de 7.385 caracteres: una cabecera de agencia seguida de doce bloques mensuales.",
        "name_pt": "FBI Crime Data Explorer — arquivos mestres Return A",
        "name_en": "FBI Crime Data Explorer — Return A summary master files",
        "name_es": "FBI Crime Data Explorer — archivos maestros Return A",
        "url": "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads",
        "tables": ["ucr_summary"],
    },
    {
        "key": "lee",
        "description_pt": "Extrato em CSV com o quadro de pessoal de cada agência policial por ano, de 1960 a 2025, incluindo a população coberta e as contagens de policiais e civis por sexo.",
        "description_en": "CSV extract of each law enforcement agency's staffing by year, 1960 to 2025, including the population covered and the officer and civilian counts by sex.",
        "description_es": "Extracto en CSV con la plantilla de cada agencia policial por año, de 1960 a 2025, incluida la población cubierta y los conteos de policías y civiles por sexo.",
        "name_pt": "FBI Crime Data Explorer — quadro de pessoal das agências policiais",
        "name_en": "FBI Crime Data Explorer — Law Enforcement Employees extract",
        "name_es": "FBI Crime Data Explorer — plantilla de las agencias policiales",
        "url": "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads",
        "tables": ["agency"],
    },
    {
        "key": "hate_crime",
        "description_pt": "Extrato em CSV com um registro por incidente de crime de ódio comunicado ao programa UCR desde 1991, acompanhado da nota metodológica da série.",
        "description_en": "CSV extract with one record per hate crime incident reported to the UCR program since 1991, accompanied by the series' methodology note.",
        "description_es": "Extracto en CSV con un registro por incidente de delito de odio comunicado al programa UCR desde 1991, acompañado de la nota metodológica de la serie.",
        "name_pt": "FBI Crime Data Explorer — extrato de crimes de ódio",
        "name_en": "FBI Crime Data Explorer — Hate Crime extract",
        "name_es": "FBI Crime Data Explorer — extracto de delitos de odio",
        "url": "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads",
        "tables": ["hate_crime"],
    },
]

# Observation levels per table, and the column that identifies each one.
OBSERVATION_LEVELS = {
    "agency": [("agency", "ori"), ("year", "year")],
    "incident": [
        ("record", "incident_id"),
        ("agency", "ori"),
        ("year", "year"),
    ],
    "offense": [("record", "offense_id"), ("year", "year")],
    "offender": [("person", "offender_id"), ("year", "year")],
    "victim": [("person", "victim_id"), ("year", "year")],
    "victim_offense": [("record", "victim_id"), ("year", "year")],
    "victim_offender_relationship": [
        ("person", "victim_id"),
        ("year", "year"),
    ],
    "arrestee": [("person", "arrestee_id"), ("year", "year")],
    "property": [("property", "property_description_id"), ("year", "year")],
    "hate_crime": [
        ("record", "incident_id"),
        ("agency", "ori"),
        ("year", "year"),
    ],
    "ucr_summary": [("agency", "ori"), ("month", "month"), ("year", "year")],
}

# Subject tags only. Place names, theme names and the organization are already
# structured metadata and must not be duplicated here. The backend uses English
# slugs in production and Portuguese ones in staging for the same tag, so each
# entry lists the alternatives and the first one that resolves is used.
TAG_SLUGS = [
    ("crime",),
    ("homicide", "homicidio"),
    ("robbery", "roubo"),
    ("assault", "assalto"),
    ("police", "policia"),
    ("weapon", "arma"),
    ("imprisonment", "prisao"),
    ("hate-crime", "crime-de-odio"),
    ("violence", "violencia"),
]

ARCH = Path(__file__).resolve().parent / "architecture"


def columns_payload(table):
    """Build the bulk_upsert_columns payload for one table from the spec."""
    payload = []
    for column in TABLES[table]["columns"]:
        entry = {
            "name": column["name"],
            "bigquery_type": column["bigquery_type"],
            "description_pt": column["description_pt"],
            "description_en": column["description_en"],
            "description_es": column["description_es"],
            "covered_by_dictionary": column["covered_by_dictionary"] == "yes",
            "has_sensitive_data": False,
        }
        if column["measurement_unit"]:
            entry["measurement_unit"] = column["measurement_unit"]
        if column["directory_column"]:
            entry["directory_column"] = column["directory_column"]
        if column["observations"]:
            entry["observations_pt"] = column["observations"]
            entry["observations_en"] = column["observations"]
            entry["observations_es"] = column["observations"]
        payload.append(entry)
    return payload


def existing_state(env):
    """Read back what is already registered, so a re-run updates rather than duplicates.

    create_update_observation_level, create_update_cloud_table,
    create_update_coverage and create_update_update all create a second record
    when called without an id, so every one of them is passed the id found here.
    """
    dataset = server.get_dataset(slug=DATASET_SLUG, env=env)
    if not dataset.get("found"):
        dataset = server.get_dataset(slug=LEGACY_DATASET_SLUG, env=env)
        if dataset.get("found"):
            print(
                f"found the shell under its legacy slug "
                f"{LEGACY_DATASET_SLUG}; renaming it to {DATASET_SLUG}"
            )
    if not dataset.get("found"):
        raise SystemExit(
            f"no dataset shell on {env} under {DATASET_SLUG} "
            f"or {LEGACY_DATASET_SLUG}"
        )
    state = {"dataset": dataset, "tables": {}}
    for slug, table in dataset.get("tables", {}).items():
        state["tables"][slug] = {
            "id": table["id"],
            "columns": {c["name"]: c["id"] for c in table.get("columns", [])},
            "observation_levels": {
                ol["entity_slug"]: ol["id"]
                for ol in table.get("observation_levels", [])
            },
            "cloud_table": (table.get("cloud_tables") or [{}])[0].get("id"),
            "coverage": (table.get("coverages") or [{}])[0].get("id"),
            "datetime_range": (
                (
                    (table.get("coverages") or [{}])[0].get("datetime_ranges")
                    or [{}]
                )[0]
            ).get("id"),
            "update": (table.get("updates") or [{}])[0].get("id"),
        }
    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env", default="staging", choices=["staging", "prod"]
    )
    parser.add_argument(
        "--only", default=None, help="comma-separated table subset"
    )
    parser.add_argument(
        "--today", default=None, help="table Update latest, YYYY-MM-DD"
    )
    args = parser.parse_args()
    env = args.env
    gcp_project = "basedosdados-dev" if env == "staging" else "basedosdados"
    # The backend types Update.latest as DateTime, so a bare date is rejected.
    today = (args.today or date.today().isoformat()) + "T00:00:00"

    ids = server.discover_ids(
        env=env,
        keys=["status", "theme", "tag", "entity", "license", "availability"],
    )
    area_us = server.lookup_id(category="area", slug="us", env=env)
    area_id = area_us.get("id") if isinstance(area_us, dict) else area_us
    print(f"area us = {area_id}")

    state = existing_state(env)
    dataset = state["dataset"]
    dataset_id = dataset["id"]
    print(f"dataset shell {DATASET_SLUG} = {dataset_id}")
    print(
        "consolidation candidates (untouched): "
        + ", ".join(CONSOLIDATION_CANDIDATES)
    )

    account_id = server.get_authenticated_account(env=env)["id"]

    tag_ids = list({t["id"] for t in dataset.get("tags", [])})
    missing = []
    for alternatives in TAG_SLUGS:
        resolved = next(
            (ids["tag"][s] for s in alternatives if s in ids["tag"]), None
        )
        if resolved is None:
            missing.append(alternatives[0])
        elif resolved not in tag_ids:
            tag_ids.append(resolved)
    if missing:
        print(f"tags absent from this backend, not created: {missing}")

    server.create_update_dataset(
        id=dataset_id,
        slug=DATASET_SLUG,
        name_pt=dataset["name_pt"],
        name_en=dataset["name_en"],
        name_es=dataset["name_es"],
        description_pt=dataset["description_pt"],
        description_en=dataset["description_en"],
        description_es=dataset["description_es"],
        organization_ids=[o["id"] for o in dataset["organizations"]],
        theme_ids=[t["id"] for t in dataset["themes"]],
        tag_ids=tag_ids,
        status_id=ids["status"]["under_review"],
        env=env,
    )
    print(f"dataset updated: status under_review, {len(tag_ids)} tags")

    # The shell carries one raw source from 2023 named "Dados" whose URL,
    # https://crime-data-explorer.fr.cloud.gov/downloads-and-docs, is a hard 404
    # since the CDE moved hosts. Its id is reused for the NIBRS source so the
    # record is refreshed rather than left beside a working duplicate.
    # Called directly the tool returns a bare list; through the MCP layer it is
    # wrapped in {"result": [...]}. Accept either.
    existing = server.get_raw_data_sources(dataset_slug=DATASET_SLUG, env=env)
    if isinstance(existing, dict):
        existing = existing.get("result", [])
    # get_raw_data_sources returns a single `name`, and it is the Portuguese one.
    # Keying this on name_en never matched, so every run created four more
    # sources instead of updating the existing four.
    known_sources = {source["name"]: source["id"] for source in existing}
    legacy = next(
        (
            s
            for s in existing
            if "crime-data-explorer.fr.cloud.gov" in (s.get("url") or "")
        ),
        None,
    )
    if legacy:
        print(
            f"reusing the dead 2023 raw source {legacy['id']} ({legacy['url']}) for NIBRS"
        )
        known_sources.setdefault(RAW_SOURCES[0]["name_pt"], legacy["id"])
    raw_ids = {}
    for source in RAW_SOURCES:
        result = server.create_update_raw_data_source(
            id=known_sources.get(source["name_pt"]),
            dataset_id=dataset_id,
            name_pt=source["name_pt"],
            name_en=source["name_en"],
            name_es=source["name_es"],
            url=source["url"],
            license_id=ids["license"]["cc0"],
            availability_id=ids["availability"]["online"],
            description_pt=source["description_pt"],
            description_en=source["description_en"],
            description_es=source["description_es"],
            has_structured_data=True,
            is_free=True,
            contains_api=True,
            requires_registration=False,
            status_id=ids["status"]["published"],
            env=env,
        )
        raw_ids[source["key"]] = result["id"]
        print(f"raw source {source['key']} = {result['id']}")

    raw_for_table = {}
    for source in RAW_SOURCES:
        for table in source["tables"]:
            raw_for_table[table] = raw_ids[source["key"]]

    wanted = args.only.split(",") if args.only else list(TABLES)
    for table in wanted:
        spec = TABLES[table]
        prior = state["tables"].get(table, {})
        name_pt, name_en, name_es = TABLE_NAMES[table]
        desc_pt, desc_en, desc_es = TABLE_DESCRIPTIONS[table]
        aux = (
            ""
            if table == "dicionario"
            else f"https://storage.googleapis.com/{AUXILIARY_BUCKET}/auxiliary_files/"
            f"{GCP_DATASET_ID}/{table}/auxiliary_files.zip"
        )
        table_id = server.create_update_table(
            id=prior.get("id"),
            dataset_id=dataset_id,
            slug=table,
            name_pt=name_pt,
            name_en=name_en,
            name_es=name_es,
            description_pt=desc_pt,
            description_en=desc_en,
            description_es=desc_es,
            status_id=ids["status"]["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            auxiliary_files_url=aux,
            env=env,
        )["id"]
        print(f"\n{table}: table {table_id}")

        ol_ids = {}
        for entity_slug, _ in OBSERVATION_LEVELS.get(table, []):
            entity_id = ids["entity"].get(entity_slug)
            if not entity_id:
                print(f"  [warn] entity {entity_slug} absent on {env}")
                continue
            ol_ids[entity_slug] = server.create_update_observation_level(
                id=prior.get("observation_levels", {}).get(entity_slug),
                table_id=table_id,
                entity_id=entity_id,
                env=env,
            )["id"]
        if ol_ids:
            print(f"  observation levels: {', '.join(ol_ids)}")

        result = server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(
                columns_payload(table), ensure_ascii=False
            ),
            env=env,
        )
        if result.get("errors"):
            raise SystemExit(f"{table}: column errors {result['errors']}")
        registered = server.get_dataset(slug=DATASET_SLUG, env=env)
        by_name = {
            c["name"]: c["id"]
            for c in registered["tables"][table].get("columns", [])
        }
        print(
            f"  columns registered: {len(by_name)} of {len(spec['columns'])}"
        )

        # bulk_upsert_columns does not set is_partition, and update_column's
        # boolean arguments default to False, so the flag is re-passed whenever
        # the same column also carries an observation-level link.
        ol_for_column = {
            column: ol_ids[entity]
            for entity, column in OBSERVATION_LEVELS.get(table, [])
            if entity in ol_ids
        }
        for name in sorted(set(spec["partitions"]) | set(ol_for_column)):
            if name not in by_name:
                print(f"  [warn] column {name} missing, cannot flag")
                continue
            server.update_column(
                column_id=by_name[name],
                column_name=name,
                table_id=table_id,
                is_partition=name in spec["partitions"],
                observation_level_id=ol_for_column.get(name),
                env=env,
            )
        print(
            f"  partitions {spec['partitions']}, OL links {sorted(ol_for_column)}"
        )

        server.create_update_cloud_table(
            id=prior.get("cloud_table"),
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            env=env,
        )

        if spec["first_year"]:
            coverage_id = server.create_update_coverage(
                id=prior.get("coverage"),
                table_id=table_id,
                area_id=area_id,
                env=env,
            )["id"]
            server.create_update_datetime_range(
                id=prior.get("datetime_range"),
                coverage_id=coverage_id,
                start_year=spec["first_year"],
                end_year=spec["last_year"],
                interval=1,
                env=env,
            )
            print(f"  coverage us {spec['first_year']}-{spec['last_year']}")

        # The table Update is a wall clock: when Data Basis last refreshed the
        # table, not the source's coverage date.
        server.create_update_update(
            id=prior.get("update"),
            table_id=table_id,
            entity_id=ids["entity"]["year"],
            frequency=1,
            lag=1,
            latest=today,
            env=env,
        )

        # Deferred: the raw source link is a second write, once every source exists.
        if table in raw_for_table:
            server.create_update_table(
                id=table_id,
                dataset_id=dataset_id,
                slug=table,
                name_pt=name_pt,
                name_en=name_en,
                name_es=name_es,
                description_pt=desc_pt,
                description_en=desc_en,
                description_es=desc_es,
                status_id=ids["status"]["published"],
                published_by_ids=[account_id],
                data_cleaned_by_ids=[account_id],
                auxiliary_files_url=aux,
                raw_data_source_ids=[raw_for_table[table]],
                env=env,
            )
            print(f"  raw source linked: {raw_for_table[table]}")

    server.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=list(TABLES), env=env
    )
    print("\ntable order set")

    # The raw data source Update is a coverage date: what the source published.
    server.create_update_update(
        raw_data_source_id=raw_ids["nibrs"],
        entity_id=ids["entity"]["year"],
        frequency=1,
        latest=f"{TABLES['incident']['last_year']}-12-01T00:00:00",
        env=env,
    )
    print("raw source Update recorded at the source's max coverage date")
    print("done")


if __name__ == "__main__":
    main()
