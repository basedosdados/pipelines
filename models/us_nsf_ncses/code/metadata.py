"""Register the us_nsf_ncses metadata in the Data Basis backend.

Fills the existing NCSES dataset shell — the record created for the Survey of
Earned Doctorates, id 60aaf560-d82a-42b9-bf86-155d18b8c5bf — and broadens it to
the agency's two surveys rather than creating a second dataset. See
``models/us_nsf_ncses/README.md`` for why.

Columns come from the architecture CSVs, so the registered types, descriptions
and directory links cannot drift from the dbt models, which are generated from
the same files.

Run with the shared venv, which has fastmcp and requests:

    ~/.venvs/bd-pipelines/bin/python models/us_nsf_ncses/code/metadata.py staging
    ~/.venvs/bd-pipelines/bin/python models/us_nsf_ncses/code/metadata.py prod
"""

from __future__ import annotations

import csv
import datetime
import json
import sys
from pathlib import Path

sys.path.insert(
    0,
    str(
        Path.home()
        / "Monash Uni Enterprise Dropbox"
        / "Ricardo Dahis"
        / "BD"
        / "mcp"
    ),
)

import server

CODE_DIR = Path(__file__).resolve().parent
ARCH_DIR = CODE_DIR / "architecture"

DATASET_ID = "60aaf560-d82a-42b9-bf86-155d18b8c5bf"
DATASET_SLUG = "ncses"
GCP_DATASET = "us_nsf_ncses"
ORGANIZATION_ID = "72c64ec6-4504-4352-ac6e-e1493601b417"
TODAY = f"{datetime.date.today().isoformat()}T00:00:00+00:00"

NAME_PT = "Estatísticas de Ciência e Engenharia do NCSES (HERD e SED)"
NAME_EN = "NCSES Science and Engineering Statistics (HERD and SED)"
NAME_ES = "Estadísticas de Ciencia e Ingeniería del NCSES (HERD y SED)"

DESCRIPTION_PT = (
    "Dois levantamentos do National Center for Science and Engineering "
    "Statistics (NCSES), a agência estatística federal dentro da National "
    "Science Foundation dos Estados Unidos. O Higher Education Research and "
    "Development Survey (HERD) é um censo anual das faculdades e universidades "
    "americanas que gastam ao menos US$ 150 mil em pesquisa e desenvolvimento "
    "orçado separadamente; aqui estão seus arquivos de uso público em nível de "
    "instituição para os anos fiscais de 1972 a 2024, incluindo o levantamento "
    "anterior, o Survey of R&D Expenditures at Universities and Colleges, com "
    "despesas por campo de pesquisa, fonte de recursos e agência federal "
    "financiadora, além das contagens de pessoal de pesquisa. O Survey of "
    "Earned Doctorates (SED) é um censo anual, realizado desde o ano acadêmico "
    "de 1958, de todos os que recebem um doutorado de pesquisa de uma "
    "instituição americana credenciada; aqui estão suas tabelas agregadas "
    "publicadas, já que os registros individuais são de uso restrito. As "
    "instituições do HERD carregam o UNITID do IPEDS, o que liga este conjunto "
    "a us_ed_ipeds e us_ed_college_scorecard; com us_nih_reporter, os três "
    "descrevem uma mesma cadeia: o financiamento federal que entra, o gasto em "
    "pesquisa que a instituição realiza e os pesquisadores que o sistema forma."
)
DESCRIPTION_EN = (
    "Two surveys run by the National Center for Science and Engineering "
    "Statistics (NCSES), the federal statistical agency inside the U.S. "
    "National Science Foundation. The Higher Education Research and "
    "Development (HERD) Survey is an annual census of U.S. colleges and "
    "universities that spend at least $150,000 on separately budgeted research "
    "and development; this dataset carries its institution-level public use "
    "files for fiscal years 1972 to 2024, including the predecessor Survey of "
    "R&D Expenditures at Universities and Colleges, with expenditures by field "
    "of research, source of funds and funding federal agency, and counts of "
    "research personnel. The Survey of Earned Doctorates (SED) is an annual "
    "census, conducted since academic year 1958, of everyone receiving a "
    "research doctorate from an accredited U.S. institution; this dataset "
    "carries its published aggregate tables, since the individual records are "
    "restricted use. HERD institutions carry their IPEDS UNITID, which joins "
    "this dataset to us_ed_ipeds and us_ed_college_scorecard; with "
    "us_nih_reporter the three describe one chain: the federal funding that "
    "comes in, the research spending the institution performs, and the "
    "researchers the system produces."
)
DESCRIPTION_ES = (
    "Dos encuestas del National Center for Science and Engineering Statistics "
    "(NCSES), la agencia estadística federal dentro de la National Science "
    "Foundation de los Estados Unidos. La Higher Education Research and "
    "Development Survey (HERD) es un censo anual de las universidades "
    "estadounidenses que gastan al menos 150 mil dólares en investigación y "
    "desarrollo presupuestado por separado; aquí están sus archivos de uso "
    "público a nivel de institución para los años fiscales de 1972 a 2024, "
    "incluida la encuesta anterior, el Survey of R&D Expenditures at "
    "Universities and Colleges, con gastos por campo de investigación, fuente "
    "de recursos y agencia federal financiadora, además de los conteos de "
    "personal de investigación. La Survey of Earned Doctorates (SED) es un "
    "censo anual, realizado desde el año académico de 1958, de todas las "
    "personas que reciben un doctorado de investigación de una institución "
    "estadounidense acreditada; aquí están sus tablas agregadas publicadas, ya "
    "que los registros individuales son de uso restringido. Las instituciones "
    "del HERD llevan el UNITID del IPEDS, lo que conecta este conjunto con "
    "us_ed_ipeds y us_ed_college_scorecard; junto con us_nih_reporter, los tres "
    "describen una misma cadena: el financiamiento federal que entra, el gasto "
    "en investigación que realiza la institución y los investigadores que el "
    "sistema forma."
)

# Reference slugs, resolved against whichever backend is being written. Never
# hardcode the ids: most happen to match across environments, but the
# higher_education_institution entity does not (cc6669a8 on staging,
# b39d2987 on prod), and copying the staging id into prod would silently file
# every HERD table under the wrong entity.
REF_SLUGS = {
    "status_under_review": ("status", "under_review"),
    "status_published": ("status", "published"),
    "theme_economics": ("theme", "economics"),
    "theme_science": ("theme", "science-technology"),
    "theme_education": ("theme", "education"),
    "entity_year": ("entity", "year"),
    "entity_institution": ("entity", "higher_education_institution"),
    "entity_document": ("entity", "document"),
    "license_ppdl": ("license", "ppdl"),
    "availability_online": ("availability", "online"),
    "area_us": ("area", "us"),
}


def resolve_refs(env: str) -> dict[str, str]:
    """Look every reference id up in the target backend."""
    refs = {}
    for key, (category, slug) in REF_SLUGS.items():
        refs[key] = server.lookup_id(category=category, slug=slug, env=env)[
            "id"
        ]
    return refs


# Per-table auxiliary file bundle, written by auxiliary_files.py. The bucket is
# requester-pays, so an anonymous fetch of these URLs returns HTTP 400 — true of
# every production table using the field, not of this dataset in particular.
AUXILIARY_BASE = "https://storage.googleapis.com/basedosdados-dev/auxiliary_files/us_nsf_ncses"

# Partition column per table; dicionario has none.
PARTITIONS = {
    "herd_institution": "year",
    "herd_expenditure": "year",
    "herd_personnel": "year",
    "herd_survey_item": "year",
    "sed_data_table": "reference_year",
    "sed_estimate": "reference_year",
}

HERD_LEVELS = ("year", "institution")
SED_LEVELS = ("year", "document")

TABLES = [
    {
        "slug": "herd_institution",
        "name_pt": "Instituições pesquisadas (HERD)",
        "name_en": "Surveyed institutions (HERD)",
        "name_es": "Instituciones encuestadas (HERD)",
        "description_pt": (
            "Instituições de ensino superior dos Estados Unidos pesquisadas "
            "pelo levantamento HERD, uma linha por instituição e ano fiscal, "
            "de 1972 a 2024. Reúne a era atual do levantamento, a partir do "
            "ano fiscal de 2010, e a anterior, o Survey of R&D Expenditures at "
            "Universities and Colleges. A coluna unitid liga a instituição ao "
            "IPEDS."
        ),
        "description_en": (
            "U.S. higher education institutions surveyed by HERD, one row per "
            "institution and fiscal year, from 1972 to 2024. It brings "
            "together the current era of the survey, from fiscal year 2010, "
            "and the earlier Survey of R&D Expenditures at Universities and "
            "Colleges. The unitid column links the institution to IPEDS."
        ),
        "description_es": (
            "Instituciones de educación superior de los Estados Unidos "
            "encuestadas por HERD, una fila por institución y año fiscal, de "
            "1972 a 2024. Reúne la era actual de la encuesta, desde el año "
            "fiscal de 2010, y la anterior, el Survey of R&D Expenditures at "
            "Universities and Colleges. La columna unitid conecta la "
            "institución con IPEDS."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 1972,
        "end": 2024,
    },
    {
        "slug": "herd_expenditure",
        "name_pt": "Despesas em pesquisa e desenvolvimento (HERD)",
        "name_en": "Research and development expenditures (HERD)",
        "name_es": "Gastos en investigación y desarrollo (HERD)",
        "description_pt": (
            "Despesas de pesquisa e desenvolvimento declaradas por instituição "
            "de ensino superior ao levantamento HERD, de 1972 a 2024, em "
            "formato longo: uma linha por instituição, ano fiscal e célula do "
            "questionário. As células cobrem fonte de recursos, campo de "
            "pesquisa, agência federal financiadora, tipo de custo, recursos "
            "estrangeiros, ensaios clínicos, repasses recebidos e repassados e "
            "equipamentos capitalizados. Valores em dólares correntes, "
            "convertidos dos milhares de dólares publicados pelo NCSES."
        ),
        "description_en": (
            "Research and development expenditures reported by higher "
            "education institutions to the HERD Survey, from 1972 to 2024, in "
            "long form: one row per institution, fiscal year and questionnaire "
            "cell. The cells cover source of funds, field of research, funding "
            "federal agency, type of cost, foreign funds, clinical trials, "
            "funds received as a subrecipient and passed through, and "
            "capitalized equipment. Values in current dollars, converted from "
            "the thousands of dollars NCSES publishes."
        ),
        "description_es": (
            "Gastos en investigación y desarrollo declarados por instituciones "
            "de educación superior a la encuesta HERD, de 1972 a 2024, en "
            "formato largo: una fila por institución, año fiscal y celda del "
            "cuestionario. Las celdas cubren fuente de recursos, campo de "
            "investigación, agencia federal financiadora, tipo de costo, "
            "recursos extranjeros, ensayos clínicos, transferencias recibidas y "
            "traspasadas, y equipos capitalizados. Valores en dólares "
            "corrientes, convertidos de los miles de dólares que publica el "
            "NCSES."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 1972,
        "end": 2024,
    },
    {
        "slug": "herd_personnel",
        "name_pt": "Pessoal de pesquisa e desenvolvimento (HERD)",
        "name_en": "Research and development personnel (HERD)",
        "name_es": "Personal de investigación y desarrollo (HERD)",
        "description_pt": (
            "Pessoal de pesquisa e desenvolvimento nas instituições "
            "pesquisadas pelo HERD, em número de pessoas e em equivalentes de "
            "tempo integral, de 2010 a 2024. Os arquivos de uso público não "
            "trazem contagem de pessoal para os anos fiscais de 2020 e 2021, e "
            "os equivalentes de tempo integral passaram a ser coletados no ano "
            "fiscal de 2022."
        ),
        "description_en": (
            "Research and development personnel at the institutions HERD "
            "surveys, as headcounts and as full-time equivalents, from 2010 to "
            "2024. The public use files carry no personnel count for fiscal "
            "years 2020 and 2021, and full-time equivalents were first "
            "collected in fiscal year 2022."
        ),
        "description_es": (
            "Personal de investigación y desarrollo en las instituciones que "
            "encuesta HERD, en número de personas y en equivalentes de tiempo "
            "completo, de 2010 a 2024. Los archivos de uso público no traen "
            "conteo de personal para los años fiscales de 2020 y 2021, y los "
            "equivalentes de tiempo completo comenzaron a recolectarse en el "
            "año fiscal de 2022."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 2010,
        "end": 2024,
    },
    {
        "slug": "herd_survey_item",
        "name_pt": "Outros itens do questionário (HERD)",
        "name_en": "Other questionnaire items (HERD)",
        "name_es": "Otros ítems del cuestionario (HERD)",
        "description_pt": (
            "Itens do questionário HERD que não são despesas nem contagens de "
            "pessoal, de 2010 a 2024: a composição dos recursos próprios da "
            "instituição declarados como pesquisa financiada internamente, a "
            "inclusão de ensaios clínicos no relatório do ano fiscal de 2009 e "
            "os limites de capitalização de equipamentos e de software."
        ),
        "description_en": (
            "HERD questionnaire items that are neither expenditures nor "
            "personnel counts, from 2010 to 2024: what the institution counted "
            "as institutionally financed research, whether clinical trials "
            "were included in the fiscal year 2009 report, and the "
            "capitalization thresholds for equipment and software."
        ),
        "description_es": (
            "Ítems del cuestionario HERD que no son gastos ni conteos de "
            "personal, de 2010 a 2024: la composición de los recursos propios "
            "de la institución declarados como investigación financiada "
            "internamente, la inclusión de ensayos clínicos en el informe del "
            "año fiscal de 2009 y los límites de capitalización de equipos y "
            "de software."
        ),
        "levels": HERD_LEVELS,
        "level_columns": {"year": "year", "institution": "institution_id"},
        "start": 2010,
        "end": 2024,
    },
    {
        "slug": "sed_estimate",
        "name_pt": "Estimativas publicadas (SED)",
        "name_en": "Published estimates (SED)",
        "name_es": "Estimaciones publicadas (SED)",
        "description_pt": (
            "Estimativas publicadas pelo Survey of Earned Doctorates em "
            "formato longo: uma linha por célula das tabelas de dados do "
            "ciclo, com o caminho hierárquico completo da linha e da coluna "
            "preservado. Cobre contagens de doutores por ano, campo, sexo, "
            "situação de cidadania, etnia e raça, além de compromissos após a "
            "titulação, apoio financeiro, dívida educacional, tempo até o "
            "título, salários e instituições de origem e de titulação. A "
            "microdados individual do SED é de uso restrito e não está aqui. "
            "Cada ciclo republica a própria série histórica, então filtre pelo "
            "maior reference_year em vez de somar entre ciclos."
        ),
        "description_en": (
            "Estimates published by the Survey of Earned Doctorates in long "
            "form: one row per cell of the cycle's data tables, with the full "
            "hierarchical path of both the row and the column preserved. It "
            "covers counts of doctorate recipients by year, field, sex, "
            "citizenship status, ethnicity and race, along with postgraduation "
            "commitments, financial support, education-related debt, time to "
            "degree, salaries, and baccalaureate-origin and doctorate-granting "
            "institutions. SED individual microdata is restricted use and is "
            "not here. Each cycle republishes its own history, so filter to the "
            "largest reference_year rather than summing across cycles."
        ),
        "description_es": (
            "Estimaciones publicadas por la Survey of Earned Doctorates en "
            "formato largo: una fila por celda de las tablas de datos del "
            "ciclo, con la ruta jerárquica completa de la fila y de la columna "
            "preservada. Cubre conteos de doctores por año, campo, sexo, "
            "situación de ciudadanía, etnia y raza, además de compromisos tras "
            "la titulación, apoyo financiero, deuda educativa, tiempo hasta el "
            "título, salarios e instituciones de origen y de titulación. Los "
            "microdatos individuales del SED son de uso restringido y no están "
            "aquí. Cada ciclo republica su propia serie histórica, así que "
            "filtre por el mayor reference_year en vez de sumar entre ciclos."
        ),
        "levels": SED_LEVELS,
        "level_columns": {"year": "year", "document": "table_id"},
        "start": 1958,
        "end": 2024,
    },
    {
        "slug": "sed_data_table",
        "name_pt": "Catálogo das tabelas publicadas (SED)",
        "name_en": "Catalogue of published tables (SED)",
        "name_es": "Catálogo de las tablas publicadas (SED)",
        "description_pt": (
            "Catálogo das tabelas de dados publicadas pelo Survey of Earned "
            "Doctorates em cada ciclo da pesquisa: identificador, título, grupo "
            "temático e declaração de unidade. Serve de índice para "
            "sed_estimate, que traz as células dessas mesmas tabelas."
        ),
        "description_en": (
            "Catalogue of the data tables the Survey of Earned Doctorates "
            "publishes in each survey cycle: identifier, title, thematic group "
            "and unit statement. It indexes sed_estimate, which carries the "
            "cells of those same tables."
        ),
        "description_es": (
            "Catálogo de las tablas de datos que publica la Survey of Earned "
            "Doctorates en cada ciclo de la encuesta: identificador, título, "
            "grupo temático y declaración de unidad. Sirve de índice para "
            "sed_estimate, que trae las celdas de esas mismas tablas."
        ),
        "levels": SED_LEVELS,
        "level_columns": {"year": "reference_year", "document": "table_id"},
        "start": 2024,
        "end": 2024,
    },
    {
        "slug": "dicionario",
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Dicionário de valores codificados do conjunto. Vários códigos "
            "mudam de significado entre as duas eras do levantamento HERD, por "
            "isso cada entrada traz sua própria cobertura temporal."
        ),
        "description_en": (
            "Dictionary of the dataset's coded values. Several codes change "
            "meaning between the two eras of the HERD Survey, so each entry "
            "carries its own temporal coverage."
        ),
        "description_es": (
            "Diccionario de los valores codificados del conjunto. Varios "
            "códigos cambian de significado entre las dos eras de la encuesta "
            "HERD, por eso cada entrada trae su propia cobertura temporal."
        ),
        "levels": (),
        "level_columns": {},
        "start": 1972,
        "end": 2024,
    },
]

RAW_SOURCES = [
    {
        "name_pt": "Arquivos de uso público do HERD",
        "name_en": "HERD public use data files",
        "name_es": "Archivos de uso público del HERD",
        "url": (
            "https://ncses.nsf.gov/explore-data/microdata/"
            "higher-education-research-development"
        ),
        "tables": [
            "herd_institution",
            "herd_expenditure",
            "herd_personnel",
            "herd_survey_item",
        ],
    },
    {
        "name_pt": "Tabelas de dados do SED",
        "name_en": "SED data tables",
        "name_es": "Tablas de datos del SED",
        "url": "https://ncses.nsf.gov/surveys/earned-doctorates",
        "tables": ["sed_estimate", "sed_data_table"],
    },
]


def architecture(table: str) -> list[dict]:
    with open(ARCH_DIR / f"{table}.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def columns_payload(table: str) -> str:
    """Build the bulk_upsert_columns payload from the architecture CSV.

    Every description and every observation goes in all three languages:
    bulk_upsert_columns writes a bare ``description`` or ``observations`` key as
    Portuguese only, which is how thousands of production columns ended up
    single-language.
    """
    payload = []
    for row in architecture(table):
        entry = {
            "name": row["name"],
            "bigquery_type": row["bigquery_type"],
            "description_pt": row["description"],
            "description_en": row["description_en"],
            "description_es": row["description_es"],
            "covered_by_dictionary": row["covered_by_dictionary"],
            "has_sensitive_data": row["has_sensitive_data"],
        }
        for key in (
            "directory_column",
            "measurement_unit",
            "temporal_coverage",
        ):
            if row[key]:
                entry[key] = row[key]
        if row["observations"]:
            entry["observations_pt"] = row["observations"]
            entry["observations_en"] = row["observations_en"]
            entry["observations_es"] = row["observations_es"]
        payload.append(entry)
    return json.dumps(payload, ensure_ascii=False)


# Tags, by id. The ids are stable across environments but the slugs are not —
# staging spells them in Portuguese and production in English, and not always
# the same English word: `financiamento` on staging is `financing` on prod, so
# resolving by slug silently dropped it from the first prod registration.
# No tag for "science": that would restate the science-technology theme, which
# the dataset already carries, and tags are not for duplicating other metadata.
TAG_IDS = {
    "6c3ab030-1bd3-4910-82f7-1f399c302ca9": "doutorado / doctorate",
    "4ae52b90-bc5e-49b3-92f6-c5e86ae5a241": "pesquisa / research",
    "343275c0-ab19-4be5-bfa2-530180a501ee": "renda / income",
    "1d05dabf-1ed7-46e3-8c68-8628752cdf39": "salario / salary",
    "161d4c2e-a61e-481d-8821-3f70b534c063": "trabalho / labor",
    "21d15e6c-d39b-4c79-800f-5a13a6e797d3": "universidade / university",
    "25cde861-2c55-4c85-9c5a-48048953c6d4": "financiamento / financing",
}


def resolve_tags(env: str) -> list[str]:
    """Return the dataset's tag ids, checking each one exists in this backend."""
    query = (
        "query($id: ID!) { allTag(id: $id) { edges { node { id slug } } } }"
    )
    ids, missing = [], []
    for tag_id, label in TAG_IDS.items():
        edges = server._gql(query, {"id": tag_id}, env=env)["allTag"]["edges"]
        if edges:
            ids.append(tag_id)
        else:
            missing.append(label)
    if missing:
        print(f"  tags absent from {env}, skipped: {', '.join(missing)}")
    return ids


def register(env: str) -> dict:
    """Register the dataset, its raw sources and its seven tables."""
    refs = resolve_refs(env)
    account = server.get_authenticated_account(env=env)
    account_id = account["id"]
    area_us = refs["area_us"]

    dataset = server.create_update_dataset(
        id=DATASET_ID,
        slug=DATASET_SLUG,
        name_pt=NAME_PT,
        name_en=NAME_EN,
        name_es=NAME_ES,
        description_pt=DESCRIPTION_PT,
        description_en=DESCRIPTION_EN,
        description_es=DESCRIPTION_ES,
        organization_ids=[ORGANIZATION_ID],
        theme_ids=[
            refs["theme_science"],
            refs["theme_education"],
            refs["theme_economics"],
        ],
        tag_ids=resolve_tags(env),
        status_id=refs["status_under_review"],
        env=env,
    )
    print(f"dataset {DATASET_SLUG}: {dataset}")

    # Raw sources are matched on URL so a re-run updates rather than appends.
    by_url = {
        existing_source["url"]: existing_source["id"]
        for existing_source in server.get_raw_data_sources(
            dataset_slug=DATASET_SLUG, env=env
        )
    }
    source_ids: dict[str, list[str]] = {}
    for source in RAW_SOURCES:
        created = server.create_update_raw_data_source(
            dataset_id=DATASET_ID,
            name_pt=source["name_pt"],
            name_en=source["name_en"],
            name_es=source["name_es"],
            url=source["url"],
            license_id=refs["license_ppdl"],
            availability_id=refs["availability_online"],
            has_structured_data=True,
            is_free=True,
            requires_registration=False,
            id=by_url.get(source["url"]),
            env=env,
        )
        print(f"raw source {source['name_en']}: {created}")
        for table in source["tables"]:
            source_ids.setdefault(table, []).append(created["id"])

    existing = server.get_dataset(slug=DATASET_SLUG, env=env).get("tables", {})
    report = {}
    for spec in TABLES:
        slug = spec["slug"]
        prior = existing.get(slug, {})
        table = server.create_update_table(
            id=prior.get("id"),
            slug=slug,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            description_en=spec["description_en"],
            description_es=spec["description_es"],
            dataset_id=DATASET_ID,
            status_id=refs["status_published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=source_ids.get(slug, []),
            auxiliary_files_url=(
                ""
                if slug == "dicionario"
                else f"{AUXILIARY_BASE}/{slug}/auxiliary_files.zip"
            ),
            env=env,
        )
        table_id = table["id"]

        # create_update_* is not idempotent: called without an id it appends a
        # second observation level, cloud table, coverage or update rather than
        # replacing the first, so every existing record's id is reused.
        prior_levels = {
            level["entity_id"]: level["id"]
            for level in prior.get("observation_levels", [])
        }
        levels = {}
        for level in spec["levels"]:
            entity_id = refs[f"entity_{level}"]
            created = server.create_update_observation_level(
                id=prior_levels.get(entity_id),
                table_id=table_id,
                entity_id=entity_id,
                env=env,
            )
            levels[level] = created["id"]

        cloud_tables = prior.get("cloud_tables", [])
        server.create_update_cloud_table(
            id=cloud_tables[0]["id"] if cloud_tables else None,
            table_id=table_id,
            gcp_project_id=(
                "basedosdados" if env == "prod" else "basedosdados-dev"
            ),
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=slug,
            env=env,
        )

        coverages = prior.get("coverages", [])
        coverage = server.create_update_coverage(
            id=coverages[0]["id"] if coverages else None,
            table_id=table_id,
            area_id=area_us,
            env=env,
        )
        ranges = coverages[0].get("datetime_ranges", []) if coverages else []
        server.create_update_datetime_range(
            id=ranges[0]["id"] if ranges else None,
            coverage_id=coverage["id"],
            start_year=spec["start"],
            end_year=spec["end"],
            interval=1,
            env=env,
        )
        updates = prior.get("updates", [])
        server.create_update_update(
            id=updates[0]["id"] if updates else None,
            table_id=table_id,
            entity_id=refs["entity_year"],
            frequency=1,
            lag=1,
            latest=TODAY,
            env=env,
        )

        written = server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=columns_payload(slug),
            env=env,
        )

        # bulk_upsert_columns neither links a column to its observation level
        # nor sets is_partition, and a bare update_column would clear the flag,
        # so both are written together, per column, here.
        by_name = {
            c["name"]: c["id"]
            for c in server.get_dataset(slug=DATASET_SLUG, env=env)["tables"][
                slug
            ]["columns"]
        }
        partition = PARTITIONS.get(slug, "")
        wanted = dict(spec["level_columns"])
        if partition and partition not in wanted.values():
            wanted["__partition__"] = partition
        for level, column in wanted.items():
            if column not in by_name:
                raise RuntimeError(f"{slug}: no column {column} to link")
            server.update_column(
                column_id=by_name[column],
                column_name=column,
                table_id=table_id,
                observation_level_id=levels.get(level),
                is_partition=column == partition,
                env=env,
            )
        report[slug] = {"id": table_id, "columns": written}
        print(f"table {slug}: {written}")

    server.reorder_tables(
        dataset_slug=DATASET_SLUG,
        table_slugs=[t["slug"] for t in TABLES],
        env=env,
    )
    return report


def set_dataset_status(env: str, status: str) -> dict:
    """Re-register the dataset with a different status.

    The API has no partial update, so every required field is passed again.
    """
    status_id = resolve_refs(env)[f"status_{status}"]
    refs = resolve_refs(env)
    return server.create_update_dataset(
        id=DATASET_ID,
        slug=DATASET_SLUG,
        name_pt=NAME_PT,
        name_en=NAME_EN,
        name_es=NAME_ES,
        description_pt=DESCRIPTION_PT,
        description_en=DESCRIPTION_EN,
        description_es=DESCRIPTION_ES,
        organization_ids=[ORGANIZATION_ID],
        theme_ids=[
            refs["theme_science"],
            refs["theme_education"],
            refs["theme_economics"],
        ],
        tag_ids=resolve_tags(env),
        status_id=status_id,
        env=env,
    )


def rename_organization(env: str) -> dict:
    """Shorten the NSF organization slug to `nsf`.

    The organization also carries an unfilled Survey of Doctorate Recipients
    shell, whose public URL changes with this rename.
    """
    return server.create_update_organization(
        id=ORGANIZATION_ID,
        slug="nsf",
        name_pt="National Science Foundation (NSF)",
        name_en="National Science Foundation (NSF)",
        name_es="Fundación Nacional de la Ciencia (NSF)",
        website="https://nsf.gov",
        env=env,
    )


def main() -> int:
    args = sys.argv[1:]
    env = next((a for a in args if not a.startswith("--")), "staging")
    if env not in {"staging", "prod"}:
        raise SystemExit("env must be 'staging' or 'prod'")
    if "--rename-org" in args:
        print(f"renaming the NSF organization slug on {env}")
        print(rename_organization(env))
        return 0
    if "--publish" in args:
        print(f"publishing us_nsf_ncses on {env}")
        print(set_dataset_status(env, "published"))
        return 0
    print(f"registering us_nsf_ncses metadata on {env}", flush=True)
    report = register(env)
    print(json.dumps(report, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
