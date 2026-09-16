"""Register us_nih_reporter metadata on the Data Basis backend.

Idempotent by construction: every create_update_* call is given the id read back
from get_dataset when the record already exists, because those tools create a
second record when called without one.

Set ``DATABASIS_MCP_PATH`` to the Data Basis MCP checkout before running.

Run: ~/.venvs/bd-pipelines/bin/python register_metadata.py [staging|prod]
     [--materialized]
"""

import json
import sys
from datetime import date
from pathlib import Path

from common import import_mcp_server

server = import_mcp_server()

ARGS = sys.argv[1:]
ENV = next((a for a in ARGS if not a.startswith("-")), "staging")

# Whether this environment's BigQuery tables have actually been built. It gates
# one thing: the table-anchored Update record, whose `latest` means "when Data
# Basis last refreshed this table". Registration alone refreshes nothing, and
# the poll reads that field (`compare_against="table_update"`) with a strict
# `source_max > latest`, so writing today's date here would both claim a
# materialisation that has not happened and suppress the table's first refresh.
# Off by default so a bare re-run can never re-introduce that claim.
MATERIALIZED = "--materialized" in ARGS
COLUMNS_DIR = Path(__file__).resolve().parent / "columns"

DATASET_SLUG = "nih_reporter"
GCP_DATASET = "us_nih_reporter"
GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}[ENV]

AUX_URL = (
    "https://storage.googleapis.com/basedosdados/auxiliary_files/"
    "us_nih_reporter/{table}/auxiliary_files.zip"
)

# ---------------------------------------------------------------- dataset text

DATASET = dict(
    slug=DATASET_SLUG,
    name_pt="NIH RePORTER",
    name_en="NIH RePORTER",
    name_es="NIH RePORTER",
    description_pt=(
        "Todos os projetos de pesquisa financiados pelos National Institutes of "
        "Health desde o ano fiscal de 1985, com os pesquisadores principais, a "
        "instituição beneficiária, o valor concedido, o painel de revisão, o "
        "código de atividade e o instituto administrador, além das publicações, "
        "patentes e estudos clínicos que citam cada projeto como fonte de apoio. "
        "Os arquivos em massa do ExPORTER cobrem também projetos financiados "
        "pela ACF, AHRQ, CDC, FDA, HRSA e Departamento de Assuntos de Veteranos, "
        "mas o valor concedido só está disponível para NIH, CDC, FDA e ACF. O ano "
        "das tabelas project e project_abstract é o ano FISCAL federal, de 1 de "
        "outubro a 30 de setembro e nomeado pelo ano em que termina; o das "
        "tabelas de publicações é o ano-calendário de divulgação do arquivo de "
        "origem. Complementa us_treasury_usaspending, que registra a transação "
        "orçamentária federal sem o detalhe científico presente aqui."
    ),
    description_en=(
        "Every research project funded by the National Institutes of Health "
        "since fiscal year 1985, with the principal investigators, the awardee "
        "institution, the amount awarded, the review panel, the activity code "
        "and the administering institute, alongside the publications, patents "
        "and clinical studies that cite each project as support. The ExPORTER "
        "bulk files also cover projects funded by ACF, AHRQ, CDC, FDA, HRSA and "
        "the Department of Veterans Affairs, but the amount awarded is available "
        "only for NIH, CDC, FDA and ACF. The year in the project and "
        "project_abstract tables is the federal FISCAL year, running from 1 "
        "October to 30 September and named for the year in which it ends; the "
        "year in the publication tables is the calendar year of the source "
        "file's release. Complements us_treasury_usaspending, which records the "
        "federal budget transaction without the scientific detail held here."
    ),
    description_es=(
        "Todos los proyectos de investigación financiados por los National "
        "Institutes of Health desde el año fiscal de 1985, con los "
        "investigadores principales, la institución beneficiaria, el monto "
        "concedido, el panel de revisión, el código de actividad y el instituto "
        "administrador, además de las publicaciones, patentes y estudios "
        "clínicos que citan cada proyecto como fuente de apoyo. Los archivos en "
        "bloque del ExPORTER cubren también proyectos financiados por ACF, AHRQ, "
        "CDC, FDA, HRSA y el Departamento de Asuntos de Veteranos, pero el monto "
        "concedido solo está disponible para NIH, CDC, FDA y ACF. El año de las "
        "tablas project y project_abstract es el año FISCAL federal, del 1 de "
        "octubre al 30 de septiembre y nombrado por el año en que termina; el de "
        "las tablas de publicaciones es el año calendario de divulgación del "
        "archivo de origen. Complementa us_treasury_usaspending, que registra la "
        "transacción presupuestaria federal sin el detalle científico presente "
        "aquí."
    ),
)

# ---------------------------------------------------------------- table text

TABLES = {
    "project": dict(
        name_pt="Projeto",
        name_en="Project",
        name_es="Proyecto",
        description_pt=(
            "Um registro por solicitação financiada e ano FISCAL federal, desde "
            "o ano fiscal de 1985, com título, instituição beneficiária, "
            "pesquisadores principais, código de atividade, instituto "
            "administrador, painel de revisão e custo. O ano fiscal federal vai "
            "de 1 de outubro a 30 de setembro e é nomeado pelo ano em que "
            "termina; não é o ano-calendário. A coluna core_project_num é o que "
            "liga esta tabela a publication_link, patent_link e "
            "clinical_study_link; as colunas de componente do número do projeto "
            "descrevem a concessão como ela está hoje e divergem do número em "
            "3,09% das linhas, porque o instituto administrador muda quando a "
            "concessão é transferida."
        ),
        description_en=(
            "One record per funded application and federal FISCAL year, since "
            "fiscal year 1985, with the title, awardee institution, principal "
            "investigators, activity code, administering institute, review panel "
            "and cost. The federal fiscal year runs from 1 October to 30 "
            "September and is named for the year in which it ends; it is not the "
            "calendar year. The core_project_num column is what joins this table "
            "to publication_link, patent_link and clinical_study_link; the "
            "project number's component columns describe the award as it now "
            "stands and diverge from the number on 3.09% of rows, because the "
            "administering institute moves when an award is transferred."
        ),
        description_es=(
            "Un registro por solicitud financiada y año FISCAL federal, desde el "
            "año fiscal de 1985, con título, institución beneficiaria, "
            "investigadores principales, código de actividad, instituto "
            "administrador, panel de revisión y costo. El año fiscal federal va "
            "del 1 de octubre al 30 de septiembre y se nombra por el año en que "
            "termina; no es el año calendario. La columna core_project_num es la "
            "que une esta tabla con publication_link, patent_link y "
            "clinical_study_link; las columnas de componente del número del "
            "proyecto describen la subvención como está hoy y divergen del "
            "número en el 3,09% de las filas, porque el instituto administrador "
            "cambia cuando la subvención se transfiere."
        ),
    ),
    "project_abstract": dict(
        name_pt="Resumo do projeto",
        name_en="Project abstract",
        name_es="Resumen del proyecto",
        description_pt=(
            "Um registro por solicitação financiada e ano FISCAL federal com o "
            "resumo da pesquisa, ligado a project pelo par "
            "(year, application_id). A fonte publica os resumos em arquivo "
            "separado do arquivo de projetos por causa do tamanho. Nas "
            "concessões o resumo é fornecido ao NIH pelo beneficiário; nem todo "
            "projeto tem resumo publicado."
        ),
        description_en=(
            "One record per funded application and federal FISCAL year carrying "
            "the research abstract, joined to project on the pair "
            "(year, application_id). The source publishes abstracts in a file "
            "separate from the project file because of their size. For grants "
            "the abstract is supplied to NIH by the recipient; not every project "
            "has a published abstract."
        ),
        description_es=(
            "Un registro por solicitud financiada y año FISCAL federal con el "
            "resumen de la investigación, unido a project por el par "
            "(year, application_id). La fuente publica los resúmenes en un "
            "archivo separado del archivo de proyectos por su tamaño. En las "
            "subvenciones el resumen es proporcionado al NIH por el "
            "beneficiario; no todo proyecto tiene resumen publicado."
        ),
    ),
    "publication": dict(
        name_pt="Publicação",
        name_en="Publication",
        name_es="Publicación",
        description_pt=(
            "Um registro por publicação e ano-CALENDÁRIO de divulgação do "
            "arquivo de origem, desde 1980, com título, periódico, autores, "
            "vínculo do primeiro autor e identificadores PubMed e PubMed "
            "Central. O ano desta tabela é o do arquivo de divulgação, e não o "
            "ano fiscal usado em project; 22.930 PMIDs aparecem em mais de um "
            "arquivo anual, e por isso a chave é o par (year, pmid). A ligação "
            "aos projetos que financiaram cada publicação está em "
            "publication_link."
        ),
        description_en=(
            "One record per publication and CALENDAR year of the source file's "
            "release, since 1980, with the title, journal, authors, first "
            "author's affiliation and the PubMed and PubMed Central "
            "identifiers. The year in this table is that of the release file, "
            "not the fiscal year used in project; 22,930 PMIDs appear in more "
            "than one annual file, which is why the key is the pair "
            "(year, pmid). The link to the projects that funded each "
            "publication is in publication_link."
        ),
        description_es=(
            "Un registro por publicación y año CALENDARIO de divulgación del "
            "archivo de origen, desde 1980, con título, revista, autores, "
            "vinculación del primer autor e identificadores PubMed y PubMed "
            "Central. El año de esta tabla es el del archivo de divulgación, y "
            "no el año fiscal usado en project; 22.930 PMID aparecen en más de "
            "un archivo anual, y por eso la clave es el par (year, pmid). El "
            "enlace con los proyectos que financiaron cada publicación está en "
            "publication_link."
        ),
    ),
    "publication_link": dict(
        name_pt="Ligação projeto-publicação",
        name_en="Project-publication link",
        name_es="Enlace proyecto-publicación",
        description_pt=(
            "Um registro por par de publicação e projeto de pesquisa citado "
            "como fonte de apoio, no formato longo, desde o ano-calendário de "
            "1980. Liga publication a project: pmid identifica a publicação e "
            "core_project_num o projeto. A associação vem dos agradecimentos do "
            "artigo, anotados pelo PubMed, ou do sistema de submissão de "
            "manuscritos do NIH, e não identifica um ano do projeto nem um ano "
            "fiscal de financiamento."
        ),
        description_en=(
            "One record per pair of publication and research project cited as "
            "its support, in long format, since calendar year 1980. It joins "
            "publication to project: pmid identifies the publication and "
            "core_project_num the project. The association comes from the "
            "article's acknowledgements, annotated by PubMed, or from the NIH "
            "manuscript submission system, and identifies neither a year of the "
            "project nor a fiscal year of funding."
        ),
        description_es=(
            "Un registro por par de publicación y proyecto de investigación "
            "citado como fuente de apoyo, en formato largo, desde el año "
            "calendario de 1980. Une publication con project: pmid identifica la "
            "publicación y core_project_num el proyecto. La asociación proviene "
            "de los agradecimientos del artículo, anotados por PubMed, o del "
            "sistema de envío de manuscritos del NIH, y no identifica un año del "
            "proyecto ni un año fiscal de financiamiento."
        ),
    ),
    "patent_link": dict(
        name_pt="Ligação projeto-patente",
        name_en="Project-patent link",
        name_es="Enlace proyecto-patente",
        description_pt=(
            "Um registro por par de patente e projeto de pesquisa reconhecido "
            "como apoio ao seu desenvolvimento, no formato longo, com o título "
            "da patente e o nome de seu titular. A fonte publica um único "
            "arquivo cobrindo todos os anos fiscais, e por isso esta tabela não "
            "é particionada por ano. O registro é reconhecidamente incompleto: "
            "só constam patentes concedidas, não pedidos em andamento, e nem "
            "toda organização beneficiária cumpre a obrigação de reportar ao "
            "iEdison depois de encerrado o apoio. Patentes só são reportadas "
            "para projetos do NIH, não para os das demais agências presentes em "
            "project."
        ),
        description_en=(
            "One record per pair of patent and research project acknowledged as "
            "supporting its development, in long format, with the patent's title "
            "and the name of its owner. The source publishes a single file "
            "covering every fiscal year, which is why this table is not "
            "partitioned by year. The record is incomplete by the source's own "
            "account: it lists issued patents only, not applications in "
            "progress, and not every recipient organisation keeps reporting to "
            "iEdison once support has ended. Patents are reported for NIH "
            "projects only, not for the other agencies present in project."
        ),
        description_es=(
            "Un registro por par de patente y proyecto de investigación "
            "reconocido como apoyo a su desarrollo, en formato largo, con el "
            "título de la patente y el nombre de su titular. La fuente publica un "
            "único archivo que cubre todos los años fiscales, y por eso esta "
            "tabla no está particionada por año. El registro es reconocidamente "
            "incompleto: solo constan patentes concedidas, no solicitudes en "
            "curso, y no toda organización beneficiaria cumple la obligación de "
            "reportar al iEdison después de terminado el apoyo. Las patentes solo "
            "se reportan para proyectos del NIH, no para los de las demás "
            "agencias presentes en project."
        ),
    ),
    "clinical_study_link": dict(
        name_pt="Ligação projeto-estudo clínico",
        name_en="Project-clinical study link",
        name_es="Enlace proyecto-estudio clínico",
        description_pt=(
            "Um registro por par de estudo clínico e projeto de pesquisa "
            "reconhecido como seu apoio, no formato longo, com o título e a "
            "situação do estudo. A fonte publica um único arquivo cobrindo todos "
            "os anos fiscais, e por isso esta tabela não é particionada por ano. "
            "A associação vem do próprio ClinicalTrials.gov, que informa ao "
            "RePORTER os números de concessão declarados no registro do estudo, "
            "e não identifica um ano do projeto nem um ano fiscal de "
            "financiamento. A situação descreve o estágio do estudo na data de "
            "extração do arquivo."
        ),
        description_en=(
            "One record per pair of clinical study and research project "
            "acknowledged as its support, in long format, with the study's title "
            "and status. The source publishes a single file covering every fiscal "
            "year, which is why this table is not partitioned by year. The "
            "association comes from ClinicalTrials.gov itself, which reports to "
            "RePORTER the grant numbers entered in the study's registration, and "
            "identifies neither a year of the project nor a fiscal year of "
            "funding. The status describes the study's stage on the file's "
            "extraction date."
        ),
        description_es=(
            "Un registro por par de estudio clínico y proyecto de investigación "
            "reconocido como su apoyo, en formato largo, con el título y la "
            "situación del estudio. La fuente publica un único archivo que cubre "
            "todos los años fiscales, y por eso esta tabla no está particionada "
            "por año. La asociación proviene del propio ClinicalTrials.gov, que "
            "informa al RePORTER los números de subvención declarados en el "
            "registro del estudio, y no identifica un año del proyecto ni un año "
            "fiscal de financiamiento. La situación describe la etapa del estudio "
            "en la fecha de extracción del archivo."
        ),
    ),
    "dicionario": dict(
        name_pt="Dicionário",
        name_en="Dictionary",
        name_es="Diccionario",
        description_pt=(
            "Registro dos valores assumidos pelas colunas codificadas da tabela "
            "project, com a cobertura temporal de cada valor em anos fiscais. "
            "Cobre application_type e arra_funded a partir do dicionário de dados "
            "publicado pelo NIH, activity a partir do registro oficial de códigos "
            "de atividade, e administering_ic a partir da própria coluna ic_name "
            "dos dados. A coluna valor fica vazia nos códigos de atividade que o "
            "registro oficial não cobre, que são os de contratos, projetos "
            "intramuros e agências que não o NIH."
        ),
        description_en=(
            "Register of the values taken by the coded columns of the project "
            "table, with each value's temporal coverage in fiscal years. It "
            "covers application_type and arra_funded from the data dictionary NIH "
            "publishes, activity from the official activity code register, and "
            "administering_ic from the data's own ic_name column. The valor "
            "column is blank for the activity codes the official register does "
            "not cover, which are those of contracts, intramural projects and "
            "agencies other than NIH."
        ),
        description_es=(
            "Registro de los valores que asumen las columnas codificadas de la "
            "tabla project, con la cobertura temporal de cada valor en años "
            "fiscales. Cubre application_type y arra_funded a partir del "
            "diccionario de datos publicado por el NIH, activity a partir del "
            "registro oficial de códigos de actividad, y administering_ic a "
            "partir de la propia columna ic_name de los datos. La columna valor "
            "queda vacía en los códigos de actividad que el registro oficial no "
            "cubre, que son los de contratos, proyectos intramuros y agencias "
            "distintas del NIH."
        ),
    ),
}

TABLE_ORDER = [
    "project",
    "project_abstract",
    "publication",
    "publication_link",
    "patent_link",
    "clinical_study_link",
    "dicionario",
]

# entity slug -> the column that identifies that level, per table
OBSERVATION_LEVELS = {
    "project": [
        ("year", "year"),
        ("project", "application_id"),
        ("country", "org_country"),
    ],
    "project_abstract": [("year", "year"), ("project", "application_id")],
    "publication": [("year", "year"), ("article", "pmid")],
    "publication_link": [
        ("year", "year"),
        ("article", "pmid"),
        ("project", "core_project_num"),
    ],
    "patent_link": [("patent", "patent_id"), ("project", "core_project_num")],
    "clinical_study_link": [
        ("clinical_study", "nct_id"),
        ("project", "core_project_num"),
    ],
    "dicionario": [],
}

# Observation-level entities this dataset needs that the backend vocabulary does
# not already carry, as slug -> (category slug, name_pt, name_en, name_es).
# `clinical_study` joins the `health` category alongside aih,
# health_care_provider and notification. It is shared reference data, so it is
# created once per environment and reused, never re-created per dataset — and
# its id differs between staging and prod, like several other entities, so it is
# always resolved by slug rather than hardcoded.
NEW_ENTITIES = {
    "clinical_study": (
        "health",
        "Estudo clínico",
        "Clinical study",
        "Estudio clínico",
    ),
}

DATETIME_RANGES = {
    "project": (1985, 2025),
    "project_abstract": (1985, 2025),
    "publication": (1980, 2025),
    "publication_link": (1980, 2025),
}

RAW_SOURCES = [
    dict(
        key="annual",
        name_pt="ExPORTER — arquivos anuais em massa",
        name_en="ExPORTER — annual bulk files",
        name_es="ExPORTER — archivos anuales en bloque",
        url="https://reporter.nih.gov/exporter",
        description_pt=(
            "Arquivos anuais de projetos, resumos, publicações e tabelas de "
            "ligação, em CSV compactado. Reconstruídos ao final de cada ano "
            "fiscal, quando os três anos fiscais anteriores também são "
            "reprocessados."
        ),
        description_en=(
            "Annual files of projects, abstracts, publications and link tables, "
            "as compressed CSV. Rebuilt at the close of each fiscal year, when "
            "the three prior fiscal years are restated as well."
        ),
        description_es=(
            "Archivos anuales de proyectos, resúmenes, publicaciones y tablas de "
            "enlace, en CSV comprimido. Reconstruidos al cierre de cada año "
            "fiscal, cuando los tres años fiscales anteriores también se "
            "reprocesan."
        ),
        tables=[
            "project",
            "project_abstract",
            "publication",
            "publication_link",
            "dicionario",
        ],
        latest="2026-07-09",
    ),
    dict(
        key="links",
        name_pt="ExPORTER — patentes e estudos clínicos",
        name_en="ExPORTER — patents and clinical studies",
        name_es="ExPORTER — patentes y estudios clínicos",
        url="https://reporter.nih.gov/exporter/patents",
        description_pt=(
            "Arquivo único de patentes e arquivo único de estudos clínicos, "
            "ambos cobrindo todos os anos fiscais e reescritos aproximadamente "
            "toda semana."
        ),
        description_en=(
            "One file of patents and one of clinical studies, both covering "
            "every fiscal year and rewritten roughly weekly."
        ),
        description_es=(
            "Un archivo de patentes y uno de estudios clínicos, ambos cubriendo "
            "todos los años fiscales y reescritos aproximadamente cada semana."
        ),
        tables=["patent_link", "clinical_study_link"],
        latest="2026-09-07",
    ),
    dict(
        key="api",
        name_pt="API do RePORTER",
        name_en="RePORTER API",
        name_es="API del RePORTER",
        url="https://api.reporter.nih.gov/",
        description_pt=(
            "Interface de programação do RePORTER, com os mesmos projetos e "
            "publicações dos arquivos em massa, para consultas incrementais. Não "
            "é a fonte das tabelas deste conjunto, que vêm dos arquivos do "
            "ExPORTER."
        ),
        description_en=(
            "The RePORTER programming interface, carrying the same projects and "
            "publications as the bulk files, for incremental queries. It is not "
            "the source of this dataset's tables, which come from the ExPORTER "
            "files."
        ),
        description_es=(
            "Interfaz de programación del RePORTER, con los mismos proyectos y "
            "publicaciones de los archivos en bloque, para consultas "
            "incrementales. No es la fuente de las tablas de este conjunto, que "
            "provienen de los archivos del ExPORTER."
        ),
        tables=[],
        latest=None,
    ),
]

# Tags, as the alternative slugs the same tag carries in each environment.
# Staging's vocabulary is Portuguese and prod's is English, but the records are
# the same — `pesquisa` on staging and `research` on prod share the UUID
# 4ae52b90-…. So the tag is resolved by trying each spelling and taking the one
# that environment has, rather than by hardcoding either. Every one of the nine
# already exists in both; none is minted here.
TAGS = [
    ("pesquisa", "research"),
    ("financiamento", "financing"),
    ("gasto", "spending"),
    ("academia",),
    ("publicacao", "publication"),
    ("propriedade_intelectual", "intellectual_property"),
    ("medicina", "medicine"),
    ("inovacao", "innovation"),
    ("federal",),
]


def resolve_tags(available: dict) -> list[str]:
    out = []
    for alts in TAGS:
        hit = next((available[s] for s in alts if s in available), None)
        if hit is None:
            raise SystemExit(
                f"no tag found for any of {alts} in this environment"
            )
        out.append(hit)
    return out


THEMES = ["health", "science-technology"]


def main() -> int:
    print(f"env={ENV}")
    ids = server.discover_ids(
        env=ENV,
        keys=["status", "license", "availability", "entity", "theme", "tag"],
    )
    account = server.get_authenticated_account(env=ENV)["id"]
    area_us = server.lookup_id(category="area", slug="us", env=ENV)["id"]
    org = server.lookup_id(
        category="organization",
        slug="national_institutes_of_health_nih",
        env=ENV,
    )["id"]
    status_under_review = ids["status"]["under_review"]
    status_published = ids["status"]["published"]
    license_cc0 = ids["license"]["cc0"]
    availability_online = ids["availability"]["online"]
    tag_ids = resolve_tags(ids["tag"])
    entity = dict(ids["entity"])
    print(f"account={account} org={org} area={area_us}")

    # Entity vocabulary this dataset needs but the backend does not have yet.
    # discover_ids cannot read entity categories — its query asks for
    # `allEntityCategory` and the schema calls the field `allEntitycategory` —
    # so the category is resolved through _gql instead.
    cats = {
        e["node"]["slug"]: server._strip_id(e["node"]["id"])
        for e in server._gql(
            "query { allEntitycategory { edges { node { id slug } } } }",
            {},
            env=ENV,
        )["allEntitycategory"]["edges"]
    }
    for slug, (category, name_pt, name_en, name_es) in NEW_ENTITIES.items():
        if slug in entity:
            print(f"entity {slug} -> {entity[slug]} (existing)")
            continue
        eid = server.create_update_entity(
            slug=slug,
            name_pt=name_pt,
            name_en=name_en,
            name_es=name_es,
            category_id=cats[category],
            env=ENV,
        )["id"]
        entity[slug] = eid
        print(f"entity {slug} -> {eid} (created under {category})")

    existing = server.get_dataset(slug=DATASET_SLUG, env=ENV)

    ds = server.create_update_dataset(
        id=existing.get("id"),
        **DATASET,
        organization_ids=[org],
        theme_ids=[ids["theme"][t] for t in THEMES],
        tag_ids=tag_ids,
        status_id=status_under_review,
        env=ENV,
    )
    dataset_id = ds["id"]
    print(f"dataset {DATASET_SLUG} -> {dataset_id}")

    # raw data sources ------------------------------------------------------
    listed = server.get_raw_data_sources(dataset_slug=DATASET_SLUG, env=ENV)
    if isinstance(listed, dict):
        listed = listed.get("result", [])
    have = {s["url"]: s["id"] for s in listed}
    raw_ids = {}
    for spec in RAW_SOURCES:
        rid = server.create_update_raw_data_source(
            id=have.get(spec["url"]),
            dataset_id=dataset_id,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            description_en=spec["description_en"],
            description_es=spec["description_es"],
            url=spec["url"],
            license_id=license_cc0,
            availability_id=availability_online,
            has_structured_data=True,
            is_free=True,
            contains_api=spec["key"] == "api",
            requires_registration=False,
            env=ENV,
        )["id"]
        raw_ids[spec["key"]] = rid
        print(f"raw source {spec['key']} -> {rid}")

    # tables ----------------------------------------------------------------
    existing = server.get_dataset(slug=DATASET_SLUG, env=ENV)
    prior = existing.get("tables", {})
    table_ids = {}
    for table in TABLE_ORDER:
        p = prior.get(table, {})
        tid = server.create_update_table(
            id=p.get("id"),
            slug=table,
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            auxiliary_files_url=AUX_URL.format(table=table),
            env=ENV,
            **TABLES[table],
        )["id"]
        table_ids[table] = tid
        print(f"\ntable {table} -> {tid}")

        # Observation levels. An existing record is reused whenever one exists,
        # including when its entity has changed: the entity is rewritten in
        # place on the same observation-level id, rather than a second record
        # being created beside the first. There is no MCP tool to delete an
        # observation level, so a stale one would have to be removed by hand
        # through GraphQL — and the column FK points at the id, so reusing it
        # also keeps the column link intact.
        want = [slug for slug, _c in OBSERVATION_LEVELS[table]]
        have_ol = {
            o.get("entity_id"): o["id"]
            for o in p.get("observation_levels", [])
        }
        spare = [
            oid
            for eid, oid in have_ol.items()
            if eid not in {entity[s] for s in want}
        ]
        ol_ids = {}
        for slug, _col in OBSERVATION_LEVELS[table]:
            eid = entity[slug]
            oid = have_ol.get(eid) or (spare.pop(0) if spare else None)
            reused = " (rewritten)" if oid and eid not in have_ol else ""
            ol_ids[slug] = server.create_update_observation_level(
                id=oid, table_id=tid, entity_id=eid, env=ENV
            )["id"]
            if reused:
                print(f"  observation level {slug}{reused}")
        if ol_ids:
            print(f"  observation levels: {list(ol_ids)}")
        if spare:
            print(
                f"  WARNING: {len(spare)} stale observation level(s) left: {spare}"
            )

        # columns
        with open(
            COLUMNS_DIR / f"columns_{table}.json", encoding="utf-8"
        ) as fh:
            payload = json.load(fh)
        res = server.bulk_upsert_columns(
            table_id=tid,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=ENV,
        )
        print(f"  columns: {res}")

        # partition flag and observation-level column links. bulk_upsert_columns
        # does not set either, and update_column needs the column's id, so the
        # dataset is re-read here rather than guessed.
        col_ids = {
            c["name"]: c["id"]
            for c in server.get_dataset(slug=DATASET_SLUG, env=ENV)["tables"][
                table
            ]["columns"]
        }
        linked = {col for _s, col in OBSERVATION_LEVELS[table]}
        for slug, col in OBSERVATION_LEVELS[table]:
            server.update_column(
                column_id=col_ids[col],
                column_name=col,
                table_id=tid,
                observation_level_id=ol_ids[slug],
                is_partition=(col == "year"),
                env=ENV,
            )
        if table in DATETIME_RANGES and "year" not in linked:
            server.update_column(
                column_id=col_ids["year"],
                column_name="year",
                table_id=tid,
                is_partition=True,
                env=ENV,
            )

        # cloud table
        have_ct = [c["id"] for c in p.get("cloud_tables", [])]
        server.create_update_cloud_table(
            id=have_ct[0] if have_ct else None,
            table_id=tid,
            gcp_project_id=GCP_PROJECT,
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            env=ENV,
        )

        # coverage + datetime range
        have_cov = [
            c for c in p.get("coverages", []) if c.get("area_slug") == "us"
        ]
        cov = server.create_update_coverage(
            id=have_cov[0]["id"] if have_cov else None,
            table_id=tid,
            area_id=area_us,
            env=ENV,
        )["id"]
        if table in DATETIME_RANGES:
            start, end = DATETIME_RANGES[table]
            have_dt = (
                have_cov[0].get("datetime_ranges", []) if have_cov else []
            )
            server.create_update_datetime_range(
                id=have_dt[0]["id"] if have_dt else None,
                coverage_id=cov,
                start_year=start,
                end_year=end,
                interval=1,
                is_closed=False,
                env=ENV,
            )

        # Table-anchored Update: when Data Basis last refreshed the table.
        # Written only when this environment's tables really have been built —
        # see MATERIALIZED above. On prod they are materialised by the
        # table-approve action on merge, not by this script, so a bare prod
        # registration leaves the record alone and the first materialisation
        # (or `register_table_materialization_task`) sets it.
        if MATERIALIZED:
            have_up = [u["id"] for u in p.get("updates", [])]
            server.create_update_update(
                id=have_up[0] if have_up else None,
                table_id=tid,
                entity_id=entity["year"],
                frequency=1,
                latest=f"{date.today()}T00:00:00",
                env=ENV,
            )
        print(
            "  cloud table, coverage, datetime range"
            + (", update: ok" if MATERIALIZED else " (update skipped): ok")
        )

    # deferred raw source links --------------------------------------------
    for spec in RAW_SOURCES:
        for table in spec["tables"]:
            server.create_update_table(
                id=table_ids[table],
                slug=table,
                dataset_id=dataset_id,
                status_id=status_published,
                published_by_ids=[account],
                data_cleaned_by_ids=[account],
                auxiliary_files_url=AUX_URL.format(table=table),
                raw_data_source_ids=[raw_ids[spec["key"]]],
                env=ENV,
                **TABLES[table],
            )
        # raw-source Update: what the source last published. This is the same
        # publication timestamp the pipeline's commit_source_update_task writes,
        # because the poll compares Last-Modified against Table.Update.latest.
        if spec["latest"]:
            server.create_update_update(
                raw_data_source_id=raw_ids[spec["key"]],
                entity_id=entity["year"]
                if spec["key"] == "annual"
                else entity["week"],
                frequency=1,
                latest=f"{spec['latest']}T00:00:00",
                env=ENV,
            )
    print("\nraw source links and source updates: ok")

    server.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=ENV
    )
    print(f"table order set: {TABLE_ORDER}")

    # publish on staging only — prod stays under_review until the PR merges
    if ENV in ("staging", "dev"):
        server.create_update_dataset(
            id=dataset_id,
            **DATASET,
            organization_ids=[org],
            theme_ids=[ids["theme"][t] for t in THEMES],
            tag_ids=tag_ids,
            status_id=status_published,
            env=ENV,
        )
        print("staging dataset published")

    print("\ndone")
    return 0


if __name__ == "__main__":
    sys.exit(main())
