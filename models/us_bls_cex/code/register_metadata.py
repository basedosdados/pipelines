"""Register us_bls_cex metadata in the Data Basis backend.

    ~/.pyenv/versions/3.11.6/bin/python models/us_bls_cex/code/register_metadata.py --env dev
    ~/.pyenv/versions/3.11.6/bin/python models/us_bls_cex/code/register_metadata.py --env prod

Needs an interpreter that imports the databasis MCP ``server.py`` (fastmcp).
Columns come from ``code/architecture/*.csv`` (English) plus
``code/translations.json`` (Portuguese, Spanish), so the backend, dbt models and
parquet schema derive from one source. The MCP tool functions are called
directly, which keeps ~1 MB of column JSON out of the conversation.

Idempotency: ``create_update_*`` duplicates child records when called without
an id, so every id is read back from ``get_dataset`` and reused.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
import sys
from pathlib import Path

MCP_REPO = os.environ.get(
    "DATABASIS_MCP_REPO",
    str(Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)
if not (Path(MCP_REPO) / "server.py").exists():
    raise SystemExit(f"databasis MCP server not found at {MCP_REPO}")
sys.path.insert(0, MCP_REPO)

# pyrefly: ignore [missing-import]
import server  # noqa: E402

CODE = Path(__file__).resolve().parent
ARCH = CODE / "architecture"
TRANSLATIONS = json.loads(
    (CODE / "translations.json").read_text(encoding="utf-8")
)

# Update.latest is a DateTime; a bare date is rejected.
TODAY = datetime.date.today().isoformat() + "T00:00:00+00:00"

SLUG = "cex"
GCP_DATASET_ID = "us_bls_cex"
LAST_RELEASE = 2024

# Organization slugs differ per backend for the same record.
ORGANIZATION = {"prod": "bls", "staging": "us-bls", "dev": "bls"}
THEMES = ["economics", "population"]
# Content tags. Resolved by their prod slug: tag UUIDs are shared across
# backends, slugs are not (staging names them in Portuguese).
TAGS = [
    "consumption",
    "expenditure",
    "spending",
    "income",
    "cost-of-living",
    "poverty",
    "family",
    "inequality",
    "wealth",
]

DATASET_TEXT = {
    "name_pt": "Pesquisas de Despesas do Consumidor (CE)",
    "name_en": "Consumer Expenditure Surveys (CE)",
    "name_es": "Encuestas de Gastos del Consumidor (CE)",
    "description_en": (
        "Expenditures, income and characteristics of US consumer units from the "
        "Bureau of Labor Statistics Consumer Expenditure Surveys, collected by the "
        "Census Bureau in two surveys: a quarterly Interview Survey for large and "
        "recurring purchases and a two-week Diary Survey for small, frequent ones. "
        "Includes the published annual estimates by demographic group from 1984 "
        "and the public-use microdata from 1996, with final and replicate weights. "
        "The CE is the source of the Consumer Price Index expenditure weights."
    ),
    "description_pt": (
        "Despesas, renda e características das unidades de consumo dos Estados "
        "Unidos, das Pesquisas de Despesas do Consumidor do Bureau of Labor "
        "Statistics, coletadas pelo Census Bureau em duas pesquisas: uma Pesquisa "
        "de Entrevista trimestral para compras grandes e recorrentes e uma Pesquisa "
        "de Diário de duas semanas para compras pequenas e frequentes. Inclui as "
        "estimativas anuais publicadas por grupo demográfico desde 1984 e os "
        "microdados de uso público desde 1996, com pesos finais e de replicação. A "
        "CE é a fonte dos pesos de despesa do Índice de Preços ao Consumidor."
    ),
    "description_es": (
        "Gastos, ingresos y características de las unidades de consumo de Estados "
        "Unidos, de las Encuestas de Gastos del Consumidor del Bureau of Labor "
        "Statistics, recolectadas por el Census Bureau en dos encuestas: una "
        "Encuesta de Entrevista trimestral para compras grandes y recurrentes y una "
        "Encuesta de Diario de dos semanas para compras pequeñas y frecuentes. "
        "Incluye las estimaciones anuales publicadas por grupo demográfico desde "
        "1984 y los microdatos de uso público desde 1996, con ponderaciones finales "
        "y de réplica. La CE es la fuente de las ponderaciones de gasto del Índice "
        "de Precios al Consumidor."
    ),
}

PUMD_NOTE_EN = (
    " Collection quarters that BLS ships in two releases before 2020 are loaded "
    "once, from the release of their own year. Missing values (blank or '.') are "
    "NULL; all-digit codes are stored without leading zeros so that a code reads "
    "the same in every year."
)
PUMD_NOTE_PT = (
    " Trimestres de coleta que o BLS publica em dois lançamentos antes de 2020 "
    "são carregados uma vez, a partir do lançamento do próprio ano. Valores "
    "ausentes (vazio ou '.') são NULL; códigos só com dígitos são armazenados sem "
    "zeros à esquerda, para que um código seja lido igual em todos os anos."
)
PUMD_NOTE_ES = (
    " Los trimestres de recolección que el BLS publica en dos entregas antes de "
    "2020 se cargan una sola vez, desde la entrega de su propio año. Los valores "
    "faltantes (vacío o '.') son NULL; los códigos solo con dígitos se guardan sin "
    "ceros a la izquierda, para que un código se lea igual en todos los años."
)


def t(name_en, name_pt, name_es, en, pt, es, *, pumd=False, **extra):
    if pumd:
        en, pt, es = en + PUMD_NOTE_EN, pt + PUMD_NOTE_PT, es + PUMD_NOTE_ES
    return {
        "name_en": name_en,
        "name_pt": name_pt,
        "name_es": name_es,
        "description_en": en,
        "description_pt": pt,
        "description_es": es,
        **extra,
    }


# entities: observation levels, with the column linked to each (None = no column)
TABLE_TEXT = {
    "annual": t(
        "Annual published estimates",
        "Estimativas anuais publicadas",
        "Estimaciones anuales publicadas",
        "Annual estimates published by BLS from the integrated Interview and Diary "
        "data, one row per series and year from 1984: mean expenditure, income or "
        "characteristic per consumer unit for each item and demographic group, with "
        "standard error, relative standard error, expenditure share and percent "
        "reporting from 2010 and aggregate expenditure from 2011. Series are "
        "described in the series table.",
        "Estimativas anuais publicadas pelo BLS a partir dos dados integrados das "
        "pesquisas de Entrevista e de Diário, uma linha por série e ano desde 1984: "
        "despesa, renda ou característica média por unidade de consumo para cada "
        "item e grupo demográfico, com erro padrão, erro padrão relativo, "
        "participação na despesa e percentual de declarantes desde 2010 e despesa "
        "agregada desde 2011. As séries são descritas na tabela series.",
        "Estimaciones anuales publicadas por el BLS a partir de los datos "
        "integrados de las encuestas de Entrevista y de Diario, una fila por serie "
        "y año desde 1984: gasto, ingreso o característica media por unidad de "
        "consumo para cada ítem y grupo demográfico, con error estándar, error "
        "estándar relativo, participación en el gasto y porcentaje de declarantes "
        "desde 2010 y gasto agregado desde 2011. Las series se describen en la "
        "tabla series.",
        entities={"year": "year", "series": "series_id"},
        source="labstat",
        coverage=((1984, None), (2024, None)),
    ),
    "series": t(
        "Published series",
        "Séries publicadas",
        "Series publicadas",
        "Catalogue of the series in the BLS LABSTAT database cx, one row per "
        "series, with the item, demographic classification and group each series "
        "is cut by and its first and last year.",
        "Catálogo das séries do banco de dados LABSTAT cx do BLS, uma linha por "
        "série, com o item, a classificação demográfica e o grupo de cada série e "
        "seus primeiro e último anos.",
        "Catálogo de las series de la base de datos LABSTAT cx del BLS, una fila "
        "por serie, con el ítem, la clasificación demográfica y el grupo de cada "
        "serie y sus primer y último años.",
        entities={"series": "series_id"},
        source="labstat",
        coverage=None,
    ),
    "interview_household": t(
        "Interview Survey: consumer units",
        "Pesquisa de Entrevista: unidades de consumo",
        "Encuesta de Entrevista: unidades de consumo",
        "Interview Survey public-use microdata, consumer-unit file (FMLI): one row "
        "per consumer unit interview from 1996 Q1 to 2025 Q1, with "
        "characteristics, income, assets, quarterly summary expenditures, the "
        "final weight and 44 replicate weights. Variables keep their BLS names, "
        "lowercased, except keys, dates and weights; each column carries the years "
        "it exists.",
        "Microdados de uso público da Pesquisa de Entrevista, arquivo de unidades "
        "de consumo (FMLI): uma linha por entrevista de unidade de consumo, do 1º "
        "trimestre de 1996 ao 1º trimestre de 2025, com características, renda, "
        "ativos, despesas trimestrais resumidas, o peso final e 44 pesos de "
        "replicação. As variáveis mantêm os nomes do BLS, em minúsculas, exceto "
        "chaves, datas e pesos; cada coluna informa os anos em que existe.",
        "Microdatos de uso público de la Encuesta de Entrevista, archivo de "
        "unidades de consumo (FMLI): una fila por entrevista de unidad de consumo, "
        "del 1.er trimestre de 1996 al 1.er trimestre de 2025, con "
        "características, ingresos, activos, gastos trimestrales resumidos, la "
        "ponderación final y 44 ponderaciones de réplica. Las variables mantienen "
        "los nombres del BLS, en minúsculas, salvo claves, fechas y "
        "ponderaciones; cada columna indica los años en que existe.",
        pumd=True,
        entities={"year": "year", "quarter": "quarter", "household": "newid"},
        source="interview",
        coverage=((1996, 1), (2025, 3)),
    ),
    "interview_member": t(
        "Interview Survey: members",
        "Pesquisa de Entrevista: membros",
        "Encuesta de Entrevista: miembros",
        "Interview Survey public-use microdata, member file (MEMI): one row per "
        "consumer unit member per interview from 1996 Q1 to 2025 Q1, with "
        "demographics, work and income. Join to interview_household on newid.",
        "Microdados de uso público da Pesquisa de Entrevista, arquivo de membros "
        "(MEMI): uma linha por membro da unidade de consumo por entrevista, do 1º "
        "trimestre de 1996 ao 1º trimestre de 2025, com dados demográficos, de "
        "trabalho e de renda. Liga-se a interview_household por newid.",
        "Microdatos de uso público de la Encuesta de Entrevista, archivo de "
        "miembros (MEMI): una fila por miembro de la unidad de consumo por "
        "entrevista, del 1.er trimestre de 1996 al 1.er trimestre de 2025, con "
        "datos demográficos, de trabajo y de ingresos. Se une a "
        "interview_household por newid.",
        pumd=True,
        entities={
            "year": "year",
            "quarter": "quarter",
            "person": "member_number",
        },
        source="interview",
        coverage=((1996, 1), (2025, 3)),
    ),
    "interview_expenditure": t(
        "Interview Survey: monthly expenditures",
        "Pesquisa de Entrevista: despesas mensais",
        "Encuesta de Entrevista: gastos mensuales",
        "Interview Survey public-use microdata, monthly expenditure file (MTBI): "
        "one row per consumer unit interview, reference month and expenditure "
        "record from 1996 Q1 to 2025 Q1, with the cost coded by Universal "
        "Classification Code (UCC). The ucc table maps UCCs to the published "
        "category tree.",
        "Microdados de uso público da Pesquisa de Entrevista, arquivo de despesas "
        "mensais (MTBI): uma linha por entrevista de unidade de consumo, mês de "
        "referência e registro de despesa, do 1º trimestre de 1996 ao 1º "
        "trimestre de 2025, com o custo codificado pelo Universal Classification "
        "Code (UCC). A tabela ucc relaciona os UCCs à árvore de categorias "
        "publicada.",
        "Microdatos de uso público de la Encuesta de Entrevista, archivo de gastos "
        "mensuales (MTBI): una fila por entrevista de unidad de consumo, mes de "
        "referencia y registro de gasto, del 1.er trimestre de 1996 al 1.er "
        "trimestre de 2025, con el costo codificado por el Universal "
        "Classification Code (UCC). La tabla ucc relaciona los UCC con el árbol de "
        "categorías publicado.",
        pumd=True,
        entities={
            "year": "year",
            "quarter": "quarter",
            "household": "newid",
            "item": "ucc",
        },
        source="interview",
        coverage=((1996, 1), (2025, 3)),
    ),
    "interview_income": t(
        "Interview Survey: monthly income",
        "Pesquisa de Entrevista: renda mensal",
        "Encuesta de Entrevista: ingresos mensuales",
        "Interview Survey public-use microdata, monthly income file (ITBI): one "
        "row per consumer unit interview, reference month and income UCC from 1996 "
        "Q1 to 2025 Q1.",
        "Microdados de uso público da Pesquisa de Entrevista, arquivo de renda "
        "mensal (ITBI): uma linha por entrevista de unidade de consumo, mês de "
        "referência e UCC de renda, do 1º trimestre de 1996 ao 1º trimestre de "
        "2025.",
        "Microdatos de uso público de la Encuesta de Entrevista, archivo de "
        "ingresos mensuales (ITBI): una fila por entrevista de unidad de consumo, "
        "mes de referencia y UCC de ingreso, del 1.er trimestre de 1996 al 1.er "
        "trimestre de 2025.",
        pumd=True,
        entities={
            "year": "year",
            "quarter": "quarter",
            "household": "newid",
            "item": "ucc",
        },
        source="interview",
        coverage=((1996, 1), (2025, 3)),
    ),
    "diary_household": t(
        "Diary Survey: consumer units",
        "Pesquisa de Diário: unidades de consumo",
        "Encuesta de Diario: unidades de consumo",
        "Diary Survey public-use microdata, consumer-unit file (FMLD): one row per "
        "consumer unit diary week from 1996 to 2024, with characteristics, income, "
        "weekly summary expenditures, the final weight and 44 replicate weights.",
        "Microdados de uso público da Pesquisa de Diário, arquivo de unidades de "
        "consumo (FMLD): uma linha por semana de diário de unidade de consumo, de "
        "1996 a 2024, com características, renda, despesas semanais resumidas, o "
        "peso final e 44 pesos de replicação.",
        "Microdatos de uso público de la Encuesta de Diario, archivo de unidades "
        "de consumo (FMLD): una fila por semana de diario de unidad de consumo, de "
        "1996 a 2024, con características, ingresos, gastos semanales resumidos, "
        "la ponderación final y 44 ponderaciones de réplica.",
        pumd=True,
        entities={"year": "year", "quarter": "quarter", "household": "newid"},
        source="diary",
        coverage=((1996, 1), (2024, 12)),
    ),
    "diary_member": t(
        "Diary Survey: members",
        "Pesquisa de Diário: membros",
        "Encuesta de Diario: miembros",
        "Diary Survey public-use microdata, member file (MEMD): one row per "
        "consumer unit member per diary week from 1996 to 2024. Join to "
        "diary_household on newid.",
        "Microdados de uso público da Pesquisa de Diário, arquivo de membros "
        "(MEMD): uma linha por membro da unidade de consumo por semana de diário, "
        "de 1996 a 2024. Liga-se a diary_household por newid.",
        "Microdatos de uso público de la Encuesta de Diario, archivo de miembros "
        "(MEMD): una fila por miembro de la unidad de consumo por semana de "
        "diario, de 1996 a 2024. Se une a diary_household por newid.",
        pumd=True,
        entities={
            "year": "year",
            "quarter": "quarter",
            "person": "member_number",
        },
        source="diary",
        coverage=((1996, 1), (2024, 12)),
    ),
    "diary_expenditure": t(
        "Diary Survey: expenditures",
        "Pesquisa de Diário: despesas",
        "Encuesta de Diario: gastos",
        "Diary Survey public-use microdata, expenditure file (EXPD): one row per "
        "item purchased during the diary week from 1996 to 2024, coded by "
        "Universal Classification Code (UCC).",
        "Microdados de uso público da Pesquisa de Diário, arquivo de despesas "
        "(EXPD): uma linha por item comprado durante a semana de diário, de 1996 "
        "a 2024, codificado pelo Universal Classification Code (UCC).",
        "Microdatos de uso público de la Encuesta de Diario, archivo de gastos "
        "(EXPD): una fila por ítem comprado durante la semana de diario, de 1996 a "
        "2024, codificado por el Universal Classification Code (UCC).",
        pumd=True,
        entities={
            "year": "year",
            "quarter": "quarter",
            "household": "newid",
            "item": "ucc",
        },
        source="diary",
        coverage=((1996, 1), (2024, 12)),
    ),
    "diary_income": t(
        "Diary Survey: income",
        "Pesquisa de Diário: renda",
        "Encuesta de Diario: ingresos",
        "Diary Survey public-use microdata, income file (DTBD): one row per "
        "consumer unit diary week and income UCC from 1996 to 2024.",
        "Microdados de uso público da Pesquisa de Diário, arquivo de renda "
        "(DTBD): uma linha por semana de diário de unidade de consumo e UCC de "
        "renda, de 1996 a 2024.",
        "Microdatos de uso público de la Encuesta de Diario, archivo de ingresos "
        "(DTBD): una fila por semana de diario de unidad de consumo y UCC de "
        "ingreso, de 1996 a 2024.",
        pumd=True,
        entities={
            "year": "year",
            "quarter": "quarter",
            "household": "newid",
            "item": "ucc",
        },
        source="diary",
        coverage=((1996, 1), (2024, 12)),
    ),
    "ucc": t(
        "UCC hierarchical groupings",
        "Agrupamentos hierárquicos de UCC",
        "Agrupaciones jerárquicas de UCC",
        "BLS hierarchical grouping files, one row per line per year and grouping "
        "(integrated 1996-2024, interview and diary 1997-2024), mapping Universal "
        "Classification Codes (UCC) to the published expenditure and income "
        "category tree, with each row's level and parent.",
        "Arquivos de agrupamento hierárquico do BLS, uma linha por linha do "
        "arquivo por ano e agrupamento (integrado 1996-2024, entrevista e diário "
        "1997-2024), que relacionam os Universal Classification Codes (UCC) à "
        "árvore publicada de categorias de despesa e renda, com o nível e o pai "
        "de cada linha.",
        "Archivos de agrupación jerárquica del BLS, una fila por línea del archivo "
        "por año y agrupación (integrada 1996-2024, entrevista y diario "
        "1997-2024), que relacionan los Universal Classification Codes (UCC) con "
        "el árbol publicado de categorías de gasto e ingreso, con el nivel y el "
        "padre de cada fila.",
        entities={"year": "year", "item": "ucc"},
        source="stubs",
        coverage=((1996, None), (2024, None)),
    ),
    "dicionario": t(
        "Dictionary",
        "Dicionário",
        "Diccionario",
        "Labels of the coded values in the us_bls_cex tables, from the BLS PUMD "
        "dictionary, the BLS flag codes and the LABSTAT mapping files.",
        "Rótulos dos valores codificados das tabelas de us_bls_cex, a partir do "
        "dicionário dos microdados do BLS, dos códigos de flag do BLS e dos "
        "arquivos de mapeamento do LABSTAT.",
        "Etiquetas de los valores codificados de las tablas de us_bls_cex, a "
        "partir del diccionario de los microdatos del BLS, de los códigos de flag "
        "del BLS y de los archivos de mapeo de LABSTAT.",
        entities={},
        source=None,
        coverage=None,
    ),
}

TABLE_ORDER = [
    "annual",
    "series",
    "interview_household",
    "interview_member",
    "interview_expenditure",
    "interview_income",
    "diary_household",
    "diary_member",
    "diary_expenditure",
    "diary_income",
    "ucc",
    "dicionario",
]

# One raw data source per table (the client raises on two or more).
RAW_SOURCES = {
    "labstat": (
        "LABSTAT time series database (cx)",
        "Banco de séries temporais LABSTAT (cx)",
        "Base de series temporales LABSTAT (cx)",
        "https://download.bls.gov/pub/time.series/cx/",
    ),
    "interview": (
        "Interview Survey public-use microdata (PUMD)",
        "Microdados de uso público da Pesquisa de Entrevista (PUMD)",
        "Microdatos de uso público de la Encuesta de Entrevista (PUMD)",
        "https://www.bls.gov/cex/pumd_data.htm",
    ),
    "diary": (
        "Diary Survey public-use microdata (PUMD)",
        "Microdados de uso público da Pesquisa de Diário (PUMD)",
        "Microdatos de uso público de la Encuesta de Diario (PUMD)",
        "https://www.bls.gov/cex/pumd_data.htm",
    ),
    "stubs": (
        "PUMD hierarchical grouping files",
        "Arquivos de agrupamento hierárquico dos PUMD",
        "Archivos de agrupación jerárquica de los PUMD",
        "https://www.bls.gov/cex/pumd_doc.htm",
    ),
}

_FLAG = re.compile(r"^Data-quality flag for (\S+): (.*)$")
_ITER = re.compile(r"^Imputation Iteration #\s*(\d)\s*-\s*(\S+)$", re.I)
_WT = re.compile(r"^Balanced half-sample replicate weight (\d+) of 44, (.*)$")


def translate(en: str) -> tuple[str, str]:
    if en in TRANSLATIONS:
        pt, es = TRANSLATIONS[en]
        return pt, es
    if m := _FLAG.match(en):
        v = m.group(1)
        return (
            f"Indicador de qualidade de {v}: válido, em branco, alocado, imputado ou com teto (topcoded)",
            f"Indicador de calidad de {v}: válido, en blanco, asignado, imputado o con tope (topcoded)",
        )
    if m := _ITER.match(en):
        k, v = m.groups()
        return (
            f"Iteração de imputação nº {k} de {v}",
            f"Iteración de imputación n.º {k} de {v}",
        )
    if m := _WT.match(en):
        k = m.group(1)
        return (
            f"Peso de replicação de meia amostra balanceada {k} de 44, usado para estimar a variância amostral",
            f"Ponderación de réplica de media muestra balanceada {k} de 44, usada para estimar la varianza muestral",
        )
    raise KeyError(f"no translation for {en!r}")


def read_arch(table: str) -> list[dict]:
    with open(ARCH / f"{table}.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def columns_payload(table: str) -> list[dict]:
    out = []
    for a in read_arch(table):
        pt, es = translate(a["description"])
        col = {
            "name": a["name"],
            "bigquery_type": a["bigquery_type"],
            "description_en": a["description"],
            "description_pt": pt,
            "description_es": es,
            "covered_by_dictionary": a["covered_by_dictionary"] == "yes",
            "has_sensitive_data": a["has_sensitive_data"] == "yes",
        }
        for key in (
            "measurement_unit",
            "directory_column",
            "temporal_coverage",
        ):
            if a[key]:
                col[key] = a[key]
        if a["observations"]:
            col["observations_en"] = a["observations"]
        out.append(col)
    return out


def resolve(env: str) -> dict:
    def look(cat, slug, e=env):
        return server.lookup_id(category=cat, slug=slug, env=e)["id"]

    ids = {
        "organization": look("organization", ORGANIZATION[env]),
        "license": look("license", "cc0"),
        "availability": look("availability", "online"),
        "area": look("area", "us"),
        "published": look("status", "published"),
        "under_review": look("status", "under_review"),
        "themes": [look("theme", s) for s in THEMES],
        "account": server.get_authenticated_account(env=env)["id"],
    }
    for e in ("year", "quarter", "household", "person", "item", "series"):
        ids[e] = look("entity", e)
    prod_tags = [look("tag", s, "prod") for s in TAGS]
    known = set(server.discover_ids(env=env, keys=["tag"])["tag"].values())
    missing = [
        s for s, i in zip(TAGS, prod_tags, strict=True) if i not in known
    ]
    if missing:
        raise SystemExit(f"tags missing on {env}: {missing}")
    ids["tags"] = prod_tags
    return ids


def dataset_fields(ids: dict, status: str) -> dict:
    return {
        "slug": SLUG,
        "organization_ids": [ids["organization"]],
        "theme_ids": ids["themes"],
        "tag_ids": ids["tags"],
        "status_id": ids[status],
        **DATASET_TEXT,
    }


def table_fields(table, dataset_id, ids, **extra):
    text = {
        k: v
        for k, v in TABLE_TEXT[table].items()
        if k not in ("entities", "source", "coverage")
    }
    return {
        "slug": table,
        "dataset_id": dataset_id,
        "status_id": ids["published"],
        "published_by_ids": [ids["account"]],
        "data_cleaned_by_ids": [ids["account"]],
        **text,
        **extra,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True, choices=["dev", "staging", "prod"])
    ap.add_argument(
        "--publish",
        action="store_true",
        help="status published (dev/staging pre-promotion; prod only post-merge)",
    )
    ap.add_argument("--tables", nargs="*", default=TABLE_ORDER)
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    server.auth(env=env)
    ids = resolve(env)
    existing = server.get_dataset(slug=SLUG, env=env)
    prior_tables = existing.get("tables", {}) if existing.get("found") else {}

    status = "published" if args.publish else "under_review"
    ds = server.create_update_dataset(
        id=existing.get("id") if existing.get("found") else None,
        env=env,
        **dataset_fields(ids, status),
    )
    dataset_id = ds["id"]
    print(f"dataset {SLUG} -> {dataset_id} ({status})")

    prior_sources = {
        s["name"]: s["id"]
        for s in server.get_raw_data_sources(dataset_slug=SLUG, env=env)
    }
    source_ids = {}
    for key, (en, pt, es, url) in RAW_SOURCES.items():
        r = server.create_update_raw_data_source(
            id=prior_sources.get(en) or prior_sources.get(pt),
            dataset_id=dataset_id,
            name_en=en,
            name_pt=pt,
            name_es=es,
            url=url,
            license_id=ids["license"],
            availability_id=ids["availability"],
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            env=env,
        )
        source_ids[key] = r["id"]
        print(f"  raw source {key} -> {r['id']}")
        # source Update: the source's max COVERAGE date, never today
        src_updates = server._gql(
            "query($s:ID!){allUpdate(rawDataSource_Id:$s){edges{node{id}}}}",
            {"s": r["id"]},
            env=env,
        )["allUpdate"]["edges"]
        server.create_update_update(
            id=server._strip_id(src_updates[0]["node"]["id"])
            if src_updates
            else None,
            raw_data_source_id=r["id"],
            entity_id=ids["year"],
            frequency=1,
            latest=f"{LAST_RELEASE}-01-01T00:00:00+00:00",
            env=env,
        )

    for table in args.tables:
        spec = TABLE_TEXT[table]
        prior = prior_tables.get(table, {})
        tb = server.create_update_table(
            id=prior.get("id"), env=env, **table_fields(table, dataset_id, ids)
        )
        table_id = tb["id"]
        print(f"table {table} -> {table_id}")

        prior_ols = {
            ol["entity_slug"]: ol["id"]
            for ol in prior.get("observation_levels", [])
        }
        ol_ids = {}
        for entity in spec["entities"]:
            r = server.create_update_observation_level(
                id=prior_ols.get(entity),
                table_id=table_id,
                entity_id=ids[entity],
                env=env,
            )
            ol_ids[entity] = r["id"]

        cols = columns_payload(table)
        res = server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(cols, ensure_ascii=False),
            batch_size=100,
            env=env,
        )
        summary = {
            k: (len(v) if isinstance(v, list) else v) for k, v in res.items()
        }
        print(f"  columns: {summary}")
        if res.get("errors"):
            raise SystemExit(f"{table}: column errors {res['errors'][:5]}")

        cols_now = server._gql(
            "query($t:ID!){allColumn(table_Id:$t){edges{node{id name}}}}",
            {"t": table_id},
            env=env,
        )["allColumn"]["edges"]
        by_name = {
            c["node"]["name"]: server._strip_id(c["node"]["id"])
            for c in cols_now
        }
        # bulk_upsert can append retried columns; restore the architecture order
        server.reorder_columns(
            table_id=table_id,
            column_names=[a["name"] for a in read_arch(table)],
            env=env,
        )
        # link each grain column to its observation level; re-pass is_partition
        # because update_column's booleans default to False
        for entity, colname in spec["entities"].items():
            server.update_column(
                column_id=by_name[colname],
                column_name=colname,
                table_id=table_id,
                is_partition=(colname == "year"),
                observation_level_id=ol_ids[entity],
                env=env,
            )
        if "year" in by_name and "year" not in spec["entities"].values():
            server.update_column(
                column_id=by_name["year"],
                column_name="year",
                table_id=table_id,
                is_partition=True,
                env=env,
            )

        prior_cloud = (prior.get("cloud_tables") or [{}])[0].get("id")
        server.create_update_cloud_table(
            id=prior_cloud,
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            env=env,
        )

        prior_cov = (prior.get("coverages") or [{}])[0]
        cov = server.create_update_coverage(
            id=prior_cov.get("id"),
            table_id=table_id,
            area_id=ids["area"],
            env=env,
        )
        if spec["coverage"]:
            (sy, sm), (ey, em) = spec["coverage"]
            rng = {"start_year": sy, "end_year": ey, "interval": 1}
            if sm:
                rng |= {"start_month": sm, "end_month": em}
            prior_range = (prior_cov.get("datetime_ranges") or [{}])[0].get(
                "id"
            )
            server.create_update_datetime_range(
                id=prior_range, coverage_id=cov["id"], env=env, **rng
            )

        prior_updates = {
            u["entity_slug"]: u["id"] for u in prior.get("updates", [])
        }
        server.create_update_update(
            id=prior_updates.get("year"),
            table_id=table_id,
            entity_id=ids["year"],
            frequency=1,
            lag=1,  # year Y lands in Sep-Dec of Y+1
            latest=TODAY,
            env=env,
        )

        if spec["source"]:
            server.create_update_table(
                id=table_id,
                env=env,
                **table_fields(
                    table,
                    dataset_id,
                    ids,
                    raw_data_source_ids=[source_ids[spec["source"]]],
                ),
            )

    if args.tables == TABLE_ORDER:
        server.reorder_tables(
            dataset_slug=SLUG, table_slugs=TABLE_ORDER, env=env
        )
    print("\ndone. Verify with get_dataset / GraphQL before promoting.")


if __name__ == "__main__":
    main()
