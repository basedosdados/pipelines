"""Register the us_fdic_bankfind metadata in the Data Basis backend.

    ~/.pyenv/versions/3.11.6/bin/python models/us_fdic_bankfind/code/register_metadata.py [staging|prod] [under_review|published]

Everything is resolved by slug at runtime, because reference ids differ between
backends, and the whole script is idempotent: re-running it updates rather than
duplicating, and a second run is a no-op.

It calls the databasis MCP server's functions in-process rather than through the
MCP tool layer.  Same code path, but it makes the 290-column `financials`
payload practical: as a tool argument that is 137 KB of JSON in one call.

Three backend behaviours this has to work around, each of which cost real time:

* **`create_update_*` is not idempotent for a table's child records.**
  Observation levels, cloud tables, coverages and updates get a brand-new row
  whenever `id` is omitted.  Re-runs left `financials` with eight observation
  levels and four coverages.  `prune()` clears duplicates, and existing ids are
  read back and passed.
* **Duplicate coverages then break `create_update_table`** with
  `'TableForm' has no field named 'coverages_areas'` -- an error naming nothing
  relevant.  It only appears on tables whose coverage carries a datetime range.
* **`get_dataset` caps a table's columns at 200**, so on `financials` (290) the
  partition column was absent from the listing and `is_partition` silently went
  unset.  Column ids come from `_fetch_table_columns`, whose ids are relay
  globals (`ColumnNode:<uuid>`) and need the prefix stripped.
"""

from __future__ import annotations

import csv
import datetime
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

sys.path.insert(0, str(Path.home() / "Dropbox/BD/mcp"))
import server  # pyrefly: ignore [missing-import]  (resolved via sys.path above)

DATASET_SLUG = "bankfind"
GCP_DATASET = "us_fdic_bankfind"
# the staging backend is paired with the dev GCP project; prod with prod
GCP_PROJECT = {
    "staging": "basedosdados-dev",
    "dev": "basedosdados-dev",
    "prod": "basedosdados",
}

ARCH = Path(__file__).resolve().parent / "architecture_trilingual"

TABLE_ORDER = [
    "institution",
    "indicator",
    "financials",
    "financials_indicator",
]
NAMES = {
    "institution": ("Instituições", "Institutions", "Instituciones"),
    "indicator": ("Rubricas", "Indicators", "Partidas"),
    "financials": (
        "Dados financeiros trimestrais",
        "Quarterly financials",
        "Datos financieros trimestrales",
    ),
    "financials_indicator": (
        "Dados financeiros trimestrais por rubrica",
        "Quarterly financials by indicator",
        "Datos financieros trimestrales por partida",
    ),
}
DESCRIPTIONS = {
    "institution": (
        "Cadastro de todas as instituições financeiras registradas pelo FDIC, "
        "ativas ou encerradas, com identificadores, classificação de charter e "
        "supervisão, controle acionário, localização, datas relevantes e um "
        "retrato dos dados financeiros mais recentes. Uma linha por número de "
        "certificado do FDIC.",
        "Directory of every financial institution registered by the FDIC, active "
        "or closed, with identifiers, charter and supervisory classification, "
        "ownership, location, key dates and a snapshot of its latest reported "
        "financials. One row per FDIC certificate number.",
        "Directorio de todas las instituciones financieras registradas por la "
        "FDIC, activas o cerradas, con identificadores, clasificación de charter "
        "y supervisión, control accionario, ubicación, fechas relevantes y un "
        "retrato de sus datos financieros más recientes. Una fila por número de "
        "certificado de la FDIC.",
    ),
    "indicator": (
        "Dicionário das rubricas trimestrais reportadas ao FDIC, com nome "
        "legível, a definição publicada pelo FDIC, a unidade em que os valores "
        "são expressos e a coluna correspondente na tabela financials, quando "
        "existe. Decodifica a tabela financials_indicator.",
        "Dictionary of the quarterly line items reported to the FDIC, giving each "
        "one a readable name, the FDIC's published definition, the unit its "
        "values are expressed in, and the matching column in the financials "
        "table when it has one. Decodes the financials_indicator table.",
        "Diccionario de las partidas trimestrales reportadas a la FDIC, con "
        "nombre legible, la definición publicada por la FDIC, la unidad en que se "
        "expresan los valores y la columna correspondiente en la tabla "
        "financials, cuando existe. Decodifica la tabla financials_indicator.",
    ),
    "financials": (
        "Dados financeiros trimestrais dos Call Reports de cada instituição "
        "segurada pelo FDIC, cobrindo balanço patrimonial, demonstração de "
        "resultados, qualidade dos ativos, capital e os indicadores de desempenho "
        "calculados pelo FDIC. Uma linha por instituição e trimestre, com as "
        "rubricas principais em colunas; o conjunto completo está em "
        "financials_indicator. Valores monetários em dólares, convertidos dos "
        "milhares em que o FDIC publica.",
        "Quarterly Call Report financials for every FDIC-insured institution, "
        "covering the balance sheet, income statement, asset quality, capital and "
        "the performance ratios the FDIC computes. One row per institution and "
        "quarter, holding the headline line items as columns; the complete set is "
        "in financials_indicator. Monetary values are in dollars, converted from "
        "the thousands the FDIC publishes.",
        "Datos financieros trimestrales de los Call Reports de cada institución "
        "asegurada por la FDIC, que cubren balance, estado de resultados, calidad "
        "de los activos, capital e indicadores de desempeño calculados por la "
        "FDIC. Una fila por institución y trimestre, con las partidas principales "
        "en columnas; el conjunto completo está en financials_indicator. Valores "
        "monetarios en dólares, convertidos de los miles en que la FDIC publica.",
    ),
    "financials_indicator": (
        "Todas as rubricas trimestrais reportadas ao FDIC em formato longo: uma "
        "linha por instituição, trimestre e rubrica. Cobre o conjunto completo de "
        "rubricas, e não apenas a seleção principal presente em financials, ao "
        "custo de exigir uma junção com a tabela indicator para ser lida. Só "
        "existem linhas onde a instituição reportou a rubrica.",
        "Every quarterly line item reported to the FDIC, in long form: one row "
        "per institution, quarter and line item. Covers the complete set of line "
        "items rather than the headline selection in financials, at the cost of "
        "needing a join to the indicator table to read. Rows exist only where the "
        "institution reported the item.",
        "Todas las partidas trimestrales reportadas a la FDIC en formato largo: "
        "una fila por institución, trimestre y partida. Cubre el conjunto completo "
        "de partidas, y no solo la selección principal de financials, a costa de "
        "requerir una unión con la tabla indicator para su lectura. Solo existen "
        "filas donde la institución reportó la partida.",
    ),
}

ORGANIZATION = {
    "slug": "fdic",
    "name": "Federal Deposit Insurance Corporation (FDIC)",
    "description_pt": (
        "Agência federal independente dos Estados Unidos que garante os depósitos "
        "bancários, supervisiona instituições financeiras quanto à solidez e à "
        "proteção do consumidor, e administra a resolução de bancos em falência. "
        "Publica o cadastro de instituições e os dados financeiros trimestrais do "
        "sistema bancário norte-americano."
    ),
    "description_en": (
        "Independent United States federal agency that insures bank deposits, "
        "supervises financial institutions for safety, soundness and consumer "
        "protection, and manages the resolution of failed banks. Publishes the "
        "institution directory and the quarterly financial data of the US banking "
        "system."
    ),
    "description_es": (
        "Agencia federal independiente de los Estados Unidos que asegura los "
        "depósitos bancarios, supervisa a las instituciones financieras en materia "
        "de solidez y protección al consumidor, y administra la resolución de "
        "bancos en quiebra. Publica el directorio de instituciones y los datos "
        "financieros trimestrales del sistema bancario estadounidense."
    ),
    "website": "https://www.fdic.gov/",
}

DATASET_NAME = (
    "Instituições e Dados Financeiros Bancários (BankFind)",
    "Bank Institutions and Financial Data (BankFind)",
    "Instituciones y Datos Financieros Bancarios (BankFind)",
)
DATASET_DESCRIPTION = (
    "Cadastro de todas as instituições financeiras registradas pelo FDIC, ativas "
    "ou encerradas, e os dados financeiros trimestrais dos Call Reports de cada "
    "instituição segurada, de 1984 a 2026. Cobre ativos, depósitos, carteira de "
    "crédito, demonstração de resultados, qualidade dos ativos, capital e os "
    "indicadores de desempenho calculados pelo FDIC. Os valores monetários estão "
    "em dólares, convertidos dos milhares em que o FDIC publica.",
    "Directory of every financial institution registered by the FDIC, active or "
    "closed, and the quarterly Call Report financials of each insured institution "
    "from 1984 to 2026. Covers assets, deposits, the loan portfolio, the income "
    "statement, asset quality, capital and the performance ratios the FDIC "
    "computes. Monetary values are in dollars, converted from the thousands the "
    "FDIC publishes.",
    "Directorio de todas las instituciones financieras registradas por la FDIC, "
    "activas o cerradas, y los datos financieros trimestrales de los Call Reports "
    "de cada institución asegurada, de 1984 a 2026. Cubre activos, depósitos, la "
    "cartera de crédito, el estado de resultados, la calidad de los activos, el "
    "capital y los indicadores de desempeño calculados por la FDIC. Los valores "
    "monetarios están en dólares, convertidos de los miles en que la FDIC publica.",
)

# existing tags, then the two this dataset had to create
TAGS = ["banking", "financial-institution", "credit", "loan", "balance-sheet"]
NEW_TAGS = {
    "deposit": ("depósito", "deposit", "depósito"),
    "bank-supervision": (
        "supervisão bancária",
        "bank supervision",
        "supervisión bancaria",
    ),
}

SOURCES = {
    "institutions": {
        "name": (
            "FDIC BankFind Suite — endpoint de instituições",
            "FDIC BankFind Suite — institutions endpoint",
            "FDIC BankFind Suite — endpoint de instituciones",
        ),
        "url": "https://api.fdic.gov/banks/institutions",
        "description": (
            "Endpoint REST que devolve o cadastro de instituições financeiras "
            "registradas pelo FDIC, ativas ou encerradas, com 152 campos de "
            "identificação, classificação de charter, supervisão, localização e "
            "datas. O dicionário de campos é publicado em "
            "institution_properties.yaml.",
            "REST endpoint returning the directory of financial institutions "
            "registered by the FDIC, active or closed, with 152 fields covering "
            "identification, charter classification, supervision, location and key "
            "dates. The field dictionary is published at "
            "institution_properties.yaml.",
            "Endpoint REST que devuelve el directorio de instituciones financieras "
            "registradas por la FDIC, activas o cerradas, con 152 campos de "
            "identificación, clasificación de charter, supervisión, ubicación y "
            "fechas. El diccionario de campos se publica en "
            "institution_properties.yaml.",
        ),
    },
    "financials": {
        "name": (
            "FDIC BankFind Suite — endpoint de dados financeiros",
            "FDIC BankFind Suite — financials endpoint",
            "FDIC BankFind Suite — endpoint de datos financieros",
        ),
        "url": "https://api.fdic.gov/banks/financials",
        "description": (
            "Endpoint REST que devolve os dados financeiros trimestrais dos Call "
            "Reports por instituição e data de referência, com 2.378 campos que "
            "cobrem balanço, demonstração de resultados, qualidade dos ativos, "
            "capital e indicadores calculados pelo FDIC. Valores monetários são "
            "publicados em milhares de dólares. O dicionário de campos é publicado "
            "em risview_properties.yaml.",
            "REST endpoint returning quarterly Call Report financials by "
            "institution and report date, with 2,378 fields covering the balance "
            "sheet, income statement, asset quality, capital and the ratios the "
            "FDIC computes. Monetary values are published in thousands of dollars. "
            "The field dictionary is published at risview_properties.yaml.",
            "Endpoint REST que devuelve los datos financieros trimestrales de los "
            "Call Reports por institución y fecha de referencia, con 2.378 campos "
            "que cubren balance, estado de resultados, calidad de los activos, "
            "capital e indicadores calculados por la FDIC. Los valores monetarios "
            "se publican en miles de dólares. El diccionario de campos se publica "
            "en risview_properties.yaml.",
        ),
    },
}
# exactly one raw source per table: client._raw_source_id raises on two or more,
# which would break the recurring pipeline's poll on its first run
TABLE_SOURCE = {
    "institution": "institutions",
    "indicator": "financials",
    "financials": "financials",
    "financials_indicator": "financials",
}
# the columns that identify each grain; without these links the site renders the
# observation level's columns as "Não informado"
GRAIN = {
    "institution": {"company": ["cert"]},
    "indicator": {"series": ["indicator_id"]},
    "financials": {"company": ["cert"], "quarter": ["year", "quarter"]},
    "financials_indicator": {
        "company": ["cert"],
        "quarter": ["year", "quarter"],
        "series": ["indicator_id"],
    },
}
PARTITION = {"financials": "year", "financials_indicator": "year"}
TEMPORAL = {"financials", "financials_indicator"}
COVERAGE_START = (1984, 3)  # 1984Q1
COVERAGE_END = (2026, 6)  # 2026Q2


def fn(name: str) -> Callable[..., Any]:
    """The plain function behind an MCP tool.

    FastMCP's decorator keeps the original callable on `.fn`; annotating the
    return type keeps every call site type-checkable, since `getattr` alone is
    `Any | None` to the checker.
    """
    f = getattr(server, name)
    return cast("Callable[..., Any]", getattr(f, "fn", f))


def lookup(category: str, slug: str, env: str) -> str | None:
    try:
        return fn("lookup_id")(category=category, slug=slug, env=env)["id"]
    except Exception:
        return None


def columns_payload(table: str) -> str:
    rows = []
    with (ARCH / f"{table}.csv").open() as handle:
        for r in csv.DictReader(handle):
            entry = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": r["description_pt"],
                "description_en": r["description_en"],
                "description_es": r["description_es"],
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
            }
            for field in (
                "directory_column",
                "measurement_unit",
                "observations",
            ):
                if r[field]:
                    entry[field] = r[field]
            rows.append(entry)
    return json.dumps(rows, ensure_ascii=False)


def table_columns(table_id: str, env: str) -> dict[str, str]:
    """Column name -> bare uuid, from the uncapped query."""
    return {
        c["name"]: c["id"].split(":")[-1]
        for c in server._fetch_table_columns(table_id, env)
    }


def delete(kind: str, record_id: str, env: str) -> None:
    query = f"mutation($id: UUID!) {{ Delete{kind}(id: $id) {{ errors }} }}"
    payload = server._gql(query, {"id": record_id}, env=env)[f"Delete{kind}"]
    if payload and payload.get("errors"):
        raise RuntimeError(f"Delete{kind} {record_id}: {payload['errors']}")


def prune(node: dict, env: str) -> None:
    """Delete every duplicate child record beyond the first of each kind."""
    seen: set[str] = set()
    for level in node["observation_levels"]:
        if level["entity_id"] in seen:
            delete("ObservationLevel", level["id"], env)
        else:
            seen.add(level["entity_id"])
    for extra in node["cloud_tables"][1:]:
        delete("CloudTable", extra["id"], env)
    for extra in node["coverages"][1:]:
        delete("Coverage", extra["id"], env)
    seen = set()
    for upd in node["updates"]:
        if upd["entity_id"] in seen:
            delete("Update", upd["id"], env)
        else:
            seen.add(upd["entity_id"])


def existing(node: dict) -> dict:
    coverages = node["coverages"]
    return {
        "levels": {
            o["entity_id"]: o["id"] for o in node["observation_levels"]
        },
        "cloud": node["cloud_tables"][0]["id"]
        if node["cloud_tables"]
        else None,
        "coverage": coverages[0]["id"] if coverages else None,
        "range": (
            coverages[0]["datetime_ranges"][0]["id"]
            if coverages and coverages[0]["datetime_ranges"]
            else None
        ),
        "updates": {u["entity_id"]: u["id"] for u in node["updates"]},
    }


def main(env: str, status: str) -> None:
    get_dataset = fn("get_dataset")
    print(f"registering us_fdic_bankfind on {env}\n")

    # --- reference ids, resolved per backend --------------------------------
    org = (
        lookup("organization", ORGANIZATION["slug"], env)
        or fn("create_update_organization")(
            slug=ORGANIZATION["slug"],
            name_pt=ORGANIZATION["name"],
            name_en=ORGANIZATION["name"],
            name_es=ORGANIZATION["name"],
            description_pt=ORGANIZATION["description_pt"],
            description_en=ORGANIZATION["description_en"],
            description_es=ORGANIZATION["description_es"],
            website=ORGANIZATION["website"],
            area_id=lookup("area", "us", env),
            env=env,
        )["id"]
    )

    tag_ids = []
    for slug in TAGS:
        found = lookup("tag", slug, env)
        if found:
            tag_ids.append(found)
    for slug, (pt, en, es) in NEW_TAGS.items():
        found = lookup("tag", slug, env)
        if not found:
            found = fn("create_update_tag")(
                slug=slug, name_pt=pt, name_en=en, name_es=es, env=env
            )["id"]
        tag_ids.append(found)

    entities = {
        e: lookup("entity", e, env) for e in ("company", "quarter", "series")
    }
    area_us = lookup("area", "us", env)
    under_review = lookup("status", "under_review", env)
    published = lookup("status", "published", env)
    account = fn("get_authenticated_account")(env=env)["id"]

    # --- dataset ------------------------------------------------------------
    # The status is passed in, not inferred. A dataset is created
    # `under_review`, which hides it from the production frontend until the
    # onboarding PR has merged and the prod tables actually exist; publishing is
    # a separate deliberate step (onboarding-workflow step 13).
    current = get_dataset(slug=DATASET_SLUG, env=env)
    dataset_status = published if status == "published" else under_review
    dataset_id = fn("create_update_dataset")(
        slug=DATASET_SLUG,
        name_pt=DATASET_NAME[0],
        name_en=DATASET_NAME[1],
        name_es=DATASET_NAME[2],
        description_pt=DATASET_DESCRIPTION[0],
        description_en=DATASET_DESCRIPTION[1],
        description_es=DATASET_DESCRIPTION[2],
        organization_ids=[org],
        theme_ids=[lookup("theme", "economics", env)],
        tag_ids=tag_ids,
        status_id=dataset_status,
        id=current["id"] if current["found"] else None,
        env=env,
    )["id"]
    print(
        f"dataset {DATASET_SLUG} ({dataset_id}) status={status}, {len(tag_ids)} tags"
    )

    # --- raw data sources ---------------------------------------------------
    have_sources = {
        s["url"]: s["id"]
        for s in fn("get_raw_data_sources")(dataset_slug=DATASET_SLUG, env=env)
    }
    source_ids = {}
    for key, spec in SOURCES.items():
        source_ids[key] = fn("create_update_raw_data_source")(
            dataset_id=dataset_id,
            name_pt=spec["name"][0],
            name_en=spec["name"][1],
            name_es=spec["name"][2],
            description_pt=spec["description"][0],
            description_en=spec["description"][1],
            description_es=spec["description"][2],
            url=spec["url"],
            license_id=lookup("license", "cc0", env),
            availability_id=lookup("availability", "online", env),
            language_ids=[lookup("language", "en", env)],
            has_structured_data=True,
            contains_api=True,
            is_free=True,
            requires_registration=False,
            id=have_sources.get(spec["url"]),
            env=env,
        )["id"]
    print(f"raw data sources: {len(source_ids)}")

    # --- tables -------------------------------------------------------------
    current = get_dataset(slug=DATASET_SLUG, env=env)
    table_ids = {}
    for table in TABLE_ORDER:
        pt, en, es = NAMES[table]
        dpt, den, des = DESCRIPTIONS[table]
        node = current["tables"].get(table)
        table_ids[table] = fn("create_update_table")(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            dataset_id=dataset_id,
            status_id=published,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            id=node["id"] if node else None,
            env=env,
        )["id"]

    # drop anything an earlier run duplicated, then reuse the surviving ids
    current = get_dataset(slug=DATASET_SLUG, env=env)
    for node in current["tables"].values():
        prune(node, env)
    current = get_dataset(slug=DATASET_SLUG, env=env)

    today = f"{datetime.date.today().isoformat()}T00:00:00"
    for table in TABLE_ORDER:
        table_id = table_ids[table]
        result = fn("bulk_upsert_columns")(
            table_id=table_id,
            columns_json=columns_payload(table),
            env=env,
            batch_size=50,
        )
        cols = table_columns(table_id, env)
        have = existing(current["tables"][table])

        for entity, names in GRAIN[table].items():
            level = fn("create_update_observation_level")(
                table_id=table_id,
                entity_id=entities[entity],
                id=have["levels"].get(entities[entity]),
                env=env,
            )["id"]
            for name in names:
                if name not in cols:
                    continue
                # update_column's booleans default False, so is_partition has to
                # be re-passed or the flag is clobbered
                fn("update_column")(
                    column_id=cols[name],
                    column_name=name,
                    table_id=table_id,
                    observation_level_id=level,
                    is_partition=(PARTITION.get(table) == name),
                    env=env,
                )

        fn("create_update_cloud_table")(
            table_id=table_id,
            gcp_project_id=GCP_PROJECT[env],
            gcp_dataset_id=GCP_DATASET,
            gcp_table_id=table,
            id=have["cloud"],
            env=env,
        )
        coverage_id = fn("create_update_coverage")(
            table_id=table_id,
            area_id=area_us,
            is_closed=False,
            id=have["coverage"],
            env=env,
        )["id"]
        if table in TEMPORAL:
            # quarterly data needs month granularity: year-only would report
            # 1984..2026 for data that really spans 1984-03..2026-06
            fn("create_update_datetime_range")(
                coverage_id=coverage_id,
                start_year=COVERAGE_START[0],
                start_month=COVERAGE_START[1],
                end_year=COVERAGE_END[0],
                end_month=COVERAGE_END[1],
                interval=1,
                id=have["range"],
                env=env,
            )
        # table Update: when WE last refreshed it, a wall clock
        fn("create_update_update")(
            entity_id=entities["quarter"],
            frequency=1,
            lag=1,
            latest=today,
            table_id=table_id,
            id=have["updates"].get(entities["quarter"]),
            env=env,
        )
        print(f"  {table:<22} columns={result['source_rows']:>4}")

    # Raw sources are linked in a deferred second pass: the backend rejects
    # CreateUpdateTable once that table's coverage carries a datetime range.
    for table in TABLE_ORDER:
        pt, en, es = NAMES[table]
        dpt, den, des = DESCRIPTIONS[table]
        # the API does no partial updates, so every required field is re-passed
        fn("create_update_table")(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            dataset_id=dataset_id,
            status_id=published,
            published_by_ids=[account],
            data_cleaned_by_ids=[account],
            raw_data_source_ids=[source_ids[TABLE_SOURCE[table]]],
            id=table_ids[table],
            env=env,
        )

    # raw source Update: what the SOURCE published, i.e. its max coverage date
    fn("create_update_update")(
        entity_id=entities["quarter"],
        frequency=1,
        latest=f"{COVERAGE_END[0]}-06-30T00:00:00",
        raw_data_source_id=source_ids["financials"],
        env=env,
    )

    current = get_dataset(slug=DATASET_SLUG, env=env)
    print()
    for table, node in sorted(current["tables"].items()):
        ranges = sum(
            len(c.get("datetime_ranges", [])) for c in node["coverages"]
        )
        levels = ",".join(
            sorted(o["entity_slug"] for o in node["observation_levels"])
        )
        print(
            f"{table:<22} cols={len(server._fetch_table_columns(node['id'], env)):<4} "
            f"OLs=[{levels}] cloud={len(node['cloud_tables'])} "
            f"coverage={len(node['coverages'])} ranges={ranges} "
            f"updates={len(node['updates'])}"
        )


if __name__ == "__main__":
    main(
        sys.argv[1] if len(sys.argv) > 1 else "staging",
        sys.argv[2] if len(sys.argv) > 2 else "under_review",
    )
