"""Register the fr_inpi_ratios_financiers metadata in the Data Basis backend.

    ~/.venvs/databasis-mcp/bin/python \
        models/fr_inpi_ratios_financiers/code/register_metadata.py [dev|staging|prod] [under_review|published]

Reference ids are resolved by slug at runtime (they differ between backends), and
existing child records are read back and their ids passed, so a re-run updates
instead of duplicating. Calls the databasis-mcp functions in-process, which makes
the column payload practical as a single call.

Columns come from ``architecture/*.csv`` (the source of truth) through
``bulk_upsert_columns``; ``is_partition`` and each column's observation level are
then set with ``update_column``, re-passing ``is_partition`` because its default
would otherwise clobber the flag.
"""

from __future__ import annotations

import csv
import datetime
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

DATASET_SLUG = "ratios_financiers"
GCP_DATASET = "fr_inpi_ratios_financiers"
# staging stands in for dev while the dev backend is down; its data lives in dev
GCP_PROJECT = {
    "dev": "basedosdados-dev",
    "staging": "basedosdados-dev",
    "prod": "basedosdados",
}
ARCH = Path(__file__).resolve().parent / "architecture"

DATASET_NAME = (
    "Índices Financeiros de Empresas (BCE / INPI)",
    "Company Financial Ratios (BCE / INPI)",
    "Índices Financieros de Empresas (BCE / INPI)",
)
DATASET_DESCRIPTION = (
    "Indicadores e índices financeiros de empresas francesas (faturamento, margem bruta, "
    "EBE, EBIT, resultado líquido, endividamento, liquidez, autonomia financeira, "
    "capacidade de autofinanciamento e prazos de estoques, clientes e fornecedores), "
    "por número SIREN e data de encerramento do exercício, calculados a partir das contas "
    "anuais depositadas no Registro Nacional do Comércio e das Sociedades (RNCS) e "
    "difundidas pelo Instituto Nacional da Propriedade Industrial (INPI). Os dados são "
    "tratados pela DNUM do Ministério do Trabalho na Base Commune Entreprise (BCE) e "
    "publicados pela Direção-Geral das Empresas (DGE) dos Ministérios Econômicos e "
    "Financeiros. Cobre cerca de 1,6 milhão de empresas, com exercícios encerrados "
    "sobretudo entre 2016 e 2025.",
    "Financial indicators and ratios of French companies (turnover, gross margin, EBE, "
    "EBIT, net income, debt, liquidity, financial autonomy, self-financing capacity and "
    "stock, customer and supplier periods), by SIREN number and fiscal-year closing date, "
    "computed from the annual accounts filed with the National Trade and Companies "
    "Register (RNCS) and disseminated by the National Institute of Industrial Property "
    "(INPI). The data are processed by the DNUM of the Ministry of Labour on the Base "
    "Commune Entreprise (BCE) and published by the Directorate-General for Enterprise "
    "(DGE) of the Ministries for the Economy and Finance. It covers about 1.6 million "
    "companies, with fiscal years closing mostly between 2016 and 2025.",
    "Indicadores e índices financieros de empresas francesas (cifra de negocios, margen "
    "bruto, EBE, EBIT, resultado neto, endeudamiento, liquidez, autonomía financiera, "
    "capacidad de autofinanciación y plazos de existencias, clientes y proveedores), por "
    "número SIREN y fecha de cierre del ejercicio, calculados a partir de las cuentas "
    "anuales depositadas en el Registro Nacional de Comercio y Sociedades (RNCS) y "
    "difundidas por el Instituto Nacional de la Propiedad Industrial (INPI). Los datos son "
    "tratados por la DNUM del Ministerio de Trabajo en la Base Commune Entreprise (BCE) y "
    "publicados por la Dirección General de Empresas (DGE) de los Ministerios Económicos y "
    "Financieros. Cubre cerca de 1,6 millones de empresas, con ejercicios cerrados sobre "
    "todo entre 2016 y 2025.",
)

ORGANIZATION = {
    "slug": "inpi",
    "name": (
        "Instituto Nacional da Propriedade Industrial (INPI)",
        "National Institute of Industrial Property (INPI)",
        "Instituto Nacional de la Propiedad Industrial (INPI)",
    ),
    "description": (
        "Instituto público francês responsável pelos títulos de propriedade industrial e "
        "pela difusão dos dados do Registro Nacional do Comércio e das Sociedades (RNCS).",
        "French public institute responsible for industrial property titles and for "
        "disseminating the data of the National Trade and Companies Register (RNCS).",
        "Instituto público francés responsable de los títulos de propiedad industrial y de "
        "la difusión de los datos del Registro Nacional de Comercio y Sociedades (RNCS).",
    ),
    "website": "https://www.inpi.fr",
}

# created on first run; no backend had a record for it (checked staging and prod)
LICENSE = {
    "slug": "licence_ouverte_2_0",
    "name": (
        "Licença Aberta 2.0 (Licence Ouverte / Etalab)",
        "Open Licence 2.0 (Licence Ouverte / Etalab)",
        "Licencia Abierta 2.0 (Licence Ouverte / Etalab)",
    ),
    "url": "https://www.etalab.gouv.fr/licence-ouverte-open-licence/",
}

TAGS = [
    "company",
    "financial-statement",
    "balance-sheet",
    "accounting",
    "profitability",
    "debt",
    "liquidity",
    "revenue",
]

SOURCE = {
    "name": (
        "Ratios financeiros (BCE / INPI) - data.economie.gouv.fr",
        "Financial ratios (BCE / INPI) - data.economie.gouv.fr",
        "Ratios financieros (BCE / INPI) - data.economie.gouv.fr",
    ),
    "description": (
        "Portal de dados abertos dos Ministérios Econômicos e Financeiros (Opendatasoft), "
        "conjunto ratios_inpi_bce publicado pela DGE, com exportação em parquet/CSV e API. "
        "Também referenciado em data.gouv.fr (ratios-financiers-bce-inpi).",
        "Open-data portal of the Ministries for the Economy and Finance (Opendatasoft), "
        "dataset ratios_inpi_bce published by the DGE, with parquet/CSV export and API. "
        "Also listed on data.gouv.fr (ratios-financiers-bce-inpi).",
        "Portal de datos abiertos de los Ministerios Económicos y Financieros "
        "(Opendatasoft), conjunto ratios_inpi_bce publicado por la DGE, con exportación en "
        "parquet/CSV y API. También referenciado en data.gouv.fr (ratios-financiers-bce-inpi).",
    ),
    "url": "https://data.economie.gouv.fr/explore/dataset/ratios_inpi_bce/",
    "license": "licence_ouverte_2_0",
}

TABLE_ORDER = ["ratios_financiers", "dicionario"]
NAMES = {
    "ratios_financiers": (
        "Índices financeiros",
        "Financial ratios",
        "Índices financieros",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}
DESCRIPTIONS = {
    "ratios_financiers": (
        "Montantes (em euros) e índices financeiros por empresa (SIREN), data de "
        "encerramento do exercício e tipo de balanço (completo, consolidado ou "
        "simplificado), calculados a partir das contas anuais do RNCS. As fórmulas de cada "
        "índice, que dependem do tipo de balanço, estão nas observações das colunas.",
        "Amounts (in euros) and financial ratios by company (SIREN), fiscal-year closing "
        "date and balance-sheet type (full, consolidated or simplified), computed from RNCS "
        "annual accounts. Each ratio's formula, which depends on the balance-sheet type, is "
        "given in the column observations.",
        "Montos (en euros) e índices financieros por empresa (SIREN), fecha de cierre del "
        "ejercicio y tipo de balance (completo, consolidado o simplificado), calculados a "
        "partir de las cuentas anuales del RNCS. Las fórmulas de cada índice, que dependen "
        "del tipo de balance, están en las observaciones de las columnas.",
    ),
    "dicionario": (
        "Dicionário dos valores codificados da tabela ratios_financiers (type_bilan).",
        "Dictionary of the coded values of the ratios_financiers table (type_bilan).",
        "Diccionario de los valores codificados de la tabla ratios_financiers (type_bilan).",
    ),
}
# observation level entity -> columns that identify it
GRAIN = {
    "ratios_financiers": {
        "company": ["siren"],
        "year": ["annee", "date_cloture_exercice"],
    },
    "dicionario": {},
}
PARTITION = {"ratios_financiers": "annee"}
# annual coverage; the 1919 and 2029 single-row typos are excluded (see CLAUDE.md)
COVERAGE = {"ratios_financiers": (2002, 2026), "dicionario": None}
ENTITIES = ("company", "year")


def fn(name: str) -> Callable[..., Any]:
    f = getattr(bd_mcp_metadata, name, None) or getattr(bd_mcp_write, name)
    return cast("Callable[..., Any]", getattr(f, "fn", f))


def lookup(category: str, slug: str, env: str) -> str | None:
    try:
        return fn("lookup_id")(category=category, slug=slug, env=env)["id"]
    except Exception:
        return None


def columns_payload(table: str) -> tuple[str, list[str]]:
    rows = []
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        for r in csv.DictReader(handle):
            entry: dict[str, Any] = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": r["description_pt"],
                "description_en": r["description_en"],
                "description_es": r["description_es"],
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
                "is_partition": PARTITION.get(table) == r["name"],
            }
            for field in (
                "directory_column",
                "measurement_unit",
                "observations_pt",
                "observations_en",
                "observations_es",
                "original_name",
            ):
                if r[field]:
                    entry[field] = r[field]
            rows.append(entry)
    return json.dumps(rows, ensure_ascii=False), [r["name"] for r in rows]


def table_columns(table_id: str, env: str) -> dict[str, str]:
    return {
        c["name"]: c["id"].split(":")[-1]
        for c in bd_mcp_write._fetch_table_columns(table_id, env)
    }


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
    print(
        f"registering {GCP_DATASET} ({DATASET_SLUG}) on {env}, status={status}\n"
    )

    org = lookup("organization", ORGANIZATION["slug"], env)
    if not org:
        pt, en, es = ORGANIZATION["name"]
        dpt, den, des = ORGANIZATION["description"]
        org = fn("create_update_organization")(
            slug=ORGANIZATION["slug"],
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            website=ORGANIZATION["website"],
            area_id=lookup("area", "fr", env),
            env=env,
        )["id"]
        print(f"CREATED organization {ORGANIZATION['slug']} ({org})")

    license_id = lookup("license", LICENSE["slug"], env)
    if not license_id:
        pt, en, es = LICENSE["name"]
        license_id = fn("create_update_license")(
            slug=LICENSE["slug"],
            name_pt=pt,
            name_en=en,
            name_es=es,
            url=LICENSE["url"],
            env=env,
        )["id"]
        print(f"CREATED license {LICENSE['slug']} ({license_id})")

    tag_ids, missing_tags = [], []
    for slug in TAGS:
        found = lookup("tag", slug, env)
        (tag_ids.append(found) if found else missing_tags.append(slug))
    if missing_tags:
        print(f"WARNING tags not found on {env}: {missing_tags}")

    entities = {e: lookup("entity", e, env) for e in ENTITIES}
    if not all(entities.values()):
        raise SystemExit(f"entities not found on {env}: {entities}")
    area = lookup("area", "fr", env)
    under_review = lookup("status", "under_review", env)
    published = lookup("status", "published", env)
    account = fn("get_authenticated_account")(env=env)["id"]

    current = get_dataset(slug=DATASET_SLUG, env=env)
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
        status_id=published if status == "published" else under_review,
        id=current["id"] if current["found"] else None,
        env=env,
    )["id"]
    print(f"dataset {DATASET_SLUG} ({dataset_id}), {len(tag_ids)} tags")

    have_sources = {
        s["url"]: s["id"]
        for s in fn("get_raw_data_sources")(dataset_slug=DATASET_SLUG, env=env)
    }
    source_id = fn("create_update_raw_data_source")(
        dataset_id=dataset_id,
        name_pt=SOURCE["name"][0],
        name_en=SOURCE["name"][1],
        name_es=SOURCE["name"][2],
        description_pt=SOURCE["description"][0],
        description_en=SOURCE["description"][1],
        description_es=SOURCE["description"][2],
        url=SOURCE["url"],
        license_id=license_id,
        availability_id=lookup("availability", "online", env),
        language_ids=[lookup("language", "fr", env)],
        has_structured_data=True,
        contains_api=True,
        is_free=True,
        requires_registration=False,
        id=have_sources.get(SOURCE["url"]),
        env=env,
    )["id"]
    print(f"raw data source {source_id}")

    def upsert_table(
        table: str, raw: list[str] | None, table_id: str | None
    ) -> str:
        pt, en, es = NAMES[table]
        dpt, den, des = DESCRIPTIONS[table]
        return fn("create_update_table")(
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
            raw_data_source_ids=raw,
            id=table_id,
            env=env,
        )["id"]

    table_ids = {}
    for table in TABLE_ORDER:
        node = current["tables"].get(table) if current["found"] else None
        table_ids[table] = upsert_table(
            table, None, node["id"] if node else None
        )

    current = get_dataset(slug=DATASET_SLUG, env=env)
    today = f"{datetime.date.today().isoformat()}T00:00:00"
    for table in TABLE_ORDER:
        table_id = table_ids[table]
        payload, order = columns_payload(table)
        result = fn("bulk_upsert_columns")(
            table_id=table_id, columns_json=payload, env=env, batch_size=50
        )
        if result.get("errors"):
            raise SystemExit(f"{table}: bulk_upsert errors {result['errors']}")
        fn("reorder_columns")(table_id=table_id, column_names=order, env=env)
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
            area_id=area,
            is_closed=False,
            id=have["coverage"],
            env=env,
        )["id"]
        span = COVERAGE[table]
        if span:
            fn("create_update_datetime_range")(
                coverage_id=coverage_id,
                start_year=span[0],
                end_year=span[1],
                interval=1,
                id=have["range"],
                env=env,
            )
        # table Update: when Data Basis last refreshed it (wall clock)
        fn("create_update_update")(
            entity_id=entities["year"],
            frequency=1,
            lag=1,
            latest=today,
            table_id=table_id,
            id=have["updates"].get(entities["year"]),
            env=env,
        )
        print(f"  {table:<20} columns={result['source_rows']}")

    # deferred: link the raw source once every child record exists
    for table in TABLE_ORDER:
        upsert_table(table, [source_id], table_ids[table])

    current = get_dataset(slug=DATASET_SLUG, env=env)
    print()
    for table, node in sorted(current["tables"].items()):
        ranges = sum(
            len(c.get("datetime_ranges", [])) for c in node["coverages"]
        )
        levels = ",".join(
            sorted(o["entity_slug"] for o in node["observation_levels"])
        )
        ncols = len(bd_mcp_write._fetch_table_columns(node["id"], env))
        print(
            f"{table:<20} cols={ncols:<3} OLs=[{levels}] "
            f"cloud={len(node['cloud_tables'])} coverage={len(node['coverages'])} "
            f"ranges={ranges} updates={len(node['updates'])}"
        )


if __name__ == "__main__":
    env_arg = sys.argv[1] if len(sys.argv) > 1 else "dev"
    status_arg = sys.argv[2] if len(sys.argv) > 2 else "under_review"
    if env_arg not in GCP_PROJECT or status_arg not in (
        "under_review",
        "published",
    ):
        raise SystemExit(
            "usage: register_metadata.py [dev|staging|prod] [under_review|published]"
        )
    main(env_arg, status_arg)
