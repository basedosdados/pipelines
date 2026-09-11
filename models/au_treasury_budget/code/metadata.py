"""Register au_treasury_budget metadata in the Data Basis backend.

Idempotent by construction: every record is looked up before it is written, and
an existing id is passed back so the call updates rather than duplicates.
``create_update_observation_level``, ``create_update_cloud_table``,
``create_update_coverage`` and ``create_update_update`` all create a *second*
record when called without an id, so re-running without that lookup silently
doubles them.

Run:  python metadata.py --env staging
      python metadata.py --env staging --publish     # flip to published
"""

from __future__ import annotations

import argparse
import contextlib
import json
import pathlib
import sys

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
import columns as column_defs
import server

DATASET_SLUG = "budget"
GCP_DATASET_ID = "au_treasury_budget"

#: Organization. The staging backend already has a `treasury` org -- it is the
#: **United States** Department of the Treasury, area `us` -- so the Australian
#: one takes the `au_` prefix that every other Australian organization uses
#: there (au_abs, au_ato, au_doe, au_rba). Production uses short slugs instead,
#: so the prod promotion must decide between `treasury` and `au_treasury`.
ORGANIZATION_SLUG = "au_treasury"

THEMES = ("economics", "government")
TAGS = (
    "orcamento",
    "divida",
    "gasto",
    "receita",
    "despesa",
    "financas_publicas",
    "politica_fiscal",
    "projecao",
)

DATASET_NAME = (
    "Orçamento do Governo da Austrália",
    "Australian Government Budget",
    "Presupuesto del Gobierno de Australia",
)

DATASET_DESCRIPTION = (
    "Agregados fiscais do Governo da Austrália por safra de divulgação, desde "
    "1970-71, e projeções de longo prazo do Relatório Intergeracional. Cada "
    "Orçamento e cada Resultado Orçamentário Final republica a série histórica "
    "inteira na sua própria base, e nenhuma divulgação anterior é sobrescrita "
    "por uma posterior, de modo que as doze safras presentes podem ser "
    "comparadas entre si. Fonte: Tesouro da Austrália, © Commonwealth of "
    "Australia, CC BY 4.0.",
    "Fiscal aggregates of the Australian Government by release vintage, from "
    "1970-71, and long-run projections from the Intergenerational Report. Every "
    "Budget and every Final Budget Outcome republishes the whole historical "
    "series on its own basis, and no earlier release is overwritten by a later "
    "one, so the twelve vintages present can be compared against each other. "
    "Source: the Australian Treasury, © Commonwealth of Australia, CC BY 4.0.",
    "Agregados fiscales del Gobierno de Australia por cosecha de publicación, "
    "desde 1970-71, y proyecciones de largo plazo del Informe Intergeneracional. "
    "Cada Presupuesto y cada Resultado Presupuestario Final republica la serie "
    "histórica completa sobre su propia base, y ninguna publicación anterior es "
    "sobrescrita por una posterior, de modo que las doce cosechas presentes "
    "pueden compararse entre sí. Fuente: el Tesoro de Australia, © Commonwealth "
    "of Australia, CC BY 4.0.",
)

TABLE_NAMES = {
    "aggregate": (
        "Agregados fiscais",
        "Fiscal aggregates",
        "Agregados fiscales",
    ),
    "payment_growth": (
        "Crescimento dos principais pagamentos",
        "Major payment growth",
        "Crecimiento de los principales pagos",
    ),
    "igr_projection": (
        "Projeções do Relatório Intergeracional",
        "Intergenerational Report projections",
        "Proyecciones del Informe Intergeneracional",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}

TABLE_DESCRIPTIONS = {
    "aggregate": (
        "Receitas, pagamentos, saldo de caixa subjacente, dívida líquida, juros "
        "líquidos e outros agregados fiscais do Governo da Austrália, desde "
        "1970-71, por safra de divulgação. Cada linha é uma combinação de "
        "divulgação, exercício financeiro, setor institucional e medida, com até "
        "quatro unidades. Construída a partir do Statement de dados históricos "
        "do Budget Paper No. 1 e do Apêndice B do Final Budget Outcome.",
        "Receipts, payments, the underlying cash balance, net debt, net interest "
        "and other fiscal aggregates of the Australian Government, from 1970-71, "
        "by release vintage. Each row is one combination of release, financial "
        "year, institutional sector and measure, carrying up to four units. "
        "Built from the historical-data Statement of Budget Paper No. 1 and "
        "Appendix B of the Final Budget Outcome.",
        "Ingresos, pagos, saldo de caja subyacente, deuda neta, intereses netos "
        "y otros agregados fiscales del Gobierno de Australia, desde 1970-71, "
        "por cosecha de publicación. Cada fila es una combinación de "
        "publicación, ejercicio financiero, sector institucional y medida, con "
        "hasta cuatro unidades. Construida a partir del Statement de datos "
        "históricos del Budget Paper No. 1 y del Apéndice B del Final Budget "
        "Outcome.",
    ),
    "payment_growth": (
        "Crescimento médio anual dos principais programas de pagamento no médio "
        "prazo, como publicado no Statement 3 do Budget Paper No. 1. O gráfico "
        "de origem plota duas safras lado a lado, de modo que a divulgação que "
        "publica a linha e a divulgação que a linha informa costumam diferir. "
        "As taxas são nominais.",
        "Average annual growth in the major payment programs over the medium "
        "term, as published in Statement 3 of Budget Paper No. 1. The source "
        "chart plots two vintages side by side, so the release that publishes a "
        "row and the release the row reports usually differ. The rates are "
        "nominal.",
        "Crecimiento promedio anual de los principales programas de pago en el "
        "mediano plazo, según se publica en el Statement 3 del Budget Paper No. "
        "1. El gráfico de origen grafica dos cosechas lado a lado, de modo que "
        "la publicación que publica una fila y la publicación que la fila "
        "informa suelen diferir. Las tasas son nominales.",
    ),
    "igr_projection": (
        "Projeções de longo prazo do Relatório Intergeracional de 2023, até "
        "2062-63, da linha de base e de seis cenários alternativos de "
        "sensibilidade. Cobre projeções demográficas, econômicas, fiscais e dos "
        "principais programas de pagamento. As edições de 2021 e 2015 do "
        "relatório não publicaram suas tabelas em formato legível por máquina e "
        "não estão presentes.",
        "Long-run projections from the 2023 Intergenerational Report, out to "
        "2062-63, on the baseline and on six alternative sensitivity scenarios. "
        "Covers demographic, economic, fiscal and major-payment projections. The "
        "2021 and 2015 editions of the report did not publish their tables in a "
        "machine-readable form and are not present.",
        "Proyecciones de largo plazo del Informe Intergeneracional de 2023, "
        "hasta 2062-63, de la línea base y de seis escenarios alternativos de "
        "sensibilidad. Cubre proyecciones demográficas, económicas, fiscales y "
        "de los principales programas de pago. Las ediciones de 2021 y 2015 del "
        "informe no publicaron sus tablas en formato legible por máquina y no "
        "están presentes.",
    ),
    "dicionario": (
        "Dicionário das colunas codificadas do conjunto. Os valores armazenados "
        "já são legíveis, de modo que o dicionário existe para defini-los: o "
        "saldo de caixa subjacente e o saldo de caixa principal diferem por um "
        "termo, e o saldo fiscal não é o saldo de caixa com outro nome.",
        "Dictionary of the dataset's coded columns. The stored values are "
        "already readable, so the dictionary exists to define them: the "
        "underlying and headline cash balances differ by one term, and the "
        "fiscal balance is not the cash balance under another name.",
        "Diccionario de las columnas codificadas del conjunto. Los valores "
        "almacenados ya son legibles, de modo que el diccionario existe para "
        "definirlos: el saldo de caja subyacente y el saldo de caja principal "
        "difieren por un término, y el saldo fiscal no es el saldo de caja con "
        "otro nombre.",
    ),
}

#: One raw data source per table. The client raises when a table has two or
#: more, which would make a recurring pipeline impossible to poll.
RAW_SOURCES = {
    "aggregate": (
        (
            "Budget Paper No. 1 e Final Budget Outcome",
            "Budget Paper No. 1 and Final Budget Outcome",
            "Budget Paper No. 1 y Final Budget Outcome",
        ),
        "https://budget.gov.au/content/bp1/index.htm",
    ),
    "payment_growth": (
        (
            "Dados de gráficos do Orçamento",
            "Budget chart data",
            "Datos de gráficos del Presupuesto",
        ),
        "https://budget.gov.au/content/downloads.htm",
    ),
    "igr_projection": (
        (
            "Relatório Intergeracional de 2023",
            "2023 Intergenerational Report",
            "Informe Intergeneracional de 2023",
        ),
        "https://treasury.gov.au/publication/2023-intergenerational-report",
    ),
}

#: Observation levels, as (entity slug, the column that identifies it).
OBSERVATION_LEVELS = {
    "aggregate": (
        ("year", "year"),
        ("sector", "sector"),
        ("other", "measure"),
    ),
    "payment_growth": (("year", "year"), ("other", "payment_program")),
    "igr_projection": (("year", "year"), ("other", "measure")),
    "dicionario": (),
}

#: Temporal coverage per table, read from the built data rather than declared.
COVERAGE = {
    "aggregate": (1970, 2029),
    "payment_growth": (2022, 2026),
    "igr_projection": (2022, 2062),
}

#: How often the source republishes, in entity units. Treasury delivers a Budget,
#: a MYEFO and a Final Budget Outcome each year, so the dataset is refreshed
#: roughly three times a year; ``frequency`` is expressed in months.
UPDATE_FREQUENCY_MONTHS = 4

#: When *we* last refreshed the tables -- a wall clock, not a coverage date. The
#: backend field is a DateTime, so a bare date is rejected.
REFRESHED_AT = "2026-09-11T00:00:00"

DATA_ROOT = pathlib.Path.home() / "Downloads" / "au_treasury_budget_data"


def strip(node_id: str | None) -> str | None:
    return server._strip_id(node_id) if node_id else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default="staging")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="flip the dataset to published (dev/staging only, before promotion)",
    )
    args = parser.parse_args()
    env = args.env

    ids = server.discover_ids(
        env=env,
        keys=["status", "entity", "license", "availability", "theme", "tag"],
    )
    area_au = strip(
        server.lookup_id(category="area", slug="au", env=env)["id"]
    )
    account = server.get_authenticated_account(env=env)
    account_id = strip(account["id"])
    print(f"account: {account.get('email')}")

    status_id = ids["status"]["published" if args.publish else "under_review"]
    license_id = ids["license"]["cc_by"]
    availability_id = ids["availability"]["online"]

    missing_tags = [t for t in TAGS if t not in ids["tag"]]
    if missing_tags:
        raise SystemExit(f"tags not in the vocabulary: {missing_tags}")
    tag_ids = [ids["tag"][t] for t in TAGS]
    theme_ids = [ids["theme"][t] for t in THEMES]

    # --- organization ----------------------------------------------------
    existing_org = None
    # lookup_id raises when the slug is absent, which is the normal first run.
    with contextlib.suppress(Exception):
        existing_org = server.lookup_id(
            category="organization", slug=ORGANIZATION_SLUG, env=env
        )
    org = server.create_update_organization(
        slug=ORGANIZATION_SLUG,
        name_pt="Tesouro da Austrália",
        name_en="Australian Treasury",
        name_es="Tesoro de Australia",
        description_pt=(
            "Departamento do Tesouro do Governo da Austrália, responsável pela "
            "política econômica e fiscal e pela elaboração do Orçamento"
        ),
        description_en=(
            "Department of the Treasury of the Australian Government, "
            "responsible for economic and fiscal policy and for preparing the "
            "Budget"
        ),
        description_es=(
            "Departamento del Tesoro del Gobierno de Australia, responsable de "
            "la política económica y fiscal y de la elaboración del Presupuesto"
        ),
        area_id=area_au,
        website="https://treasury.gov.au/",
        id=strip(existing_org["id"]) if existing_org else None,
        env=env,
    )
    organization_id = strip(org["id"])
    print(f"organization {ORGANIZATION_SLUG}: {organization_id}")

    # --- dataset ---------------------------------------------------------
    existing = server.get_dataset(slug=DATASET_SLUG, env=env)
    dataset = server.create_update_dataset(
        slug=DATASET_SLUG,
        name_pt=DATASET_NAME[0],
        name_en=DATASET_NAME[1],
        name_es=DATASET_NAME[2],
        description_pt=DATASET_DESCRIPTION[0],
        description_en=DATASET_DESCRIPTION[1],
        description_es=DATASET_DESCRIPTION[2],
        organization_ids=[organization_id],
        theme_ids=theme_ids,
        tag_ids=tag_ids,
        status_id=status_id,
        id=strip(existing["id"]) if existing.get("found") else None,
        env=env,
    )
    dataset_id = strip(dataset["id"])
    print(
        f"dataset {DATASET_SLUG}: {dataset_id}  status={'published' if args.publish else 'under_review'}"
    )

    if args.publish:
        print("published on staging; nothing else to do")
        return 0

    # --- raw data sources -------------------------------------------------
    raw = server.get_raw_data_sources(dataset_slug=DATASET_SLUG, env=env)
    if isinstance(raw, dict):
        raw = raw.get("raw_data_sources", [])
    existing_sources = {s.get("name"): strip(s.get("id")) for s in raw}
    source_ids: dict[str, str] = {}
    for table, (names, url) in RAW_SOURCES.items():
        record = server.create_update_raw_data_source(
            dataset_id=dataset_id,
            name_pt=names[0],
            name_en=names[1],
            name_es=names[2],
            url=url,
            license_id=license_id,
            availability_id=availability_id,
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            id=existing_sources.get(names[0]),
            env=env,
        )
        source_id = strip(record["id"])
        assert source_id is not None
        source_ids[table] = source_id
        print(f"raw source {table}: {source_ids[table]}")

    # --- tables -----------------------------------------------------------
    current = server.get_dataset(slug=DATASET_SLUG, env=env)
    for table in column_defs.TABLES:
        known = current.get("tables", {}).get(table, {})
        record = server.create_update_table(
            slug=table,
            name_pt=TABLE_NAMES[table][0],
            name_en=TABLE_NAMES[table][1],
            name_es=TABLE_NAMES[table][2],
            description_pt=TABLE_DESCRIPTIONS[table][0],
            description_en=TABLE_DESCRIPTIONS[table][1],
            description_es=TABLE_DESCRIPTIONS[table][2],
            dataset_id=dataset_id,
            status_id=ids["status"]["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=(
                [source_ids[table]] if table in source_ids else None
            ),
            id=strip(known["id"]) if known else None,
            env=env,
        )
        table_id = strip(record["id"])
        print(f"\ntable {table}: {table_id}")

        payload = column_defs.columns_json(table)
        result = server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payload, ensure_ascii=False),
            env=env,
        )
        print(
            f"  columns: {result.get('created', '?')} created, "
            f"{result.get('updated', '?')} updated"
        )

        # Observation levels, and the column that identifies each one. Without
        # the per-column link the site renders the level as "Nao informado".
        after = server.get_dataset(slug=DATASET_SLUG, env=env)
        table_record = after["tables"][table]
        column_ids = {
            c["name"]: strip(c["id"]) for c in table_record["columns"]
        }
        existing_levels = {
            strip(level.get("entity_id")): strip(level["id"])
            for level in table_record.get("observation_levels", [])
        }
        for entity_slug, column_name in OBSERVATION_LEVELS[table]:
            entity_id = ids["entity"][entity_slug]
            level = server.create_update_observation_level(
                table_id=table_id,
                entity_id=entity_id,
                id=existing_levels.get(entity_id),
                env=env,
            )
            level_id = strip(level["id"])
            # update_column's booleans default to False, so the partition flag
            # has to be re-passed in the same call that sets the level.
            server.update_column(
                column_id=column_ids[column_name],
                column_name=column_name,
                table_id=table_id,
                observation_level_id=level_id,
                is_partition=(column_name == "year"),
                env=env,
            )
            print(f"  observation level {entity_slug} -> {column_name}")

        if not OBSERVATION_LEVELS[table] and "year" in column_ids:
            server.update_column(
                column_id=column_ids["year"],
                column_name="year",
                table_id=table_id,
                is_partition=True,
                env=env,
            )

        existing_cloud = table_record.get("cloud_tables", [])
        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id="basedosdados",
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=strip(existing_cloud[0]["id"]) if existing_cloud else None,
            env=env,
        )
        print(f"  cloud table -> basedosdados.{GCP_DATASET_ID}.{table}")

        existing_coverages = table_record.get("coverages", [])
        coverage = server.create_update_coverage(
            table_id=table_id,
            area_id=area_au,
            id=strip(existing_coverages[0]["id"])
            if existing_coverages
            else None,
            env=env,
        )
        coverage_id = strip(coverage["id"])
        if table in COVERAGE:
            start_year, end_year = COVERAGE[table]
            ranges = (
                existing_coverages[0].get("datetime_ranges", [])
                if existing_coverages
                else []
            )
            server.create_update_datetime_range(
                coverage_id=coverage_id,
                start_year=start_year,
                end_year=end_year,
                interval=1,
                id=strip(ranges[0]["id"]) if ranges else None,
                env=env,
            )
            print(f"  coverage {start_year}-{end_year}")
        else:
            print("  coverage (no temporal range: the dictionary has no year)")

        existing_updates = table_record.get("updates", [])
        server.create_update_update(
            table_id=table_id,
            entity_id=ids["entity"]["month"],
            frequency=UPDATE_FREQUENCY_MONTHS,
            latest=REFRESHED_AT,
            id=strip(existing_updates[0]["id"]) if existing_updates else None,
            env=env,
        )
        print("  update record")

    server.reorder_tables(
        dataset_slug=DATASET_SLUG,
        table_slugs=list(column_defs.TABLES),
        env=env,
    )
    print(f"\nreordered tables: {list(column_defs.TABLES)}")
    print(f"\nverify: https://staging.basedosdados.org/dataset/{dataset_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
