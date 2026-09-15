"""Register us_census_trade metadata in the Data Basis backend.

Idempotent: every record is looked up via ``get_dataset`` first and its existing
id passed back, because ``create_update_*`` duplicates a record when called
without one.

Registers what does not depend on the data: dataset, raw data source, tables,
columns, observation levels and cloud tables. Coverage, DateTimeRange and
Update are deliberately NOT registered here -- they need the real maximum month,
which is only known once the API has actually been read, and the API needs a key
that is not available locally. Run ``register_coverage.py`` after the first dev
run instead.

Usage: ``~/.venvs/bd-pipelines/bin/python models/us_census_trade/code/register_metadata.py [--env staging]``
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(
    0, "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
import server

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"

DATASET_SLUG = "foreign_trade"
GCP_DATASET_ID = "us_census_trade"
ORG_SLUG = "us_census"
THEMES = ["economics"]
TAGS = [
    "comercio",
    "importacao",
    "exportacao",
    "balanca_comercial",
    "porto",
    "transporte",
    "frete",
]

TABLES = [
    "import",
    "export",
    "import_port",
    "export_port",
    "import_state",
    "export_state",
    "dicionario",
]

# Observation levels per table, and the column that identifies each level.
# A level with no column linked renders as "Não informado" on the site.
OBSERVATION_LEVELS = {
    "import": [
        ("year", "year"),
        ("month", "month"),
        ("country", "country_code"),
        ("customs", "district_code"),
        ("product", "hs6_code"),
    ],
    "export": [
        ("year", "year"),
        ("month", "month"),
        ("country", "country_code"),
        ("customs", "district_code"),
        ("product", "hs6_code"),
    ],
    "import_port": [
        ("year", "year"),
        ("month", "month"),
        ("country", "country_code"),
        ("port", "port_code"),
        ("product", "hs6_code"),
    ],
    "export_port": [
        ("year", "year"),
        ("month", "month"),
        ("country", "country_code"),
        ("port", "port_code"),
        ("product", "hs6_code"),
    ],
    "import_state": [
        ("year", "year"),
        ("month", "month"),
        ("country", "country_code"),
        ("state", "state_abbreviation"),
        ("product", "hs6_code"),
    ],
    "export_state": [
        ("year", "year"),
        ("month", "month"),
        ("country", "country_code"),
        ("state", "state_abbreviation"),
        ("product", "hs6_code"),
    ],
    "dicionario": [],
}

DATASET_NAME = {
    "pt": "Comércio Exterior de Mercadorias",
    "en": "Merchandise Foreign Trade",
    "es": "Comercio Exterior de Mercancías",
}

DATASET_DESC = {
    "pt": (
        "Estatísticas mensais de comércio exterior de mercadorias dos Estados "
        "Unidos publicadas pelo U.S. Census Bureau (Foreign Trade Division), de "
        "janeiro de 2010 em diante. Importações e exportações por código de seis "
        "dígitos do Sistema Harmonizado e país parceiro, em três recortes "
        "geográficos: distrito aduaneiro, porto e estado. Complementa "
        "world_cepii_baci em vez de repeti-la: o BACI é o painel mundial anual "
        "reconciliado entre declarantes, enquanto este conjunto é a fonte "
        "nacional norte-americana em frequência mensal, com quantidade, imposto "
        "calculado e detalhe de distrito aduaneiro e porto que o BACI não "
        "carrega. Os códigos do Sistema Harmonizado são revisados a cada cinco "
        "anos e a coluna hs_revision registra qual revisão vale em cada ano."
    ),
    "en": (
        "Monthly United States merchandise foreign trade statistics published by "
        "the U.S. Census Bureau (Foreign Trade Division), from January 2010 "
        "onwards. Imports and exports by six-digit Harmonized System code and "
        "partner country, on three geographic dimensions: customs district, port "
        "and state. Complements world_cepii_baci rather than repeating it: BACI "
        "is the annual world panel reconciled across reporters, while this "
        "dataset is the United States national source at monthly frequency, with "
        "quantity, calculated duty and the customs district and port detail that "
        "BACI does not carry. Harmonized System codes are revised every five "
        "years and the hs_revision column records which revision applies in each "
        "year."
    ),
    "es": (
        "Estadísticas mensuales de comercio exterior de mercancías de los Estados "
        "Unidos publicadas por el U.S. Census Bureau (Foreign Trade Division), "
        "desde enero de 2010. Importaciones y exportaciones por código de seis "
        "dígitos del Sistema Armonizado y país socio, en tres recortes "
        "geográficos: distrito aduanero, puerto y estado. Complementa "
        "world_cepii_baci en lugar de repetirla: el BACI es el panel mundial "
        "anual reconciliado entre declarantes, mientras que este conjunto es la "
        "fuente nacional estadounidense con frecuencia mensual, con cantidad, "
        "arancel calculado y el detalle de distrito aduanero y puerto que el BACI "
        "no incluye. Los códigos del Sistema Armonizado se revisan cada cinco "
        "años y la columna hs_revision registra qué revisión rige en cada año."
    ),
}

TABLE_NAME = {
    "import": (
        "Importações por Distrito Aduaneiro",
        "Imports by Customs District",
        "Importaciones por Distrito Aduanero",
    ),
    "export": (
        "Exportações por Distrito Aduaneiro",
        "Exports by Customs District",
        "Exportaciones por Distrito Aduanero",
    ),
    "import_port": (
        "Importações por Porto",
        "Imports by Port",
        "Importaciones por Puerto",
    ),
    "export_port": (
        "Exportações por Porto",
        "Exports by Port",
        "Exportaciones por Puerto",
    ),
    "import_state": (
        "Importações por Estado",
        "Imports by State",
        "Importaciones por Estado",
    ),
    "export_state": (
        "Exportações por Estado",
        "Exports by State",
        "Exportaciones por Estado",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}

TABLE_DESC = {
    "import": (
        "Importações mensais por código de seis dígitos do Sistema Harmonizado, "
        "país parceiro e distrito aduaneiro de entrada. Traz valor das "
        "importações gerais e valor das importações para consumo, que são "
        "medidas sobrepostas e não devem ser somadas, além de valor CIF, "
        "despesas, imposto calculado, duas quantidades com suas unidades e a "
        "repartição por modal aéreo e marítimo. É a única tabela do conjunto "
        "com quantidade e imposto.",
        "Monthly imports by six-digit Harmonized System code, partner country "
        "and customs district of entry. Carries general imports value and "
        "imports for consumption value, which are overlapping measures and must "
        "not be summed, plus CIF value, charges, calculated duty, two quantities "
        "with their units and the air and vessel mode split. It is the only "
        "table in the dataset with quantity and duty.",
        "Importaciones mensuales por código de seis dígitos del Sistema "
        "Armonizado, país socio y distrito aduanero de entrada. Incluye valor de "
        "las importaciones generales y valor de las importaciones para consumo, "
        "que son medidas solapadas y no deben sumarse, además de valor CIF, "
        "gastos, arancel calculado, dos cantidades con sus unidades y el reparto "
        "por modo aéreo y marítimo. Es la única tabla del conjunto con cantidad "
        "y arancel.",
    ),
    "export": (
        "Exportações mensais por código de seis dígitos do Sistema Harmonizado, "
        "país parceiro e distrito aduaneiro de saída. O valor é FAS, medido no "
        "porto de saída e sem frete e seguro internacionais. A coluna "
        "domestic_foreign_code separa exportações de mercadorias produzidas nos "
        "Estados Unidos das reexportações de mercadorias estrangeiras e é uma "
        "dimensão da linha, de modo que o total exportado é a soma das duas.",
        "Monthly exports by six-digit Harmonized System code, partner country "
        "and customs district of exit. Value is FAS, measured at the port of "
        "exit and excluding international freight and insurance. The "
        "domestic_foreign_code column separates exports of goods produced in the "
        "United States from re-exports of foreign goods and is a row dimension, "
        "so total exports is the sum of the two.",
        "Exportaciones mensuales por código de seis dígitos del Sistema "
        "Armonizado, país socio y distrito aduanero de salida. El valor es FAS, "
        "medido en el puerto de salida y sin flete ni seguro internacionales. La "
        "columna domestic_foreign_code separa las exportaciones de bienes "
        "producidos en los Estados Unidos de las reexportaciones de bienes "
        "extranjeros y es una dimensión de la fila, por lo que el total "
        "exportado es la suma de ambas.",
    ),
    "import_port": (
        "Importações mensais por código de seis dígitos do Sistema Harmonizado, "
        "país parceiro e porto de entrada. O endpoint por porto publica apenas "
        "as importações gerais e a repartição por modal; valor para consumo, "
        "imposto e quantidade existem somente na tabela import. Os dois "
        "primeiros dígitos de port_code são o distrito aduaneiro.",
        "Monthly imports by six-digit Harmonized System code, partner country "
        "and port of entry. The port endpoint publishes only general imports and "
        "the mode split; consumption value, duty and quantity exist only in the "
        "import table. The first two digits of port_code are the customs "
        "district.",
        "Importaciones mensuales por código de seis dígitos del Sistema "
        "Armonizado, país socio y puerto de entrada. El endpoint por puerto "
        "publica solo las importaciones generales y el reparto por modo; valor "
        "para consumo, arancel y cantidad existen solo en la tabla import. Los "
        "dos primeros dígitos de port_code son el distrito aduanero.",
    ),
    "export_port": (
        "Exportações mensais por código de seis dígitos do Sistema Harmonizado, "
        "país parceiro e porto de saída. O endpoint por porto não publica "
        "quantidade nem a separação entre exportação doméstica e reexportação, "
        "que existem somente na tabela export. Os dois primeiros dígitos de "
        "port_code são o distrito aduaneiro.",
        "Monthly exports by six-digit Harmonized System code, partner country "
        "and port of exit. The port endpoint publishes neither quantity nor the "
        "domestic export versus re-export split, which exist only in the export "
        "table. The first two digits of port_code are the customs district.",
        "Exportaciones mensuales por código de seis dígitos del Sistema "
        "Armonizado, país socio y puerto de salida. El endpoint por puerto no "
        "publica cantidad ni la separación entre exportación doméstica y "
        "reexportación, que existen solo en la tabla export. Los dos primeros "
        "dígitos de port_code son el distrito aduanero.",
    ),
    "import_state": (
        "Importações mensais por código de seis dígitos do Sistema Harmonizado, "
        "país parceiro e estado de destino declarado. O endpoint por estado não "
        "publica quantidade nem imposto. O estado de destino é o declarado na "
        "entrada e não necessariamente onde a mercadoria é consumida.",
        "Monthly imports by six-digit Harmonized System code, partner country "
        "and declared state of destination. The state endpoint publishes neither "
        "quantity nor duty. The state of destination is the one declared at "
        "entry and not necessarily where the goods are consumed.",
        "Importaciones mensuales por código de seis dígitos del Sistema "
        "Armonizado, país socio y estado de destino declarado. El endpoint por "
        "estado no publica cantidad ni arancel. El estado de destino es el "
        "declarado en la entrada y no necesariamente donde se consume la "
        "mercancía.",
    ),
    "export_state": (
        "Exportações mensais por código de seis dígitos do Sistema Harmonizado, "
        "país parceiro e estado de origem do movimento. O estado de origem do "
        "movimento é a localização do exportador e não necessariamente onde a "
        "mercadoria foi produzida, ressalva feita pelo próprio Census Bureau. O "
        "endpoint por estado não publica quantidade.",
        "Monthly exports by six-digit Harmonized System code, partner country "
        "and state of origin of movement. The state of origin of movement is the "
        "exporter's location and not necessarily where the goods were produced, "
        "a caveat the Census Bureau itself makes. The state endpoint does not "
        "publish quantity.",
        "Exportaciones mensuales por código de seis dígitos del Sistema "
        "Armonizado, país socio y estado de origen del movimiento. El estado de "
        "origen del movimiento es la ubicación del exportador y no "
        "necesariamente donde se produjeron los bienes, salvedad que hace el "
        "propio Census Bureau. El endpoint por estado no publica cantidad.",
    ),
    "dicionario": (
        "Dicionário com as traduções dos códigos usados nas seis tabelas de "
        "fatos. Construído a partir das listas de códigos publicadas pelo Census "
        "Bureau, a Schedule C para países e a Schedule D para distritos "
        "aduaneiros e portos, e não a partir dos valores observados numa janela "
        "de meses, de modo que a cobertura é completa.",
        "Dictionary translating the codes used in the six fact tables. Built "
        "from the code lists the Census Bureau publishes, Schedule C for "
        "countries and Schedule D for customs districts and ports, rather than "
        "from the values observed in a window of months, so coverage is "
        "complete.",
        "Diccionario con las traducciones de los códigos usados en las seis "
        "tablas de hechos. Construido a partir de las listas de códigos "
        "publicadas por el Census Bureau, la Schedule C para países y la "
        "Schedule D para distritos aduaneros y puertos, y no a partir de los "
        "valores observados en una ventana de meses, por lo que la cobertura es "
        "completa.",
    ),
}

RAW_SOURCE = {
    "name_pt": "API de Comércio Internacional do U.S. Census Bureau",
    "name_en": "U.S. Census Bureau International Trade API",
    "name_es": "API de Comercio Internacional del U.S. Census Bureau",
    "url": "https://api.census.gov/data/timeseries/intltrade/",
    "description_pt": (
        "Série temporal mensal de importações e exportações por classificação de "
        "mercadorias, distrito aduaneiro, porto e estado, de janeiro de 2010 em "
        "diante, atualizada mensalmente no dia da divulgação. Toda requisição "
        "exige uma chave de API gratuita. Obra do governo dos Estados Unidos, em "
        'domínio público. Os termos de uso da API exigem o aviso: "This product '
        "uses the Census Bureau Data API but is not endorsed or certified by the "
        'Census Bureau".'
    ),
    "description_en": (
        "Monthly time series of imports and exports by commodity classification, "
        "customs district, port and state, from January 2010 onwards, updated "
        "monthly on the day of the press release. Every request requires a free "
        "API key. A United States Government work, in the public domain. The API "
        'terms of service require the notice: "This product uses the Census '
        "Bureau Data API but is not endorsed or certified by the Census "
        'Bureau".'
    ),
    "description_es": (
        "Serie temporal mensual de importaciones y exportaciones por "
        "clasificación de mercancías, distrito aduanero, puerto y estado, desde "
        "enero de 2010, actualizada mensualmente el día de la divulgación. Toda "
        "solicitud requiere una clave de API gratuita. Obra del gobierno de los "
        "Estados Unidos, en dominio público. Los términos de uso de la API "
        'exigen el aviso: "This product uses the Census Bureau Data API but is '
        'not endorsed or certified by the Census Bureau".'
    ),
}


def read_arch(table: str) -> list[dict]:
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def columns_json(table: str) -> str:
    out = []
    for a in read_arch(table):
        out.append(
            {
                "name": a["name"],
                "bigquery_type": a["bigquery_type"],
                "description_pt": a["description_pt"],
                "description_en": a["description_en"],
                "description_es": a["description_es"],
                "covered_by_dictionary": a["covered_by_dictionary"] == "yes",
                "directory_column": a["directory_column"],
                "measurement_unit": a["measurement_unit"],
                "has_sensitive_data": a["has_sensitive_data"] == "yes",
                "observations_pt": a["observations_pt"],
                "observations_en": a["observations_en"],
                "observations_es": a["observations_es"],
            }
        )
    return json.dumps(out, ensure_ascii=False)


def bare_id(value: str) -> str:
    """Strip a GraphQL node prefix such as 'EntityNode:' from an id."""
    return value.split(":")[-1] if value else value


def entity_ids(env: str) -> dict[str, str]:
    """Resolve the entity ids the observation levels need, creating `port`.

    `port` does not exist in the shared entity vocabulary. It is created here
    under the spatial category, alongside state, country and city. This is new
    shared vocabulary and is reported so it can be vetoed or renamed.
    """
    wanted = {
        "year",
        "month",
        "country",
        "customs",
        "state",
        "product",
        "port",
    }
    q = "query($s: String) { allEntity(slug: $s) { edges { node { id slug } } } }"
    out = {}
    for slug in sorted(wanted):
        res = server._gql(q, {"s": slug}, env=env)
        edges = res["allEntity"]["edges"]
        if edges:
            out[slug] = bare_id(edges[0]["node"]["id"])
    missing = wanted - set(out)
    for slug in sorted(missing):
        if slug != "port":
            raise RuntimeError(f"entity {slug!r} missing and not auto-created")
        cat = server._gql(
            "query($s: String) { allEntitycategory(slug: $s) "
            "{ edges { node { id } } } }",
            {"s": "spatial"},
            env=env,
        )["allEntitycategory"]["edges"][0]["node"]["id"]
        res = server.create_update_entity(
            slug="port",
            name_pt="Porto",
            name_en="Port",
            name_es="Puerto",
            category_id=bare_id(cat),
            env=env,
        )
        out["port"] = bare_id(res["id"])
        print(f"  CREATED new shared entity 'port' -> {out['port']}")
    return out


def main(env: str) -> None:
    print(f"=== us_census_trade metadata registration (env={env}) ===")
    account = server.get_authenticated_account(env=env)
    account_id = bare_id(account["id"])
    print(f"account: {account.get('email')}")

    refs = server.discover_ids(
        env=env, keys=["status", "theme", "license", "availability"]
    )
    status_under_review = refs["status"]["under_review"]
    status_published = refs["status"]["published"]
    theme_ids = [refs["theme"][t] for t in THEMES]
    org_id = server.lookup_id(category="organization", slug=ORG_SLUG, env=env)[
        "id"
    ]
    tag_ids = [
        server.lookup_id(category="tag", slug=t, env=env)["id"] for t in TAGS
    ]
    print(f"org={ORG_SLUG} themes={THEMES} tags={TAGS}")

    ents = entity_ids(env)

    existing = server.get_dataset(slug=DATASET_SLUG, env=env)
    ds = server.create_update_dataset(
        slug=DATASET_SLUG,
        name_pt=DATASET_NAME["pt"],
        name_en=DATASET_NAME["en"],
        name_es=DATASET_NAME["es"],
        description_pt=DATASET_DESC["pt"],
        description_en=DATASET_DESC["en"],
        description_es=DATASET_DESC["es"],
        organization_ids=[org_id],
        theme_ids=theme_ids,
        tag_ids=tag_ids,
        # under_review until the PR merges and the prod tables materialise.
        status_id=status_under_review,
        id=existing["id"] if existing["found"] else None,
        env=env,
    )
    dataset_id = bare_id(ds["id"])
    print(f"dataset {DATASET_SLUG} -> {dataset_id}")

    # get_raw_data_sources returns a plain list, not a wrapper dict.
    sources = server.get_raw_data_sources(dataset_slug=DATASET_SLUG, env=env)
    src_existing = next(
        (s for s in sources if s["url"] == RAW_SOURCE["url"]), None
    )
    src = server.create_update_raw_data_source(
        dataset_id=dataset_id,
        license_id=refs["license"]["unknown"],
        availability_id=refs["availability"]["online"],
        has_structured_data=True,
        is_free=True,
        contains_api=True,
        requires_registration=True,
        id=bare_id(src_existing["id"]) if src_existing else None,
        env=env,
        **RAW_SOURCE,
    )
    source_id = bare_id(src["id"])
    print(f"raw data source -> {source_id}")

    existing = server.get_dataset(slug=DATASET_SLUG, env=env)
    for table in TABLES:
        prev = existing["tables"].get(table, {})
        pt, en, es = TABLE_NAME[table]
        dpt, den, des = TABLE_DESC[table]
        res = server.create_update_table(
            slug=table,
            name_pt=pt,
            name_en=en,
            name_es=es,
            description_pt=dpt,
            description_en=den,
            description_es=des,
            dataset_id=dataset_id,
            # Tables stay published; the dataset's under_review status is what
            # hides everything from the production frontend.
            status_id=status_published,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=[source_id],
            id=bare_id(prev["id"]) if prev else None,
            env=env,
        )
        table_id = bare_id(res["id"])

        cols = server.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_json(table), env=env
        )
        print(
            f"  {table:14s} id={table_id} "
            f"columns created={cols.get('created')} updated={cols.get('updated')} "
            f"errors={len(cols.get('errors') or [])}"
        )
        if cols.get("errors"):
            print("   ERRORS:", cols["errors"][:3])

        # Observation levels, and the column that identifies each. Without the
        # column link the site renders the level as "Não informado".
        prev_ols = {
            bare_id(o.get("entity_id", "")): bare_id(o["id"])
            for o in prev.get("observation_levels", [])
        }
        after = server.get_dataset(slug=DATASET_SLUG, env=env)["tables"][table]
        col_ids = {c["name"]: bare_id(c["id"]) for c in after["columns"]}
        for entity_slug, col_name in OBSERVATION_LEVELS[table]:
            ent_id = ents[entity_slug]
            ol = server.create_update_observation_level(
                table_id=table_id,
                entity_id=ent_id,
                id=prev_ols.get(ent_id),
                env=env,
            )
            ol_id = bare_id(ol["id"])
            arch = {a["name"]: a for a in read_arch(table)}
            a = arch[col_name]
            # update_column's booleans default to False, so a bare call would
            # clobber is_partition. Re-pass every flag that must survive.
            server.update_column(
                column_id=col_ids[col_name],
                column_name=col_name,
                table_id=table_id,
                observation_level_id=ol_id,
                is_partition=(col_name == "year"),
                covered_by_dictionary=a["covered_by_dictionary"] == "yes",
                directory_column_name=a["directory_column"],
                measurement_unit=a["measurement_unit"],
                env=env,
            )

        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id="basedosdados-dev",
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=bare_id(prev["cloud_tables"][0]["id"])
            if prev.get("cloud_tables")
            else None,
            env=env,
        )

    server.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLES, env=env
    )
    print("\nNOT registered here (needs the real max month, i.e. an API key):")
    print(
        "  Coverage, DateTimeRange, Update -> run register_coverage.py after the dev run"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    main(ap.parse_args().env)
