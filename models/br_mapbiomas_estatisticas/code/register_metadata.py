"""Register br_mapbiomas_estatisticas metadata in the Data Basis backend.

The dataset and its nine tables already existed as an empty shell registered for
an earlier MapBiomas collection. This script refreshes the six tables that
Collection 11 can actually fill, and leaves the three municipal transition tables
alone -- MapBiomas publishes transitions by biome and state only, so they have no
source. See VALIDATION.md, section 7.

`columns_json/` is gitignored because it is regenerable, so run
`build_architecture.py` first on a fresh checkout.

    python models/br_mapbiomas_estatisticas/code/build_architecture.py
    ~/.venvs/bd-pipelines/bin/python \
        models/br_mapbiomas_estatisticas/code/register_metadata.py --env staging

Run against staging first. Prod runs only after the PR merges and the
table-approve action has materialised the tables.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MCP_DIR = (
    Path.home()
    / "Monash Uni Enterprise Dropbox"
    / "Ricardo Dahis"
    / "BD"
    / "mcp"
)
sys.path.insert(0, str(MCP_DIR))

# pyrefly: ignore [missing-import]
import server  # noqa: E402


def _columns(table_id: str, env: str) -> list[dict]:
    """Read a table's columns back, with ids, after the bulk upsert."""
    query = """
    query($id: ID!) {
      allTable(id: $id) { edges { node { columns { edges { node { id name } } } } } }
    }
    """
    data = server._gql(query, {"id": table_id}, env=env, auth=True)
    edges = data["allTable"]["edges"]
    if not edges:
        raise SystemExit(f"table {table_id} not found when reading columns")
    return [
        {"id": server._strip_id(e["node"]["id"]), "name": e["node"]["name"]}
        for e in edges[0]["node"]["columns"]["edges"]
    ]


HERE = Path(__file__).resolve().parent
JSON_DIR = HERE / "columns_json"

DATASET_SLUG = "estatisticas"
GCP_DATASET_ID = "br_mapbiomas_estatisticas"

# Registered, but MapBiomas publishes no municipal transition statistics, so
# these have no source. Left in place, ordered last.
UNBUILDABLE = [
    "transicao_municipio_de_para_anual",
    "transicao_municipio_de_para_quinquenal",
    "transicao_municipio_de_para_decenal",
]

# Kept as a literal because create_update_table does no partial updates: any
# field omitted is written empty, and get_dataset returns no name keys at all.
TABLE_NAMES = {
    "cobertura_municipio_classe": (
        "Cobertura e uso da terra por município",
        "Land cover and land use by municipality",
        "Cobertura y uso del suelo por municipio",
    ),
    "cobertura_uf_classe": (
        "Cobertura e uso da terra por unidade da federação",
        "Land cover and land use by state",
        "Cobertura y uso del suelo por estado",
    ),
    "transicao_uf_de_para_anual": (
        "Transições anuais de cobertura e uso da terra por unidade da federação",
        "Annual land cover and land use transitions by state",
        "Transiciones anuales de cobertura y uso del suelo por estado",
    ),
    "transicao_uf_de_para_quinquenal": (
        "Transições quinquenais de cobertura e uso da terra por unidade da federação",
        "Five-year land cover and land use transitions by state",
        "Transiciones quinquenales de cobertura y uso del suelo por estado",
    ),
    "transicao_uf_de_para_decenal": (
        "Transições decenais de cobertura e uso da terra por unidade da federação",
        "Ten-year land cover and land use transitions by state",
        "Transiciones decenales de cobertura y uso del suelo por estado",
    ),
    "classe": (
        "Dicionário de classes da legenda",
        "Legend class dictionary",
        "Diccionario de clases de la leyenda",
    ),
}

TABLE_DESCRIPTIONS = {
    "cobertura_municipio_classe": (
        "Área, em hectares, de cada classe de cobertura e uso da terra por "
        "município, bioma e ano, na Coleção 11 do MapBiomas (mapeamento anual "
        "em resolução de 30 metros, 1985 a 2025). Municípios que se estendem "
        "por mais de um bioma aparecem em uma linha por bioma.",
        "Area, in hectares, of each land cover and land use class by "
        "municipality, biome and year, in MapBiomas Collection 11 (annual "
        "mapping at 30 metre resolution, 1985 to 2025). Municipalities spanning "
        "more than one biome appear in one row per biome.",
        "Área, en hectáreas, de cada clase de cobertura y uso del suelo por "
        "municipio, bioma y año, en la Colección 11 de MapBiomas (mapeo anual "
        "con resolución de 30 metros, 1985 a 2025). Los municipios que abarcan "
        "más de un bioma aparecen en una fila por bioma.",
    ),
    "cobertura_uf_classe": (
        "Área, em hectares, de cada classe de cobertura e uso da terra por "
        "unidade da federação, bioma e ano, na Coleção 11 do MapBiomas. "
        "Agregada a partir da tabela municipal.",
        "Area, in hectares, of each land cover and land use class by state, "
        "biome and year, in MapBiomas Collection 11. Aggregated from the "
        "municipal table.",
        "Área, en hectáreas, de cada clase de cobertura y uso del suelo por "
        "estado, bioma y año, en la Colección 11 de MapBiomas. Agregada a "
        "partir de la tabla municipal.",
    ),
    "transicao_uf_de_para_anual": (
        "Área, em hectares, que passou de cada classe de cobertura e uso da "
        "terra para cada outra classe, por unidade da federação e bioma, entre "
        "anos consecutivos de 1985 a 2025.",
        "Area, in hectares, that changed from each land cover and land use "
        "class to each other class, by state and biome, between consecutive "
        "years from 1985 to 2025.",
        "Área, en hectáreas, que pasó de cada clase de cobertura y uso del "
        "suelo a cada otra clase, por estado y bioma, entre años consecutivos "
        "de 1985 a 2025.",
    ),
    "transicao_uf_de_para_quinquenal": (
        "Área, em hectares, que passou de cada classe de cobertura e uso da "
        "terra para cada outra classe, por unidade da federação e bioma, em "
        "períodos de cinco anos de 1985 a 2025.",
        "Area, in hectares, that changed from each land cover and land use "
        "class to each other class, by state and biome, over five-year periods "
        "from 1985 to 2025.",
        "Área, en hectáreas, que pasó de cada clase de cobertura y uso del "
        "suelo a cada otra clase, por estado y bioma, en períodos de cinco años "
        "de 1985 a 2025.",
    ),
    "transicao_uf_de_para_decenal": (
        "Área, em hectares, que passou de cada classe de cobertura e uso da "
        "terra para cada outra classe, por unidade da federação e bioma, em "
        "períodos de dez anos de 1990 a 2020.",
        "Area, in hectares, that changed from each land cover and land use "
        "class to each other class, by state and biome, over ten-year periods "
        "from 1990 to 2020.",
        "Área, en hectáreas, que pasó de cada clase de cobertura y uso del "
        "suelo a cada otra clase, por estado y bioma, en períodos de diez años "
        "de 1990 a 2020.",
    ),
    "classe": (
        "Legenda hierárquica de cobertura e uso da terra da Coleção 11 do "
        "MapBiomas, com os rótulos dos quatro níveis em português, inglês e "
        "espanhol. Os rótulos em espanhol são tradução da Base dos Dados, já "
        "que o MapBiomas Brasil não publica legenda nesse idioma.",
        "Hierarchical land cover and land use legend of MapBiomas Collection "
        "11, with the labels of all four levels in Portuguese, English and "
        "Spanish. The Spanish labels are a Data Basis translation, as MapBiomas "
        "Brasil publishes no legend in that language.",
        "Leyenda jerárquica de cobertura y uso del suelo de la Colección 11 de "
        "MapBiomas, con las etiquetas de los cuatro niveles en portugués, "
        "inglés y español. Las etiquetas en español son traducción de Data "
        "Basis, ya que MapBiomas Brasil no publica leyenda en ese idioma.",
    ),
}

# entity slug -> the column that identifies that level, per table.
#
# The land-cover class is a real dimension of every one of these tables, and the
# earlier registration recorded it against the "unknown" / "other" entities
# without linking it to a column, which renders as "Não informado" on the site.
# There is no "land cover class" entity, so the class dimension is standardised
# on `terrain` -- which is what this dataset's own `classe` table already used --
# and linked to the class column.
OBSERVATION_LEVELS = {
    "cobertura_municipio_classe": {
        "municipality": "id_municipio",
        "year": "ano",
        "terrain": "id_classe",
    },
    "cobertura_uf_classe": {
        "state": "sigla_uf",
        "year": "ano",
        "terrain": "id_classe",
    },
    "transicao_uf_de_para_anual": {
        "state": "sigla_uf",
        "year": "ano",
        "terrain": "id_classe_de",
    },
    "transicao_uf_de_para_quinquenal": {
        "state": "sigla_uf",
        "year": "ano",
        "terrain": "id_classe_de",
    },
    "transicao_uf_de_para_decenal": {
        "state": "sigla_uf",
        "year": "ano",
        "terrain": "id_classe_de",
    },
    "classe": {"terrain": "chave"},
}

COVERAGE_YEARS = {
    "cobertura_municipio_classe": (1985, 2025, 1),
    "cobertura_uf_classe": (1985, 2025, 1),
    "transicao_uf_de_para_anual": (1986, 2025, 1),
    "transicao_uf_de_para_quinquenal": (1990, 2025, 5),
    "transicao_uf_de_para_decenal": (2000, 2020, 10),
}

# The en dashes below are part of the attribution MapBiomas requires; they are
# reproduced verbatim, hence the RUF001 suppressions.
DATASET_DESCRIPTION = (
    "Mapeamento anual da cobertura e uso da terra do Brasil produzido pelo "
    "Projeto MapBiomas a partir da classificação automática de imagens de "
    "satélite no Google Earth Engine. Os dados desta versão vêm da Coleção 11, "
    "publicada em agosto de 2026, que cobre 1985 a 2025 em resolução de 30 "
    "metros. Distribuído sob licença CC BY 4.0, com a referência exigida pela "
    'fonte: "Projeto MapBiomas – Coleção 11 da Série Anual de Mapas de '  # noqa: RUF001
    "Cobertura e Uso da Terra do Brasil, acessado em 24 de setembro de 2026 "
    'através do link: https://brasil.mapbiomas.org/downloads/estatisticas/".',
    "Annual mapping of land cover and land use in Brazil produced by the "
    "MapBiomas Project through automatic classification of satellite imagery on "
    "Google Earth Engine. The data in this version come from Collection 11, "
    "published in August 2026, covering 1985 to 2025 at 30 metre resolution. "
    "Distributed under a CC BY 4.0 licence, with the attribution the source "
    'requires: "Project MapBiomas - Collection 11 of Brazilian Land Cover & '
    "Use Map Series, accessed on 24 September 2026 through the link: "
    'https://brasil.mapbiomas.org/downloads/estatisticas/".',
    "Mapeo anual de la cobertura y uso del suelo de Brasil producido por el "
    "Proyecto MapBiomas mediante la clasificación automática de imágenes "
    "satelitales en Google Earth Engine. Los datos de esta versión provienen de "
    "la Colección 11, publicada en agosto de 2026, que cubre de 1985 a 2025 con "
    "resolución de 30 metros. Distribuido bajo licencia CC BY 4.0, con la "
    'referencia que exige la fuente: "Projeto MapBiomas – Coleção 11 da Série '  # noqa: RUF001
    "Anual de Mapas de Cobertura e Uso da Terra do Brasil, acessado em 24 de "
    "setembro de 2026 através do link: "
    'https://brasil.mapbiomas.org/downloads/estatisticas/".',
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env", default="staging", choices=["staging", "dev", "prod"]
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = args.env

    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    dataset = server.get_dataset(slug=DATASET_SLUG, env=env)
    if not dataset["found"]:
        raise SystemExit(f"dataset {DATASET_SLUG} not found in {env}")
    dataset_id = dataset["id"]
    print(f"dataset {DATASET_SLUG} = {dataset_id} ({env})")

    ids = server.discover_ids(
        env=env, keys=["status", "entity", "license", "availability"]
    )
    account = server.get_authenticated_account(env=env)
    account_id = account["id"]

    if args.dry_run:
        print(
            "dry run: would update dataset description and",
            len(TABLE_NAMES),
            "tables",
        )
        for table in TABLE_NAMES:
            existing = dataset["tables"].get(table)
            print(f"  {table}: {'found' if existing else 'MISSING'}")
        return

    # The dataset is already published; refresh its description for Collection
    # 11 and keep every other field as it is. create_update_dataset does no
    # partial updates, so every required field is re-passed explicitly.
    desc_pt, desc_en, desc_es = DATASET_DESCRIPTION
    server.create_update_dataset(
        id=dataset_id,
        slug=DATASET_SLUG,
        name_pt=dataset["name_pt"],
        name_en=dataset["name_en"],
        name_es=dataset["name_es"],
        description_pt=desc_pt,
        description_en=desc_en,
        description_es=desc_es,
        organization_ids=[o["id"] for o in dataset["organizations"]],
        theme_ids=[t["id"] for t in dataset["themes"]],
        tag_ids=[t["id"] for t in dataset["tags"]],
        status_id=ids["status"]["published"],
        env=env,
    )
    print("dataset description refreshed for Collection 11")

    for table, (name_pt, name_en, name_es) in TABLE_NAMES.items():
        existing = dataset["tables"].get(table)
        if existing is None:
            raise SystemExit(
                f"table {table} is not registered; create it first"
            )
        table_id = existing["id"]
        desc_pt, desc_en, desc_es = TABLE_DESCRIPTIONS[table]
        print(f"\n== {table} ({table_id})")

        server.create_update_table(
            id=table_id,
            slug=table,
            dataset_id=dataset_id,
            name_pt=name_pt,
            name_en=name_en,
            name_es=name_es,
            description_pt=desc_pt,
            description_en=desc_en,
            description_es=desc_es,
            status_id=ids["status"]["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            env=env,
        )
        print("  table updated")

        columns = json.loads(
            (JSON_DIR / f"{table}.json").read_text(encoding="utf-8")
        )
        server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(columns, ensure_ascii=False),
            env=env,
        )
        print(f"  {len(columns)} columns upserted")

        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=(existing["cloud_tables"] or [{}])[0].get("id"),
            env=env,
        )
        print(f"  cloud table -> {gcp_project}.{GCP_DATASET_ID}.{table}")

        # Observation levels, and the column that identifies each. Without the
        # per-column link the site renders the level's columns as "Não informado".
        by_entity = {
            level.get("entity_slug"): level["id"]
            for level in (existing["observation_levels"] or [])
        }
        for entity_slug, column_name in OBSERVATION_LEVELS[table].items():
            level_id = by_entity.get(entity_slug)
            result = server.create_update_observation_level(
                table_id=table_id,
                entity_id=ids["entity"][entity_slug],
                id=level_id,
                env=env,
            )
            level_id = level_id or result.get("id")
            column_id = next(
                (
                    c["id"]
                    for c in _columns(table_id, env)
                    if c["name"] == column_name
                ),
                None,
            )
            if column_id is None:
                raise SystemExit(f"{table}: column {column_name} not found")
            server.update_column(
                column_id=column_id,
                column_name=column_name,
                table_id=table_id,
                observation_level_id=level_id,
                # update_column's booleans default to False, so a bare call
                # would clear the partition flag on `ano`.
                is_partition=column_name == "ano" and table != "classe",
                env=env,
            )
            print(f"  observation level {entity_slug} -> {column_name}")

        # Levels from the earlier collection whose entity is not one this table
        # declares. They carry no column link, so the site shows them as
        # "Não informado"; there is no MCP tool to remove one, hence the raw
        # mutation (the argument is UUID!, not ID!).
        for entity_slug, level_id in by_entity.items():
            if entity_slug in OBSERVATION_LEVELS[table]:
                continue
            server._gql(
                "mutation($id: UUID!) "
                "{ DeleteObservationLevel(id: $id) { ok errors } }",
                {"id": level_id},
                env=env,
                auth=True,
            )
            print(f"  removed stale observation level {entity_slug!r}")

        if table in COVERAGE_YEARS:
            start, end, interval = COVERAGE_YEARS[table]
            coverage = (existing["coverages"] or [{}])[0]
            coverage_id = coverage.get("id")
            if coverage_id is None:
                coverage_id = server.create_update_coverage(
                    table_id=table_id,
                    area_id=server.lookup_id(
                        category="area", slug="br", env=env
                    )["id"],
                    env=env,
                )["id"]
            ranges = coverage.get("datetime_ranges") or [{}]
            server.create_update_datetime_range(
                coverage_id=coverage_id,
                id=ranges[0].get("id"),
                start_year=start,
                end_year=end,
                interval=interval,
                env=env,
            )
            print(f"  coverage {start}-{end} (interval {interval})")

    # Filled tables first; the three that have no source sink to the bottom.
    server.reorder_tables(
        dataset_slug=DATASET_SLUG,
        table_slugs=[*TABLE_NAMES, *UNBUILDABLE],
        env=env,
    )
    print("\ntables reordered: filled first, sourceless last")

    print("\ndone. Municipal transition tables were not touched:")
    print("  transicao_municipio_de_para_{anual,quinquenal,decenal}")
    print("  MapBiomas publishes no municipal transition statistics.")


if __name__ == "__main__":
    main()
