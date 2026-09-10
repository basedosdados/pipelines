"""Register world_noaa_ghcn metadata in the Data Basis backend.

    python models/world_noaa_ghcn/code/register_metadata.py --env staging
    python models/world_noaa_ghcn/code/register_metadata.py --env prod

Idempotent by construction: every create_update_* call is passed the existing
record's id when one is found, because these endpoints are NOT idempotent
without it and would otherwise duplicate observation levels, cloud tables,
coverages and updates on a re-run.

Columns come from code/architecture/*.csv, which is the source of truth for
names, types, descriptions and units.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(
    0,
    "/Users/rdahis/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp",
)
import server

ARCH = Path(__file__).parent / "architecture"
DATASET_SLUG = "ghcn_daily"
GCP_DATASET_ID = "world_noaa_ghcn"

# Reference ids differ between staging and prod -- cc0, `station` and `date`
# all carry different UUIDs in the two environments -- so they are resolved by
# slug at run time rather than hardcoded. Hardcoding them would have silently
# attached the wrong licence and dropped two observation levels on prod.
REFERENCES = {
    "organization": "noaa",
    "theme": "environment",
    "area": "world",
    "license": "cc0",
    "availability": "online",
}
ENTITY_SLUGS = {
    "station": "station",  # one row per weather station
    "date": "date",  # daily grain of the observation table
    "day": "day",  # cadence of the table Update record
    "other": "other",  # the meteorological element dimension
}
# Tag slugs are English on prod and Portuguese on staging, though the two
# environments share the same UUIDs. Each entry is (prod slug, staging slug) and
# the lookup tries them in turn, so the same script runs against both.
TAG_SLUGS = [
    ("temperature", "temperatura"),
    ("precipitation", "precipitacao"),
    ("rain", "chuva"),
    ("climate", "clima"),
    ("meteorology", "meteorologia"),
    ("climate_change", "mudancas_climaticas"),
    ("wind", "vento"),
    ("humidity", "umidade"),
]


def _lookup(category: str, slugs: tuple[str, ...], env: str) -> str:
    """Resolve a reference id, trying each slug the environments may use."""
    for slug in slugs:
        try:
            return server.lookup_id(category=category, slug=slug, env=env)[
                "id"
            ]
        except RuntimeError:
            continue
    raise RuntimeError(f"{category} not found in {env} under any of {slugs}")


def resolve(env: str) -> dict[str, str]:
    """Look every reference id up by slug in the target environment."""
    ids = {}
    for kind, slug in REFERENCES.items():
        ids[kind] = _lookup(kind, (slug,), env)
    for key, slug in ENTITY_SLUGS.items():
        ids[f"entity_{key}"] = _lookup("entity", (slug,), env)
    ids["tags"] = [_lookup("tag", pair, env) for pair in TAG_SLUGS]
    return ids


DATASET = {
    "name_pt": "Global Historical Climatology Network - Daily (GHCN-Daily)",
    "name_en": "Global Historical Climatology Network - Daily (GHCN-Daily)",
    "name_es": "Global Historical Climatology Network - Daily (GHCN-Daily)",
    "description_pt": (
        "Observações meteorológicas diárias de superfície de 132.501 estações "
        "em todo o mundo, compiladas pelo NOAA National Centers for "
        "Environmental Information a partir de mais de trinta fontes "
        "nacionais e internacionais. A série cobre de 1763 a 2026 e reúne 144 "
        "elementos, entre eles temperatura máxima e mínima, precipitação, "
        "queda de neve e profundidade de neve. Cada observação carrega "
        "sinalizadores de medição, qualidade e fonte; um sinalizador de "
        "qualidade não nulo indica que o valor reprovou no controle de "
        "qualidade do NCEI. Não confundir com o produto nClimGrid, que é a "
        "versão interpolada em grade de 5 km para os Estados Unidos "
        "continentais."
    ),
    "description_en": (
        "Daily land surface weather observations from 132,501 stations "
        "worldwide, compiled by the NOAA National Centers for Environmental "
        "Information from more than thirty national and international "
        "sources. The record spans 1763 to 2026 and covers 144 elements, "
        "among them maximum and minimum temperature, precipitation, snowfall "
        "and snow depth. Every observation carries measurement, quality and "
        "source flags; a non-null quality flag means the value failed NCEI's "
        "quality assurance. Not to be confused with the nClimGrid product, "
        "which is the interpolated 5 km gridded version for the contiguous "
        "United States."
    ),
    "description_es": (
        "Observaciones meteorológicas diarias de superficie de 132.501 "
        "estaciones en todo el mundo, compiladas por el NOAA National Centers "
        "for Environmental Information a partir de más de treinta fuentes "
        "nacionales e internacionales. La serie abarca de 1763 a 2026 y reúne "
        "144 elementos, entre ellos temperatura máxima y mínima, "
        "precipitación, nevada y profundidad de nieve. Cada observación lleva "
        "indicadores de medición, calidad y fuente; un indicador de calidad no "
        "nulo señala que el valor reprobó el control de calidad del NCEI. No "
        "confundir con el producto nClimGrid, que es la versión interpolada en "
        "cuadrícula de 5 km para los Estados Unidos continentales."
    ),
}

TABLES = {
    "station": {
        "name_pt": "Estações",
        "name_en": "Stations",
        "name_es": "Estaciones",
        "description_pt": (
            "Estações de superfície do GHCN-Daily, uma linha por estação, com "
            "coordenadas, altitude, país, estado e identificador da "
            "Organização Meteorológica Mundial."
        ),
        "description_en": (
            "GHCN-Daily surface stations, one row per station, with "
            "coordinates, elevation, country, state and World Meteorological "
            "Organization identifier."
        ),
        "description_es": (
            "Estaciones de superficie del GHCN-Daily, una fila por estación, "
            "con coordenadas, altitud, país, estado e identificador de la "
            "Organización Meteorológica Mundial."
        ),
        "observation_levels": [("station", "station_id")],
    },
    "station_element_inventory": {
        "name_pt": "Inventário de estação e elemento",
        "name_en": "Station and element inventory",
        "name_es": "Inventario de estación y elemento",
        "description_pt": (
            "Cobertura por estação e elemento, indicando o primeiro e o último "
            "ano com dados não sinalizados. Permite descobrir o que uma "
            "estação mediu sem varrer a tabela de observações."
        ),
        "description_en": (
            "Coverage by station and element, giving the first and last year "
            "with unflagged data. It says what a station measured without "
            "scanning the observation table."
        ),
        "description_es": (
            "Cobertura por estación y elemento, indicando el primer y el "
            "último año con datos no señalados. Permite descubrir qué midió "
            "una estación sin recorrer la tabla de observaciones."
        ),
        "observation_levels": [
            ("station", "station_id"),
            ("other", "element"),
        ],
    },
    "observation": {
        "name_pt": "Observações",
        "name_en": "Observations",
        "name_es": "Observaciones",
        "description_pt": (
            "Observações diárias por estação e elemento, em formato longo, de "
            "1763 em diante. Cobre os 144 elementos do GHCN-Daily, dos quais "
            "os cinco principais (TMAX, TMIN, PRCP, SNOW, SNWD) concentram "
            "84,6% das linhas. Os valores já estão convertidos para unidades "
            "padrão, indicadas por linha em measurement_unit; em 28 elementos "
            "o valor não é uma grandeza mensurável e measurement_unit é nulo. "
            "Observações que reprovaram no controle de qualidade foram "
            "mantidas: filtre por quality_flag IS NULL para usar apenas dados "
            "aprovados."
        ),
        "description_en": (
            "Daily observations by station and element, in long format, from "
            "1763 onward. Covers all 144 GHCN-Daily elements, of which the "
            "five core ones (TMAX, TMIN, PRCP, SNOW, SNWD) account for 84.6% "
            "of rows. Values are already converted to standard units, given "
            "per row in measurement_unit; for 28 elements the value is not a "
            "measurable quantity and measurement_unit is null. Observations "
            "that failed quality assurance were kept: filter on quality_flag "
            "IS NULL to use only data that passed."
        ),
        "description_es": (
            "Observaciones diarias por estación y elemento, en formato largo, "
            "desde 1763. Cubre los 144 elementos del GHCN-Daily, de los cuales "
            "los cinco principales (TMAX, TMIN, PRCP, SNOW, SNWD) concentran "
            "el 84,6% de las filas. Los valores ya están convertidos a "
            "unidades estándar, indicadas por fila en measurement_unit; en 28 "
            "elementos el valor no es una cantidad medible y measurement_unit "
            "es nulo. Las observaciones que reprobaron el control de calidad "
            "se conservaron: filtre por quality_flag IS NULL para usar solo "
            "datos aprobados."
        ),
        "observation_levels": [
            ("station", "station_id"),
            ("date", "date"),
            ("other", "element"),
        ],
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Dicionário de códigos das colunas categóricas das demais tabelas: "
            "elementos meteorológicos, sinalizadores de medição, qualidade e "
            "fonte, e códigos de rede das estações."
        ),
        "description_en": (
            "Dictionary of codes for the categorical columns of the other "
            "tables: meteorological elements, measurement, quality and source "
            "flags, and station network codes."
        ),
        "description_es": (
            "Diccionario de códigos de las columnas categóricas de las demás "
            "tablas: elementos meteorológicos, indicadores de medición, "
            "calidad y fuente, y códigos de red de las estaciones."
        ),
        "observation_levels": [],
    },
}

TABLE_ORDER = [
    "observation",
    "station",
    "station_element_inventory",
    "dicionario",
]

# Per-table documentation bundles, built by build_auxiliary_files.py.
#
# These sit on basedosdados-dev because local credentials are dev-only. The
# bucket is requester-pays, so every one of these URLs returns HTTP 400
# (UserProjectMissing) to an anonymous visitor -- verified 2026-09-09, and the
# same is true of every prod table already using this field. The fix is one
# bucket setting, not a bespoke hosting decision per dataset; see
# .claude/rules/auxiliary-files.md. `dicionario` gets no bundle: it is itself
# the decoded form of the documentation.
AUXILIARY_FILES_URL = {
    t: (
        "https://storage.googleapis.com/basedosdados-dev/auxiliary_files/"
        f"world_noaa_ghcn/{t}/auxiliary_files.zip"
    )
    for t in ("observation", "station", "station_element_inventory")
}

RAW_SOURCES = [
    {
        "name_pt": "GHCN-Daily: arquivos anuais e metadados de estações",
        "name_en": "GHCN-Daily: yearly files and station metadata",
        "name_es": "GHCN-Daily: archivos anuales y metadatos de estaciones",
        "description_pt": (
            "Diretório público do NCEI com um arquivo CSV comprimido por ano "
            "(by_year) e os arquivos de metadados ghcnd-stations.txt, "
            "ghcnd-inventory.txt, ghcnd-countries.txt e ghcnd-states.txt. O "
            "acervo completo é reconstruído semanalmente."
        ),
        "description_en": (
            "Public NCEI directory holding one compressed CSV per year "
            "(by_year) plus the metadata files ghcnd-stations.txt, "
            "ghcnd-inventory.txt, ghcnd-countries.txt and ghcnd-states.txt. "
            "The whole archive is reconstructed weekly."
        ),
        "description_es": (
            "Directorio público del NCEI con un archivo CSV comprimido por año "
            "(by_year) y los archivos de metadatos ghcnd-stations.txt, "
            "ghcnd-inventory.txt, ghcnd-countries.txt y ghcnd-states.txt. El "
            "acervo completo se reconstruye semanalmente."
        ),
        "url": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/",
    }
]


def read_arch(table: str) -> list[dict]:
    with open(ARCH / f"{table}.csv", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def columns_json(table: str) -> str:
    cols = []
    for i, a in enumerate(read_arch(table)):
        cols.append(
            {
                "name": a["name"],
                "bigquery_type": a["bigquery_type"],
                "description": a["description"],
                "description_en": a["description_en"],
                "description_es": a["description_es"],
                "covered_by_dictionary": a["covered_by_dictionary"],
                "measurement_unit": a["measurement_unit"],
                "has_sensitive_data": a["has_sensitive_data"],
                "observations": a["observations"],
                "order": i,
            }
        )
    return json.dumps(cols, ensure_ascii=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging", choices=["staging", "prod"])
    args = ap.parse_args()
    env = args.env

    gcp_project = "basedosdados-dev" if env == "staging" else "basedosdados"
    ids = resolve(env)
    account = server.get_authenticated_account(env=env)
    account_id = account["id"] if isinstance(account, dict) else account
    print("authenticated as", account_id)

    status = server.discover_ids(env=env, keys=["status"])["status"]
    existing = server.get_dataset(DATASET_SLUG, env=env)

    ds_id = server.create_update_dataset(
        id=existing.get("id") if existing.get("found") else None,
        slug=DATASET_SLUG,
        organization_ids=[ids["organization"]],
        theme_ids=[ids["theme"]],
        tag_ids=ids["tags"],
        status_id=status["under_review"],
        env=env,
        **DATASET,
    )
    ds_id = ds_id["id"] if isinstance(ds_id, dict) else ds_id
    print("dataset", ds_id)

    prev_raw = {
        r.get("url"): r.get("id")
        for r in server.get_raw_data_sources(DATASET_SLUG, env=env)
    }
    raw_ids = []
    for rs in RAW_SOURCES:
        r = server.create_update_raw_data_source(
            id=prev_raw.get(rs["url"]),
            dataset_id=ds_id,
            license_id=ids["license"],
            availability_id=ids["availability"],
            env=env,
            **rs,
        )
        raw_ids.append(r["id"] if isinstance(r, dict) else r)
    print("raw data sources", raw_ids)

    existing = server.get_dataset(DATASET_SLUG, env=env)
    for table in TABLE_ORDER:
        spec = TABLES[table]
        prev = existing.get("tables", {}).get(table, {})
        t = server.create_update_table(
            id=prev.get("id"),
            slug=table,
            dataset_id=ds_id,
            status_id=status["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            auxiliary_files_url=AUXILIARY_FILES_URL.get(table),
            env=env,
            **{
                k: v
                for k, v in spec.items()
                if k.startswith(("name_", "description_"))
            },
        )
        tid = t["id"] if isinstance(t, dict) else t
        print(f"  table {table} -> {tid}")

        server.bulk_upsert_columns(
            table_id=tid, columns_json=columns_json(table), env=env
        )

        ol_ids = {}
        prev_ols = {
            o.get("entity_id"): o.get("id")
            for o in prev.get("observation_levels", [])
        }
        for entity_key, col in spec["observation_levels"]:
            entity_id = ids[f"entity_{entity_key}"]
            o = server.create_update_observation_level(
                id=prev_ols.get(entity_id),
                table_id=tid,
                entity_id=entity_id,
                env=env,
            )
            ol_ids[col] = o["id"] if isinstance(o, dict) else o

        # Link each grain column to its observation level, or the site renders
        # the level's columns as "Não informado". update_column's booleans
        # default to False, so is_partition must be re-passed here.
        cols = {
            c["name"]: c["id"]
            for c in server.get_dataset(DATASET_SLUG, env=env)["tables"][
                table
            ]["columns"]
        }
        for col, oid in ol_ids.items():
            server.update_column(
                column_id=cols[col],
                column_name=col,
                table_id=tid,
                observation_level_id=oid,
                is_partition=(col == "year"),
                env=env,
            )
        if table == "observation":
            server.update_column(
                column_id=cols["year"],
                column_name="year",
                table_id=tid,
                is_partition=True,
                env=env,
            )

        server.create_update_cloud_table(
            id=(prev.get("cloud_tables") or [{}])[0].get("id"),
            table_id=tid,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            env=env,
        )

        cov = server.create_update_coverage(
            id=(prev.get("coverages") or [{}])[0].get("id"),
            table_id=tid,
            area_id=ids["area"],
            env=env,
        )
        cov_id = cov["id"] if isinstance(cov, dict) else cov
        if table in ("observation", "station_element_inventory"):
            ranges = (
                prev.get("coverages", [{}])[0].get("datetime_ranges")
                if prev.get("coverages")
                else None
            )
            server.create_update_datetime_range(
                id=(ranges or [{}])[0].get("id") if ranges else None,
                coverage_id=cov_id,
                start_year=1763,
                end_year=2026,
                interval=1,
                env=env,
            )

        server.create_update_update(
            id=(prev.get("updates") or [{}])[0].get("id"),
            table_id=tid,
            # NCEI rewrites the current year daily and reconstructs the whole
            # archive weekly, so the table refreshes daily once the recurring
            # pipeline is armed. `latest` is when WE last refreshed -- a wall
            # clock, not a coverage date.
            entity_id=ids["entity_day"],
            frequency=1,
            latest=datetime.now(UTC).isoformat(),
            env=env,
        )
        server.create_update_table(
            id=tid,
            slug=table,
            dataset_id=ds_id,
            status_id=status["published"],
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            auxiliary_files_url=AUXILIARY_FILES_URL.get(table),
            raw_data_source_ids=raw_ids,
            env=env,
            **{
                k: v
                for k, v in spec.items()
                if k.startswith(("name_", "description_"))
            },
        )

    server.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print("done.")


if __name__ == "__main__":
    main()
