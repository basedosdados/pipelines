"""Register us_census_lodes metadata in the Data Basis backend.

    ~/.venvs/bd-pipelines/bin/python models/us_census_lodes/code/register_metadata.py --env staging
    ~/.venvs/bd-pipelines/bin/python models/us_census_lodes/code/register_metadata.py --env prod

Columns come from ``code/architecture/*.csv`` so the backend, the dbt models and
the parquet schema all derive from one source. The script calls the databasis MCP
server's tool functions directly rather than through the MCP interface, which
avoids pushing ~40 KB of column JSON per table through a conversation.

Idempotency: `create_update_*` is NOT idempotent without an id — re-running
without passing the existing id creates duplicate observation levels, cloud
tables, coverages and updates. This script therefore reads the dataset back
first and reuses every id it finds.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
from pathlib import Path

# The databasis MCP server is a sibling repo, not a package dependency. Its
# tool functions are plain callables, so importing the module directly avoids
# pushing ~40 KB of column JSON per table through the MCP interface.
MCP_REPO = os.environ.get(
    "DATABASIS_MCP_REPO",
    str(Path.home() / "Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)
if not (Path(MCP_REPO) / "server.py").exists():
    raise SystemExit(
        f"databasis MCP server not found at {MCP_REPO}. "
        "Set DATABASIS_MCP_REPO to the checkout of basedosdados/mcp."
    )
sys.path.insert(0, MCP_REPO)
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

# pyrefly: ignore [missing-import]
import server  # noqa: E402

from pipelines.datasets.us_census_lodes.constants import YEARS  # noqa: E402
from pipelines.datasets.us_census_lodes.utils import read_arch  # noqa: E402

# The backend's Update.latest is a DateTime, not a Date -- a bare
# "YYYY-MM-DD" is rejected with "DateTime cannot represent value".
TODAY = datetime.date.today().isoformat() + "T00:00:00+00:00"

SLUG = "lodes"
GCP_DATASET_ID = "us_census_lodes"
TABLES = [
    "residence_jobs",
    "workplace_jobs",
    "geography_crosswalk",
    "dicionario",
]

# Reference ids differ per backend, so they are resolved at run time.
REF = {
    "organization": "us_census",
    "themes": ["economics", "population", "urbanization"],
    # Content tags only. Area names, theme synonyms and the organization name
    # are already structured metadata and must not be duplicated as tags.
    "tags": ["emprego", "trabalho", "salario", "mobilidade", "demografia"],
    "license": "cc0",
    "availability": "online",
    "area": "us",
}

DATASET_TEXT = {
    "name_pt": "LEHD Origin-Destination Employment Statistics (LODES)",
    "name_en": "LEHD Origin-Destination Employment Statistics (LODES)",
    "name_es": "LEHD Origin-Destination Employment Statistics (LODES)",
    "description_pt": (
        "Contagens de empregos por bloco censitário de 2020 nos Estados Unidos, "
        "produzidas pelo programa Longitudinal Employer-Household Dynamics (LEHD) do "
        "Census Bureau a partir de registros administrativos de seguro-desemprego e da "
        "folha de pagamento federal. Cobre 50 estados, o Distrito de Columbia e Porto "
        "Rico entre 2002 e 2023, com desagregações por idade, faixa de rendimento, "
        "setor NAICS, raça, etnia, escolaridade, sexo e características da firma, tanto "
        "pelo bloco de residência quanto pelo bloco de trabalho do trabalhador. Toda a "
        "série é enumerada na malha de blocos censitários de 2020."
    ),
    "description_en": (
        "Job counts by 2020 census block for the United States, produced by the Census "
        "Bureau's Longitudinal Employer-Household Dynamics (LEHD) program from "
        "unemployment insurance administrative records and federal payroll data. Covers "
        "50 states, the District of Columbia and Puerto Rico from 2002 to 2023, with "
        "breakdowns by age, earnings band, NAICS sector, race, ethnicity, educational "
        "attainment, sex and firm characteristics, tabulated by the worker's residence "
        "block and by the workplace block. The whole series is enumerated on 2020 "
        "census blocks."
    ),
    "description_es": (
        "Recuentos de empleos por bloque censal de 2020 en los Estados Unidos, "
        "producidos por el programa Longitudinal Employer-Household Dynamics (LEHD) de "
        "la Oficina del Censo a partir de registros administrativos del seguro de "
        "desempleo y de la nómina federal. Abarca 50 estados, el Distrito de Columbia y "
        "Puerto Rico entre 2002 y 2023, con desagregaciones por edad, rango de ingresos, "
        "sector NAICS, raza, etnia, nivel educativo, sexo y características de la "
        "empresa, tanto por el bloque de residencia como por el bloque de trabajo. Toda "
        "la serie está enumerada en la malla de bloques censales de 2020."
    ),
}

TABLE_TEXT = {
    "residence_jobs": {
        "name_pt": "Empregos por bloco de residência",
        "name_en": "Jobs by residence block",
        "name_es": "Empleos por bloque de residencia",
        "description_pt": (
            "Número de empregos por bloco censitário de residência do trabalhador, ano "
            "e tipo de vínculo, com desagregações por idade, faixa de rendimento, setor "
            "NAICS, raça, etnia, escolaridade e sexo. Corresponde aos arquivos Residence "
            "Area Characteristics (RAC) do LODES 8, segmento S000 (todos os "
            "trabalhadores). Os dados são enumerados em blocos censitários de 2020 e "
            "toda a série histórica foi reprocessada para essa malha, portanto códigos "
            "de bloco de versões anteriores do LODES não são comparáveis sem os arquivos "
            "de relacionamento do Census Bureau."
        ),
        "description_en": (
            "Number of jobs by the worker's residence census block, year and job type, "
            "with breakdowns by age, earnings band, NAICS sector, race, ethnicity, "
            "educational attainment and sex. Corresponds to the LODES 8 Residence Area "
            "Characteristics (RAC) files, segment S000 (all workers). The data are "
            "enumerated on 2020 census blocks and the whole history has been reprocessed "
            "onto that geography, so block codes from earlier LODES versions are not "
            "comparable without the Census Bureau's relationship files."
        ),
        "description_es": (
            "Número de empleos por bloque censal de residencia del trabajador, año y "
            "tipo de empleo, con desagregaciones por edad, rango de ingresos, sector "
            "NAICS, raza, etnia, nivel educativo y sexo. Corresponde a los archivos "
            "Residence Area Characteristics (RAC) de LODES 8, segmento S000 (todos los "
            "trabajadores). Los datos están enumerados en bloques censales de 2020 y "
            "toda la serie histórica fue reprocesada a esa malla, por lo que los códigos "
            "de bloque de versiones anteriores de LODES no son comparables sin los "
            "archivos de relación de la Oficina del Censo."
        ),
        "entity": "census_block",
    },
    "workplace_jobs": {
        "name_pt": "Empregos por bloco de trabalho",
        "name_en": "Jobs by workplace block",
        "name_es": "Empleos por bloque de trabajo",
        "description_pt": (
            "Número de empregos por bloco censitário do local de trabalho, ano e tipo de "
            "vínculo, com desagregações por idade, faixa de rendimento, setor NAICS, "
            "raça, etnia, escolaridade, sexo, idade da firma e porte da firma. "
            "Corresponde aos arquivos Workplace Area Characteristics (WAC) do LODES 8, "
            "segmento S000 (todos os trabalhadores). Os dados são enumerados em blocos "
            "censitários de 2020 e toda a série histórica foi reprocessada para essa "
            "malha, portanto códigos de bloco de versões anteriores do LODES não são "
            "comparáveis sem os arquivos de relacionamento do Census Bureau."
        ),
        "description_en": (
            "Number of jobs by workplace census block, year and job type, with "
            "breakdowns by age, earnings band, NAICS sector, race, ethnicity, "
            "educational attainment, sex, firm age and firm size. Corresponds to the "
            "LODES 8 Workplace Area Characteristics (WAC) files, segment S000 (all "
            "workers). The data are enumerated on 2020 census blocks and the whole "
            "history has been reprocessed onto that geography, so block codes from "
            "earlier LODES versions are not comparable without the Census Bureau's "
            "relationship files."
        ),
        "description_es": (
            "Número de empleos por bloque censal del lugar de trabajo, año y tipo de "
            "empleo, con desagregaciones por edad, rango de ingresos, sector NAICS, "
            "raza, etnia, nivel educativo, sexo, antigüedad de la empresa y tamaño de la "
            "empresa. Corresponde a los archivos Workplace Area Characteristics (WAC) de "
            "LODES 8, segmento S000 (todos los trabajadores). Los datos están enumerados "
            "en bloques censales de 2020 y toda la serie histórica fue reprocesada a esa "
            "malla, por lo que los códigos de bloque de versiones anteriores de LODES no "
            "son comparables sin los archivos de relación de la Oficina del Censo."
        ),
        "entity": "census_block",
    },
    "geography_crosswalk": {
        "name_pt": "Correspondência geográfica de blocos censitários",
        "name_en": "Census block geography crosswalk",
        "name_es": "Correspondencia geográfica de bloques censales",
        "description_pt": (
            "Relação entre cada bloco censitário de tabulação de 2020 e as demais "
            "unidades geográficas suportadas pelo aplicativo OnTheMap, incluindo "
            "condado, setor censitário, grupo de blocos, região metropolitana, ZCTA, "
            "lugar, distrito eleitoral e distrito escolar, além do ponto interno do "
            "bloco em latitude e longitude. Retrata a delimitação vigente na divulgação "
            "do LODES 8.4 e é substituída integralmente a cada nova versão."
        ),
        "description_en": (
            "Relationship between each 2020 census tabulation block and the other "
            "geographic entities supported by the OnTheMap application, including "
            "county, census tract, block group, metropolitan area, ZCTA, place, "
            "congressional district and school district, plus the block's internal point "
            "in latitude and longitude. Reflects the delineation current at the LODES "
            "8.4 release and is replaced wholesale at each new version."
        ),
        "description_es": (
            "Relación entre cada bloque censal de tabulación de 2020 y las demás "
            "unidades geográficas admitidas por la aplicación OnTheMap, incluidos "
            "condado, sector censal, grupo de bloques, área metropolitana, ZCTA, lugar, "
            "distrito electoral y distrito escolar, además del punto interno del bloque "
            "en latitud y longitud. Refleja la delimitación vigente en la publicación de "
            "LODES 8.4 y se sustituye por completo en cada nueva versión."
        ),
        "entity": "census_block",
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Dicionário de códigos das colunas codificadas do conjunto us_census_lodes"
        ),
        "description_en": (
            "Dictionary of codes for the coded columns of the us_census_lodes dataset"
        ),
        "description_es": (
            "Diccionario de códigos de las columnas codificadas del conjunto "
            "us_census_lodes"
        ),
        "entity": None,
    },
}

# One raw data source per table -- the client resolves a table's source through a
# single-node query that raises when a table has two or more.
RAW_SOURCES = {
    "residence_jobs": (
        "LODES 8 Residence Area Characteristics (RAC)",
        "https://lehd.ces.census.gov/data/lodes/LODES8/",
    ),
    "workplace_jobs": (
        "LODES 8 Workplace Area Characteristics (WAC)",
        "https://lehd.ces.census.gov/data/lodes/LODES8/",
    ),
    "geography_crosswalk": (
        "LODES 8 Geography Crosswalk",
        "https://lehd.ces.census.gov/data/lodes/LODES8/",
    ),
}

# Which column identifies each table's observation level.
OL_COLUMN = {
    "residence_jobs": "block_id",
    "workplace_jobs": "block_id",
    "geography_crosswalk": "block_id",
}


def resolve(env: str) -> dict:
    ids = {
        "organization": server.lookup_id(
            category="organization", slug=REF["organization"], env=env
        )["id"],
        "license": server.lookup_id(
            category="license", slug=REF["license"], env=env
        )["id"],
        "availability": server.lookup_id(
            category="availability", slug=REF["availability"], env=env
        )["id"],
        "area": server.lookup_id(category="area", slug=REF["area"], env=env)[
            "id"
        ],
        "published": server.lookup_id(
            category="status", slug="published", env=env
        )["id"],
        "under_review": server.lookup_id(
            category="status", slug="under_review", env=env
        )["id"],
        "year_entity": server.lookup_id(
            category="entity", slug="year", env=env
        )["id"],
        "census_block": server.lookup_id(
            category="entity", slug="census_block", env=env
        )["id"],
    }
    ids["themes"] = [
        server.lookup_id(category="theme", slug=s, env=env)["id"]
        for s in REF["themes"]
    ]
    ids["tags"] = [
        server.lookup_id(category="tag", slug=s, env=env)["id"]
        for s in REF["tags"]
    ]
    ids["account"] = server.get_authenticated_account(env=env)["id"]
    return ids


def columns_payload(table: str) -> list[dict]:
    out = []
    for a in read_arch(table):
        col = {
            "name": a["name"],
            "bigquery_type": a["bigquery_type"],
            "description_pt": a["description"],
            "description_en": a["description_en"],
            "description_es": a["description_es"],
            "covered_by_dictionary": a["covered_by_dictionary"] == "yes",
            "has_sensitive_data": a["has_sensitive_data"] == "yes",
        }
        if a["measurement_unit"]:
            col["measurement_unit"] = a["measurement_unit"]
        if a["directory_column"]:
            col["directory_column"] = a["directory_column"]
        if a["observations"]:
            # The architecture stores the three languages pipe-separated.
            parts = [p.strip() for p in a["observations"].split(" | ")]
            col["observations_pt"] = parts[0]
            col["observations_en"] = parts[1] if len(parts) > 1 else parts[0]
            col["observations_es"] = parts[2] if len(parts) > 2 else parts[0]
        out.append(col)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--env", default="staging", choices=["staging", "dev", "prod"]
    )
    ap.add_argument(
        "--publish",
        action="store_true",
        help="set the dataset status to published (staging pre-promotion, "
        "or prod only after the PR merged and prod tables verified)",
    )
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    server.auth(env=env)
    ids = resolve(env)
    existing = server.get_dataset(slug=SLUG, env=env)

    status = ids["published"] if args.publish else ids["under_review"]
    ds = server.create_update_dataset(
        id=existing.get("id") if existing.get("found") else None,
        slug=SLUG,
        organization_ids=[ids["organization"]],
        theme_ids=ids["themes"],
        tag_ids=ids["tags"],
        status_id=status,
        env=env,
        **DATASET_TEXT,
    )
    dataset_id = ds["id"]
    print(
        f"dataset {SLUG} -> {dataset_id} (status={'published' if args.publish else 'under_review'})"
    )

    # get_raw_data_sources returns a list of {id, name, url}; keying on the
    # name is what makes a re-run update the same record instead of creating a
    # second one (create_update_* is not idempotent without an id).
    prior_sources = {
        s["name"]: s["id"]
        for s in server.get_raw_data_sources(dataset_slug=SLUG, env=env)
    }
    source_ids = {}
    for table, (name, url) in RAW_SOURCES.items():
        r = server.create_update_raw_data_source(
            id=prior_sources.get(name),
            dataset_id=dataset_id,
            name_pt=name,
            name_en=name,
            name_es=name,
            url=url,
            license_id=ids["license"],
            availability_id=ids["availability"],
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            env=env,
        )
        source_ids[table] = r["id"]
        print(f"  raw source {name} -> {r['id']}")

    for table in TABLES:
        prior = (
            existing.get("tables", {}).get(table, {})
            if existing.get("found")
            else {}
        )
        t = server.create_update_table(
            id=prior.get("id"),
            slug=table,
            dataset_id=dataset_id,
            status_id=ids["published"],
            published_by_ids=[ids["account"]],
            data_cleaned_by_ids=[ids["account"]],
            env=env,
            **{k: v for k, v in TABLE_TEXT[table].items() if k != "entity"},
        )
        table_id = t["id"]
        print(f"table {table} -> {table_id}")

        # Observation levels: reuse the existing ids, since create_update_* is
        # not idempotent and would otherwise duplicate them on a re-run.
        prior_ols = {
            ol["entity_slug"]: ol["id"]
            for ol in prior.get("observation_levels", [])
        }
        ol_ids = {}
        entity = TABLE_TEXT[table]["entity"]
        if entity:
            r = server.create_update_observation_level(
                id=prior_ols.get(entity),
                table_id=table_id,
                entity_id=ids[entity],
                env=env,
            )
            ol_ids[entity] = r["id"]
        if table in ("residence_jobs", "workplace_jobs"):
            r = server.create_update_observation_level(
                id=prior_ols.get("year"),
                table_id=table_id,
                entity_id=ids["year_entity"],
                env=env,
            )
            ol_ids["year"] = r["id"]

        cols = columns_payload(table)
        res = server.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(cols, ensure_ascii=False),
            env=env,
        )
        print(f"  columns: {res}")

        # bulk_upsert_columns does not set is_partition, and update_column's
        # booleans default to False -- so the observation-level link and the
        # partition flag must be passed together or one clobbers the other.
        by_name = {
            c["name"]: c["id"]
            for c in server.get_dataset(slug=SLUG, env=env)["tables"][table][
                "columns"
            ]
        }
        if table in ("residence_jobs", "workplace_jobs"):
            server.update_column(
                column_id=by_name["year"],
                column_name="year",
                table_id=table_id,
                is_partition=True,
                observation_level_id=ol_ids["year"],
                env=env,
            )
        if table in OL_COLUMN and entity:
            name = OL_COLUMN[table]
            server.update_column(
                column_id=by_name[name],
                column_name=name,
                table_id=table_id,
                observation_level_id=ol_ids[entity],
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
        prior_range = (prior_cov.get("datetime_ranges") or [{}])[0].get("id")
        server.create_update_datetime_range(
            id=prior_range,
            coverage_id=cov["id"],
            start_year=YEARS[0],
            end_year=YEARS[-1],
            interval=1,
            env=env,
        )

        # Update records. Three exist for a recurring dataset and they mean
        # different things (see .claude/rules/metadata-schema.md):
        #   table Update           -> when *we* last refreshed (wall clock)
        #   raw data source Update -> what the *source* published (coverage date)
        #   raw data source Poll   -> when we last looked (written by the flow)
        # The Poll is only created by a flow run, so it is absent until then.
        prior_updates = {
            u["entity_slug"]: u["id"] for u in prior.get("updates", [])
        }
        server.create_update_update(
            id=prior_updates.get("year"),
            table_id=table_id,
            entity_id=ids["year_entity"],
            frequency=1,
            # LODES publishes data year N in about N+2 (2023 data in Dec 2025).
            lag=2,
            latest=TODAY,
            env=env,
        )

        if table in RAW_SOURCES:
            # The source Update's `latest` is the source's max COVERAGE date,
            # not today: putting a wall clock here would claim the Census
            # Bureau released data today. upsert_raw_source_update only creates
            # a (hardcoded month-entity) record when none exists, so seeding it
            # here with the year entity is what the pipeline then keeps current.
            src_updates = server._gql(
                "query($s:ID!){allUpdate(rawDataSource_Id:$s){edges{node{id}}}}",
                {"s": source_ids[table]},
                env=env,
            )["allUpdate"]["edges"]
            server.create_update_update(
                id=server._strip_id(src_updates[0]["node"]["id"])
                if src_updates
                else None,
                raw_data_source_id=source_ids[table],
                entity_id=ids["year_entity"],
                frequency=1,
                latest=f"{YEARS[-1]}-01-01T00:00:00+00:00",
                env=env,
            )

            server.create_update_table(
                id=table_id,
                slug=table,
                dataset_id=dataset_id,
                status_id=ids["published"],
                published_by_ids=[ids["account"]],
                data_cleaned_by_ids=[ids["account"]],
                raw_data_source_ids=[source_ids[table]],
                env=env,
                **{
                    k: v for k, v in TABLE_TEXT[table].items() if k != "entity"
                },
            )

    print("\ndone. Verify with get_dataset / GraphQL before promoting.")


if __name__ == "__main__":
    main()
