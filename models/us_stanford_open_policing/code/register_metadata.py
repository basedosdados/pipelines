"""Register us_stanford_open_policing (backend slug `open_policing`) metadata.

Idempotent: every child record (observation level, cloud table, coverage,
datetime range, update) is read over GraphQL first and its id passed back,
because create_update_* duplicates children when called without one.

The source is frozen (last release 2023-01-26, no further updates), so there is
no raw-source Update or Poll and no BD Pro window: both tables are free. The
dataset is registered `under_review`; `--publish` flips it to `published`
(dev/staging now; prod only after merge, table-approve and verification).

Columns, types and translations come from architecture.py and
observations_i18n.py. Coverage dates come from the clean.py manifests.

Usage (from the repo root):
    PYTHONPATH=. ~/.venvs/databasis-mcp/bin/python \
        models/us_stanford_open_policing/code/register_metadata.py \
        [--env dev|staging|prod] [--dry-run] [--publish]
"""

import argparse
import datetime as dt
import json
from typing import Any

import databasis_mcp.tools.metadata as bd_meta
import databasis_mcp.tools.write as bd_write

from models.us_stanford_open_policing.code import architecture as arch
from models.us_stanford_open_policing.code import constants
from models.us_stanford_open_policing.code.observations_i18n import translate

DATASET_SLUG = "open_policing"
GCP_DATASET_ID = constants.DATASET_ID
AREA_SLUG = "us"
LICENSE_SLUG = "odc_by"
ORG_SLUG = "stanford"
THEME_SLUGS = ["safety", "justice"]
TAG_SLUGS = ["police", "police-conduct", "traffic", "race", "vehicle", "crime"]
# Staging is an older snapshot whose tag vocabulary is Portuguese. It has no
# tag for race, so that one is left off there rather than created.
STAGING_TAG_SLUGS = {
    "police": "policia",
    "police-conduct": "conduta_policial",
    "traffic": "transito",
    "race": None,
    "vehicle": "veiculo",
    "crime": "crime",
}

DATASET = {
    "name_pt": "Stanford Open Policing Project: abordagens policiais nos EUA",
    "name_en": "Stanford Open Policing Project: US Police Stops",
    "name_es": "Stanford Open Policing Project: detenciones policiales en EE. UU.",
    "description_pt": (
        "O Stanford Open Policing Project, do Stanford Computational Policy Lab, "
        "coletou registros de abordagens policiais de polícias rodoviárias estaduais "
        "e departamentos de polícia municipais dos Estados Unidos e os padronizou em "
        "um esquema comum: data, local, características da pessoa abordada e do "
        "policial, motivo, busca, contrabando e desfecho. Este conjunto reúne os "
        "{rows} de abordagens dos {n} arquivos publicados ({cities} cidades e "
        "{states} polícias estaduais, {start}-{end}). Cada jurisdição preenche um "
        "subconjunto diferente das colunas. Os dados estão congelados desde a "
        "publicação de 2023 e são distribuídos sob a licença ODC-BY; cite Pierson "
        "et al. (2020), Nature Human Behaviour."
    ),
    "description_en": (
        "The Stanford Open Policing Project, run by the Stanford Computational Policy "
        "Lab, collected police stop records from US state patrols and city police "
        "departments and standardized them into a common schema: date, location, "
        "characteristics of the stopped person and the officer, reason, search, "
        "contraband and outcome. This dataset holds the {rows_en} stops in the {n} "
        "published files ({cities} cities and {states} state patrols, {start}-{end}). "
        "Each jurisdiction fills a different subset of the columns. The data have "
        "been frozen since the 2023 release and are distributed under the ODC-BY "
        "licence; cite Pierson et al. (2020), Nature Human Behaviour."
    ),
    "description_es": (
        "El Stanford Open Policing Project, del Stanford Computational Policy Lab, "
        "recopiló registros de detenciones policiales de patrullas estatales y "
        "departamentos de policía municipales de Estados Unidos y los estandarizó en "
        "un esquema común: fecha, lugar, características de la persona detenida y del "
        "policía, motivo, registro, contrabando y resultado. Este conjunto reúne las "
        "{rows} de detenciones de los {n} archivos publicados ({cities} ciudades y "
        "{states} patrullas estatales, {start}-{end}). Cada jurisdicción completa un "
        "subconjunto distinto de las columnas. Los datos están congelados desde la "
        "publicación de 2023 y se distribuyen bajo la licencia ODC-BY; cite Pierson "
        "et al. (2020), Nature Human Behaviour."
    ),
}

RAW_SOURCE = {
    "name_pt": "Stanford Open Policing Project: dados",
    "name_en": "Stanford Open Policing Project: data",
    "name_es": "Stanford Open Policing Project: datos",
    "url": "https://openpolicing.stanford.edu/data/",
    "description_pt": (
        "Página de dados do Stanford Open Policing Project, com um arquivo CSV "
        "compactado (e um RDS) por jurisdição, shapefiles para algumas cidades e um "
        "README com a cobertura de cada coluna por jurisdição. Hospedado no Stanford "
        "Digital Repository. Licença Open Data Commons Attribution (ODC-BY)."
    ),
    "description_en": (
        "Data page of the Stanford Open Policing Project, with one zipped CSV (and "
        "one RDS) per jurisdiction, shapefiles for some cities, and a README giving "
        "each column's coverage by jurisdiction. Hosted on the Stanford Digital "
        "Repository. Open Data Commons Attribution License (ODC-BY)."
    ),
    "description_es": (
        "Página de datos del Stanford Open Policing Project, con un CSV comprimido "
        "(y un RDS) por jurisdicción, shapefiles para algunas ciudades y un README "
        "con la cobertura de cada columna por jurisdicción. Alojado en el Stanford "
        "Digital Repository. Licencia Open Data Commons Attribution (ODC-BY)."
    ),
}

TABLE_ORDER = ["stop", "jurisdiction"]
TABLES = {
    "stop": {
        "name_pt": "Abordagens",
        "name_en": "Stops",
        "name_es": "Detenciones",
        "description_pt": (
            "Abordagens policiais registradas por polícias rodoviárias estaduais e "
            "departamentos de polícia municipais dos EUA, padronizadas pelo Stanford "
            "Open Policing Project, uma linha por abordagem. Cada jurisdição "
            "preenche um subconjunto diferente das colunas; as colunas raw_ "
            "específicas de cada local não foram incluídas."
        ),
        "description_en": (
            "Police stops recorded by US state patrols and city police departments, "
            "standardized by the Stanford Open Policing Project, one row per stop. "
            "Each jurisdiction fills a different subset of the columns; the "
            "location-specific raw_ columns are not included."
        ),
        "description_es": (
            "Detenciones policiales registradas por patrullas estatales y "
            "departamentos de policía municipales de EE. UU., estandarizadas por el "
            "Stanford Open Policing Project, una fila por detención. Cada "
            "jurisdicción completa un subconjunto distinto de las columnas; no se "
            "incluyeron las columnas raw_ específicas de cada lugar."
        ),
        "levels": {
            "year": "year",
            "state": "state_id",
            "agency": "jurisdiction_id",
            "record": "raw_row_number",
        },
        "dated": True,
    },
    "jurisdiction": {
        "name_pt": "Jurisdições",
        "name_en": "Jurisdictions",
        "name_es": "Jurisdicciones",
        "description_pt": (
            "Jurisdições cobertas pelo Stanford Open Policing Project, uma linha por "
            "arquivo de origem: uma polícia rodoviária estadual ou um departamento "
            "de polícia municipal, com período coberto e número de abordagens."
        ),
        "description_en": (
            "Jurisdictions covered by the Stanford Open Policing Project, one row per "
            "source file: a state patrol or a city police department, with the "
            "period covered and the number of stops."
        ),
        "description_es": (
            "Jurisdicciones cubiertas por el Stanford Open Policing Project, una fila "
            "por archivo de origen: una patrulla estatal o un departamento de "
            "policía municipal, con el período cubierto y el número de detenciones."
        ),
        "levels": {"agency": "jurisdiction_id", "state": "state_id"},
        "dated": False,
    },
}
PARTITIONS = {"stop": ["year"]}

CHILDREN_QUERY = """
query($id: ID) { allTable(id: $id) { edges { node {
  coverages { edges { node { id isClosed
    datetimeRanges { edges { node { id } } } } } }
  observationLevels { edges { node { id entity { slug } } } }
  cloudTables { edges { node { id } } }
  updates { edges { node { id } } }
} } } }
"""


def columns(table: str) -> list[dict]:
    out = []
    for name, typ, unit, sens, _orig, en, pt, es, obs in arch.TABLES[table]:
        o_pt, o_es = translate(obs)
        out.append(
            {
                "name": name,
                "bigquery_type": typ,
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
                "observations_pt": o_pt,
                "observations_en": obs,
                "observations_es": o_es,
                "covered_by_dictionary": False,
                "directory_column": arch.DIRECTORY.get(name, ""),
                "measurement_unit": unit,
                "has_sensitive_data": sens == "yes",
            }
        )
    return out


def coverage_facts() -> dict:
    """Totals and date range across every cleaned jurisdiction."""
    manifests = [
        json.loads(p.read_text())
        for p in sorted((constants.OUTPUT_DIR / "_manifest").glob("*.json"))
    ]
    if len(manifests) != len(constants.JURISDICTIONS):
        raise RuntimeError(f"only {len(manifests)} jurisdictions cleaned")
    statewide = sum(
        1
        for name, _ in constants.JURISDICTIONS.values()
        if name == "Statewide"
    )
    return {
        "rows": sum(m["rows"] for m in manifests),
        "start": min(m["start_date"] for m in manifests),
        "end": max(m["end_date"] for m in manifests),
        "n": len(manifests),
        "states": statewide,
        "cities": len(manifests) - statewide,
    }


def edges(node: dict, key: str) -> list[dict]:
    return [e["node"] for e in node[key]["edges"]]


def children(table_id: str, env: str) -> dict:
    node = bd_meta._gql(CHILDREN_QUERY, {"id": table_id}, env=env)["allTable"][
        "edges"
    ][0]["node"]
    sid = bd_meta._strip_id
    covs = {}
    for c in edges(node, "coverages"):
        tier = "pro" if c["isClosed"] else "free"
        if tier in covs:
            raise RuntimeError(
                f"table {table_id}: two {tier} coverages, fix by hand"
            )
        covs[tier] = {
            "id": sid(c["id"]),
            "ranges": [sid(r["id"]) for r in edges(c, "datetimeRanges")],
        }
    return {
        "coverages": covs,
        "ols": {
            o["entity"]["slug"]: sid(o["id"])
            for o in edges(node, "observationLevels")
        },
        "cloud_tables": [sid(c["id"]) for c in edges(node, "cloudTables")],
        "updates": [sid(u["id"]) for u in edges(node, "updates")],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--publish", action="store_true")
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    facts = coverage_facts()
    millions = facts["rows"] / 1e6
    fmt = {
        "rows": f"{millions:.1f} milhões".replace(".", ","),
        "rows_en": f"{millions:.1f} million",
        "n": facts["n"],
        "cities": facts["cities"],
        "states": facts["states"],
        "start": facts["start"][:4],
        "end": facts["end"][:4],
    }
    dataset = {k: v.format(**fmt) for k, v in DATASET.items()}
    dataset["description_es"] = dataset["description_es"].replace(
        fmt["rows"], f"{millions:.1f} millones".replace(".", ",")
    )
    payloads = {t: columns(t) for t in TABLE_ORDER}

    ids = bd_meta.discover_ids(
        env=env,
        keys=[
            "status",
            "entity",
            "license",
            "availability",
            "theme",
            "language",
        ],
    )
    status_review, status_published = (
        ids["status"]["under_review"],
        ids["status"]["published"],
    )
    theme_ids = [ids["theme"][t] for t in THEME_SLUGS]
    tag_slugs = [
        STAGING_TAG_SLUGS[t] if env == "staging" else t for t in TAG_SLUGS
    ]
    tag_ids = [
        bd_meta.lookup_id(category="tag", slug=s, env=env)["id"]
        for s in tag_slugs
        if s
    ]
    area_id = bd_meta.lookup_id(category="area", slug=AREA_SLUG, env=env)["id"]
    org_id = bd_meta.lookup_id(
        category="organization", slug=ORG_SLUG, env=env
    )["id"]
    english = ids["language"]["en"]
    entity = ids["entity"]
    account_id = bd_meta.get_authenticated_account(env=env)["id"]
    print(
        f"env={env} org={org_id} themes={len(theme_ids)} tags={len(tag_ids)} "
        f"facts={facts} columns={ {t: len(p) for t, p in payloads.items()} }"
    )
    print(dataset["description_en"])
    if args.dry_run:
        return

    existing = bd_meta.get_dataset(slug=DATASET_SLUG, env=env)
    ds = bd_write.create_update_dataset(
        slug=DATASET_SLUG,
        **dataset,
        organization_ids=[org_id],
        theme_ids=theme_ids,
        tag_ids=tag_ids,
        status_id=status_published if args.publish else status_review,
        id=existing.get("id") if existing.get("found") else None,
        env=env,
    )
    dataset_id = ds["id"]
    print(f"dataset {DATASET_SLUG} -> {dataset_id}")

    prior_sources = {
        s["url"]: s["id"]
        for s in bd_write.get_raw_data_sources(
            dataset_slug=DATASET_SLUG, env=env
        )
        if s.get("url")
    }
    source_id = bd_write.create_update_raw_data_source(
        dataset_id=dataset_id,
        **RAW_SOURCE,
        license_id=ids["license"][LICENSE_SLUG],
        availability_id=ids["availability"]["online"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        language_ids=[english],
        status_id=status_published,
        id=prior_sources.get(RAW_SOURCE["url"]),
        env=env,
    )["id"]
    print(f"raw source -> {source_id}")

    # A table Update is a wall clock; a naive value would be read as Sao Paulo time.
    now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    start = dt.date.fromisoformat(facts["start"])
    end = dt.date.fromisoformat(facts["end"])
    for table in TABLE_ORDER:
        spec = TABLES[table]
        prior = (
            bd_meta.get_dataset(slug=DATASET_SLUG, env=env)
            .get("tables", {})
            .get(table, {})
        )
        common: dict[str, Any] = {
            "slug": table,
            **{
                k: spec[k]
                for k in (
                    "name_pt",
                    "name_en",
                    "name_es",
                    "description_pt",
                    "description_en",
                    "description_es",
                )
            },
            "dataset_id": dataset_id,
            "status_id": status_published,
            "published_by_ids": [account_id],
            "data_cleaned_by_ids": [account_id],
            "env": env,
        }
        table_id = bd_write.create_update_table(**common, id=prior.get("id"))[
            "id"
        ]
        kids = children(table_id, env)
        print(f"\ntable {table} -> {table_id}")

        ol_ids = {
            ent: bd_write.create_update_observation_level(
                table_id=table_id,
                entity_id=entity[ent],
                id=kids["ols"].get(ent),
                env=env,
            )["id"]
            for ent in spec["levels"]
        }
        bd_write.reorder_observation_levels(
            table_id=table_id,
            ol_ids=[ol_ids[e] for e in spec["levels"]],
            env=env,
        )

        res = bd_write.bulk_upsert_columns(
            table_id=table_id,
            columns_json=json.dumps(payloads[table], ensure_ascii=False),
            env=env,
        )
        print(
            f"  columns: created={res['created']} updated={res['updated']} errors={res['errors']}"
        )
        if res["errors"]:
            raise RuntimeError(
                f"{table}: column upsert errors {res['errors']}"
            )
        bd_write.reorder_columns(
            table_id=table_id,
            column_names=[c["name"] for c in payloads[table]],
            env=env,
        )

        # Observation-level links and partition flags in one call per column:
        # update_column's booleans default to False, so separate calls clobber.
        cols = {
            c["name"]: c["id"]
            for c in bd_meta.get_dataset(slug=DATASET_SLUG, env=env)["tables"][
                table
            ]["columns"]
        }
        level_of = {col: ent for ent, col in spec["levels"].items()}
        parts = PARTITIONS.get(table, [])
        by_name = {c["name"]: c for c in payloads[table]}
        for col in sorted(set(level_of) | set(parts)):
            c = by_name[col]
            bd_write.update_column(
                column_id=cols[col],
                column_name=col,
                table_id=table_id,
                is_partition=col in parts,
                covered_by_dictionary=c["covered_by_dictionary"],
                has_sensitive_data=c["has_sensitive_data"],
                measurement_unit=c["measurement_unit"],
                directory_column_name=c["directory_column"],
                description_pt=c["description_pt"],
                description_en=c["description_en"],
                description_es=c["description_es"],
                observations_pt=c["observations_pt"],
                observations_en=c["observations_en"],
                observations_es=c["observations_es"],
                observation_level_id=ol_ids.get(level_of.get(col)),
                env=env,
            )
        print(f"  flagged {sorted(set(level_of) | set(parts))}")

        bd_write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=kids["cloud_tables"][0] if kids["cloud_tables"] else None,
            env=env,
        )

        free = kids["coverages"].get("free", {})
        cov = bd_write.create_update_coverage(
            table_id=table_id,
            area_id=area_id,
            is_closed=False,
            id=free.get("id"),
            env=env,
        )
        if spec["dated"]:
            ranges = free.get("ranges", [])
            bd_write.create_update_datetime_range(
                coverage_id=cov["id"],
                start_year=start.year, start_month=start.month, start_day=start.day,
                end_year=end.year, end_month=end.month, end_day=end.day,
                interval=1, is_closed=False,
                id=ranges[0] if ranges else None, env=env,
            )  # fmt: skip
            print(f"  free coverage {start}..{end}")

        bd_write.create_update_update(
            entity_id=entity["year"],
            frequency=1,
            latest=now,
            table_id=table_id,
            id=kids["updates"][0] if kids["updates"] else None,
            env=env,
        )
        bd_write.create_update_table(
            **common, raw_data_source_ids=[source_id], id=table_id
        )
        print("  cloud table, coverage, update and raw source linked")

    bd_write.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
