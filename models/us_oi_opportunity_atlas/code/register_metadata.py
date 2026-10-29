"""Register us_oi_opportunity_atlas (backend slug `opportunity_atlas`) metadata.

The dataset record, its organization (opportunity_insights), themes and tags
already exist on prod and staging; this script reuses them and adds the raw
sources and the seven tables.

Idempotent: every record is looked up first and its id passed back on update,
because create_update_* duplicates observation levels, cloud tables, coverages,
datetime ranges and updates when called without one.

The dataset is registered `under_review`. `--publish` flips it to `published`:
safe on dev/staging, which are not the public site; on prod only after the PR has
merged, table-approve has materialised the tables, and they are verified.

Usage (from the repo root, with an interpreter that has databasis_mcp):
    PYTHONPATH=.:<mcp>/src python models/us_oi_opportunity_atlas/code/register_metadata.py \
        [--env dev|staging|prod] [--dry-run] [--publish] [--only TABLE ...]
"""

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

sys.path.insert(0, str(Path(__file__).resolve().parent))
from table_meta import TABLE_ORDER, TABLES

ARCH_DIR = Path(__file__).resolve().parent / "architecture"
DATASET_SLUG = "opportunity_atlas"
GCP_DATASET_ID = "us_oi_opportunity_atlas"
ORG_SLUG = "opportunity_insights"
THEME_SLUGS = ["economics", "urbanization"]
AREA_SLUG = "us"
LICENSE_SLUG = "cc_by"
LANGUAGES = ["en"]

# Subject tags, English slugs on prod. No place, theme or organization tags.
TAG_SLUGS = [
    "equal-opportunity",
    "inequality",
    "mobility",
    "neighborhood",
    "income",
    "migration",
]
# The staging backend is an older snapshot whose tag vocabulary is still Portuguese.
STAGING_TAG_SLUGS = {
    "equal-opportunity": "igualdade_de_oportunidades",
    "inequality": "desigualdade",
    "mobility": "mobilidade",
    "neighborhood": "vizinhanca",
    "income": "renda",
    "migration": "migracao",
}

CREDIT = {
    "pt": "Ao usar os dados, credite a Opportunity Insights e o US Census Bureau.",
    "en": "When using the data, credit Opportunity Insights and the US Census Bureau.",
    "es": "Al usar los datos, cite a Opportunity Insights y a la US Census Bureau.",
}

DATASET = {
    "name_pt": "Opportunity Atlas",
    "name_en": "Opportunity Atlas",
    "name_es": "Atlas de Oportunidades",
    "description_pt": (
        "O Opportunity Atlas, da Opportunity Insights (Raj Chetty, John Friedman, "
        "Nathaniel Hendren e coautores), estima os resultados na idade adulta de "
        "crianças nascidas nos Estados Unidos entre 1978 e 1983 segundo o setor "
        "censitário, o condado e a zona de deslocamento em que cresceram, por "
        "raça, sexo e percentil de renda dos pais. As estimativas vinculam os "
        "Censos decenais de 2000 e 2010, as declarações federais de imposto de "
        "renda e a American Community Survey de 2005-2015, cobrindo cerca de 20 "
        "milhões de crianças, e incluem renda, encarceramento, escolaridade, "
        "casamento, emprego e mobilidade residencial. O conjunto inclui também os "
        "Migration Patterns, com os fluxos de jovens adultos nascidos entre 1984 "
        "e 1992 entre a zona de deslocamento onde moravam aos 16 anos e a zona "
        f"onde moram aos 26. {CREDIT['pt']}"
    ),
    "description_en": (
        "The Opportunity Atlas, by Opportunity Insights (Raj Chetty, John "
        "Friedman, Nathaniel Hendren and coauthors), estimates the adult outcomes "
        "of children born in the United States in 1978-1983 by the census tract, "
        "county and commuting zone in which they grew up, by race, gender and "
        "parental income percentile. The estimates link the 2000 and 2010 "
        "Decennial Censuses, federal income tax returns and the 2005-2015 "
        "American Community Survey, covering about 20 million children, and "
        "include income, incarceration, education, marriage, employment and "
        "residential mobility. The dataset also includes Migration Patterns, the "
        "flows of young adults born in 1984-1992 between the commuting zone where "
        f"they lived at age 16 and the one where they live at age 26. {CREDIT['en']}"
    ),
    "description_es": (
        "El Atlas de Oportunidades, de Opportunity Insights (Raj Chetty, John "
        "Friedman, Nathaniel Hendren y coautores), estima los resultados en la "
        "adultez de los niños nacidos en Estados Unidos entre 1978 y 1983 según "
        "el sector censal, el condado y la zona de desplazamiento en que "
        "crecieron, por raza, sexo y percentil de ingreso de los padres. Las "
        "estimaciones vinculan los Censos decenales de 2000 y 2010, las "
        "declaraciones federales de impuestos y la American Community Survey de "
        "2005-2015, cubren cerca de 20 millones de niños e incluyen ingresos, "
        "encarcelamiento, escolaridad, matrimonio, empleo y movilidad "
        "residencial. El conjunto incluye también los Migration Patterns, con los "
        "flujos de jóvenes adultos nacidos entre 1984 y 1992 entre la zona de "
        "desplazamiento donde vivían a los 16 años y aquella donde viven a los "
        f"26. {CREDIT['es']}"
    ),
}

RAW_SOURCES = {
    "atlas": {
        "name_pt": "The Opportunity Atlas (Opportunity Insights)",
        "name_en": "The Opportunity Atlas (Opportunity Insights)",
        "name_es": "The Opportunity Atlas (Opportunity Insights)",
        "url": "https://opportunityinsights.org/paper/the-opportunity-atlas/",
        "description_pt": (
            "Página do artigo The Opportunity Atlas na Opportunity Insights, com "
            "as tabelas de resultados por setor censitário, condado e zona de "
            "deslocamento em CSV e Stata e seus livros de códigos. O pacote de "
            "replicação está no Harvard Dataverse (doi:10.7910/DVN/NKCQM1) sob "
            "licença CC BY 4.0."
        ),
        "description_en": (
            "Opportunity Insights page of the paper The Opportunity Atlas, with "
            "the outcome tables by census tract, county and commuting zone in CSV "
            "and Stata and their codebooks. The replication package is on Harvard "
            "Dataverse (doi:10.7910/DVN/NKCQM1) under a CC BY 4.0 license."
        ),
        "description_es": (
            "Página del artículo The Opportunity Atlas en Opportunity Insights, "
            "con las tablas de resultados por sector censal, condado y zona de "
            "desplazamiento en CSV y Stata y sus libros de códigos. El paquete de "
            "replicación está en Harvard Dataverse (doi:10.7910/DVN/NKCQM1) bajo "
            "licencia CC BY 4.0."
        ),
    },
    "migration": {
        "name_pt": "The Radius of Economic Opportunity: Migration Patterns (Opportunity Insights)",
        "name_en": "The Radius of Economic Opportunity: Migration Patterns (Opportunity Insights)",
        "name_es": "The Radius of Economic Opportunity: Migration Patterns (Opportunity Insights)",
        "url": "https://opportunityinsights.org/paper/the-radius-of-economic-opportunity/",
        "description_pt": (
            "Página do artigo The Radius of Economic Opportunity na Opportunity "
            "Insights, com os quatro arquivos de fluxos origem-destino entre zonas "
            "de deslocamento (agregado, por raça, por quintil de renda dos pais e "
            "por ambos) e o dicionário de dados."
        ),
        "description_en": (
            "Opportunity Insights page of the paper The Radius of Economic "
            "Opportunity, with the four origin-destination commuting zone flow "
            "files (pooled, by race, by parental income quintile and by both) and "
            "the data dictionary."
        ),
        "description_es": (
            "Página del artículo The Radius of Economic Opportunity en Opportunity "
            "Insights, con los cuatro archivos de flujos origen-destino entre "
            "zonas de desplazamiento (agregado, por raza, por quintil de ingreso "
            "de los padres y por ambos) y el diccionario de datos."
        ),
    },
}

# Observation level entity -> column. Commuting zones have no entity of their
# own; `region` is the closest.
GEO_LEVEL = {
    "tract": ("census_tract", "tract_id"),
    "county": ("county", "county_id"),
    "commuting_zone": ("region", "commuting_zone_id"),
}
# Birth cohorts covered by each table.
COHORTS = {"atlas": (1978, 1983), "migration": (1984, 1992)}


def table_spec(table: str) -> dict[str, Any]:
    meta = TABLES[table]
    if table == "commuting_zone_migration":
        return {
            **meta,
            "levels": {"region": "origin_commuting_zone_id", "race": "race"},
            "source": "migration",
        }
    entity, col = GEO_LEVEL[meta["geo"]]
    return {
        **meta,
        "levels": {entity: col, "race": "race", "sex": "gender"},
        "source": "atlas",
    }


def read_arch(table: str) -> list[dict[str, str]]:
    """Return a table's architecture rows, in column order."""
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def columns_json(table: str) -> str:
    """Build the bulk_upsert payload for one table from its architecture CSV."""
    out = []
    for r in read_arch(table):
        col = {
            "name": r["name"],
            "bigquery_type": r["bigquery_type"],
            "description_pt": r["description"],
            "description_en": r["description_en"],
            "description_es": r["description_es"],
            "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
            "has_sensitive_data": r["has_sensitive_data"] == "yes",
        }
        for key in (
            "directory_column",
            "measurement_unit",
            "temporal_coverage",
        ):
            if r[key]:
                col[key] = r[key]
        if r["observations"]:
            if not (r["observations_en"] and r["observations_es"]):
                raise KeyError(
                    f"{table}.{r['name']}: untranslated observation"
                )
            col["observations_pt"] = r["observations"]
            col["observations_en"] = r["observations_en"]
            col["observations_es"] = r["observations_es"]
        out.append(col)
    return json.dumps(out, ensure_ascii=False)


def main() -> None:
    """Register (or update) the dataset, raw sources and the seven tables."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--only", nargs="*", default=None)
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"
    tables = [t for t in TABLE_ORDER if not args.only or t in args.only]

    for t in TABLE_ORDER:
        n = len(json.loads(columns_json(t)))  # also validates translations
        print(f"{t}: {n} columns")

    ids = bd_mcp_metadata.discover_ids(
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
    status_under_review = ids["status"]["under_review"]
    status_published = ids["status"]["published"]
    org_id = bd_mcp_metadata.lookup_id(
        category="organization", slug=ORG_SLUG, env=env
    )["id"]
    theme_ids = [ids["theme"][t] for t in THEME_SLUGS]
    tag_ids = [
        bd_mcp_metadata.lookup_id(
            category="tag",
            slug=STAGING_TAG_SLUGS[t] if env == "staging" else t,
            env=env,
        )["id"]
        for t in TAG_SLUGS
    ]
    area_id = bd_mcp_metadata.lookup_id(
        category="area", slug=AREA_SLUG, env=env
    )["id"]
    language_ids = [ids["language"][lang] for lang in LANGUAGES]
    account_id = bd_mcp_metadata.get_authenticated_account(env=env)["id"]
    entity = {
        e: ids["entity"][e]
        for e in ("census_tract", "county", "region", "race", "sex", "year")
    }
    print(
        f"env={env} org={org_id} themes={theme_ids} tags={len(tag_ids)} "
        f"area={area_id} languages={language_ids}"
    )
    if args.dry_run:
        return

    existing = bd_mcp_metadata.get_dataset(slug=DATASET_SLUG, env=env)
    ds = bd_mcp_write.create_update_dataset(
        slug=DATASET_SLUG,
        **DATASET,
        organization_ids=[org_id],
        theme_ids=theme_ids,
        tag_ids=tag_ids,
        status_id=status_published if args.publish else status_under_review,
        id=existing.get("id") if existing.get("found") else None,
        env=env,
    )
    dataset_id = ds["id"]
    print(f"dataset {DATASET_SLUG} -> {dataset_id}")

    prior_sources = {
        s["url"]: s["id"]
        for s in bd_mcp_write.get_raw_data_sources(
            dataset_slug=DATASET_SLUG, env=env
        )
        if s.get("url")
    }
    source_ids = {}
    for key, rs in RAW_SOURCES.items():
        src = bd_mcp_write.create_update_raw_data_source(
            dataset_id=dataset_id,
            **rs,
            license_id=ids["license"][LICENSE_SLUG],
            availability_id=ids["availability"]["online"],
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            language_ids=language_ids,
            status_id=status_published,
            id=prior_sources.get(rs["url"]),
            env=env,
        )
        source_ids[key] = src["id"]
        print(f"raw source {key} -> {src['id']}")

    # latest is a DateTime; a naive value is read as Sao Paulo time, so pass an offset
    today = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    for table in tables:
        spec = table_spec(table)
        prior = (
            bd_mcp_metadata.get_dataset(slug=DATASET_SLUG, env=env)
            .get("tables", {})
            .get(table, {})
        )
        names = {
            k: spec[k]
            for k in (
                "name_pt", "name_en", "name_es",
                "description_pt", "description_en", "description_es",
            )
        }  # fmt: skip
        common: dict[str, Any] = {
            "slug": table,
            **names,
            "dataset_id": dataset_id,
            "status_id": status_published,
            "published_by_ids": [account_id],
            "data_cleaned_by_ids": [account_id],
            "env": env,
        }
        t = bd_mcp_write.create_update_table(**common, id=prior.get("id"))
        table_id = t["id"]
        print(f"\ntable {table} -> {table_id}")

        prior_ols = {
            o["entity_id"]: o["id"]
            for o in prior.get("observation_levels", [])
        }
        ol_ids = {}
        for ent in spec["levels"]:
            o = bd_mcp_write.create_update_observation_level(
                table_id=table_id,
                entity_id=entity[ent],
                id=prior_ols.get(entity[ent]),
                env=env,
            )
            ol_ids[ent] = o["id"]
        bd_mcp_write.reorder_observation_levels(
            table_id=table_id,
            ol_ids=[ol_ids[e] for e in spec["levels"]],
            env=env,
        )

        res = bd_mcp_write.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_json(table), env=env
        )
        print(
            f"  columns: created={res['created']} updated={res['updated']} "
            f"errors={res['errors']}"
        )
        if res["errors"]:
            raise RuntimeError(
                f"{table}: column upsert errors {res['errors']}"
            )
        bd_mcp_write.reorder_columns(
            table_id=table_id,
            column_names=[r["name"] for r in read_arch(table)],
            env=env,
        )

        # Observation-level links, one call per column (no partition columns here).
        cols = {
            c["name"]: c["id"]
            for c in bd_mcp_metadata.get_dataset(slug=DATASET_SLUG, env=env)[
                "tables"
            ][table]["columns"]
        }
        for ent, col in spec["levels"].items():
            bd_mcp_write.update_column(
                column_id=cols[col],
                column_name=col,
                table_id=table_id,
                observation_level_id=ol_ids[ent],
                env=env,
            )
        print(f"  linked observation levels {sorted(spec['levels'].values())}")

        prior_ct = prior.get("cloud_tables", [])
        bd_mcp_write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=prior_ct[0]["id"] if prior_ct else None,
            env=env,
        )

        first, last = COHORTS[spec["source"]]
        prior_cov = prior.get("coverages", [])
        cov = bd_mcp_write.create_update_coverage(
            table_id=table_id,
            area_id=area_id,
            id=prior_cov[0]["id"] if prior_cov else None,
            env=env,
        )
        prior_dtr = prior_cov[0]["datetime_ranges"] if prior_cov else []
        bd_mcp_write.create_update_datetime_range(
            coverage_id=cov["id"],
            start_year=first,
            end_year=last,
            interval=1,
            id=prior_dtr[0]["id"] if prior_dtr else None,
            env=env,
        )

        prior_upd = prior.get("updates", [])
        bd_mcp_write.create_update_update(
            entity_id=entity["year"],
            frequency=1,
            latest=today,
            table_id=table_id,
            id=prior_upd[0]["id"] if prior_upd else None,
            env=env,
        )

        bd_mcp_write.create_update_table(
            **common,
            raw_data_source_ids=[source_ids[spec["source"]]],
            id=table_id,
        )
        print(
            f"  cloud table, coverage {first}-{last}, update and raw source linked"
        )

    if not args.only:
        bd_mcp_write.reorder_tables(
            dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
        )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
