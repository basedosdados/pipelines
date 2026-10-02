"""Register br_bd_diretorios_ar metadata in the Data Basis backend.

Idempotent: every record is looked up first and its id passed back on update,
because create_update_* duplicates observation levels, cloud tables and
coverages when called without one.

Column specs come from the architecture CSVs (the source of truth for names,
order, types and directory links). Those CSVs are written in Spanish, the
language of the data, so translations.py supplies the Portuguese and English
renderings and this script fails rather than registering a Spanish-only column.

Three entities are created if the backend does not have them: department,
locality and agglomeration. The existing vocabulary covers province and
municipality but has no exact match for Argentina's other three levels, and the
near misses (county, city, metropolitan_area) misname them.

Following br_bd_diretorios_cl, the tables carry no Update record: these are
static catalogs of the 2022 census coding, not a refreshed series. They do carry
a Coverage for Argentina with no datetime range.

Usage:
    ~/.pyenv/versions/3.11.6/bin/python register_metadata.py \
        [--env staging|prod|dev] [--dry-run] [--publish]
"""

import argparse
import csv
import json
import os

import databasis_mcp.tools.metadata as server
import databasis_mcp.tools.write as write

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


from models.br_bd_diretorios_ar.code import translations as tr  # noqa: E402

DATASET_SLUG = "br_bd_diretorios_ar"
GCP_DATASET_ID = "br_bd_diretorios_ar"
ORG_SLUG = "bd"
THEME_SLUGS = ["territory"]
AREA_SLUG = "ar"

# The tag vocabularies are slugged in different languages per backend: staging
# is Portuguese, prod English. The same five concepts therefore resolve under
# different slugs, and looking up the wrong set silently yields an untagged
# dataset. Both sets were confirmed present on their backend.
TAG_SLUGS = {
    "dev": [
        "administrative-division",
        "codigo",
        "diretorio",
        "municipio",
        "censo",
    ],
    "staging": [
        "administrative-division",
        "codigo",
        "diretorio",
        "municipio",
        "censo",
    ],
    "prod": [
        "administrative-division",
        "code",
        "directory",
        "municipality",
        "census",
    ],
}

# Entities absent from the backend vocabulary, created on first run. All spatial.
NEW_ENTITIES = {
    "department": ("Departamento", "Department", "Departamento"),
    "locality": ("Localidade", "Locality", "Localidad"),
    "agglomeration": ("Aglomerado", "Agglomeration", "Aglomerado"),
}

TABLE_ORDER = [
    "jurisdiccion",
    "departamento",
    "gobierno_local",
    "aglomerado",
    "localidad",
    "dicionario",
]

DATASET = {
    "name_pt": "Diretórios Argentina",
    "name_en": "Argentina Directories",
    "name_es": "Directorios Argentina",
    "description_pt": (
        "Tabelas de diretório com a divisão político-administrativa da "
        "Argentina segundo o INDEC: as 24 jurisdições, 529 departamentos, "
        "2.315 governos locais, 3.706 aglomerados e 4.023 localidades "
        "censitárias, com seus códigos geográficos oficiais e a hierarquia "
        "entre eles, usadas como chaves estrangeiras pelos demais conjuntos de "
        "dados argentinos. A tabela de jurisdições traz ainda o nome oficial "
        "completo e a sigla ISO 3166-2:AR."
    ),
    "description_en": (
        "Directory tables with Argentina's political and administrative "
        "division from INDEC: the 24 jurisdictions, 529 departments, 2,315 "
        "local governments, 3,706 agglomerations and 4,023 census localities, "
        "with their official geographic codes and the hierarchy between them, "
        "used as foreign keys by every other Argentine dataset. The "
        "jurisdiction table also carries the full official name and the ISO "
        "3166-2:AR abbreviation."
    ),
    "description_es": (
        "Tablas de directorio con la división político-administrativa de "
        "Argentina según el INDEC: las 24 jurisdicciones, 529 departamentos, "
        "2.315 gobiernos locales, 3.706 aglomerados y 4.023 localidades "
        "censales, con sus códigos geográficos oficiales y la jerarquía entre "
        "ellos, usadas como llaves foráneas por los demás conjuntos de datos "
        "argentinos. La tabla de jurisdicciones incluye además el nombre "
        "oficial completo y la sigla ISO 3166-2:AR."
    ),
}

# One raw source: the five workbooks are files of a single INDEC publication,
# and the auxiliary-files rule allows at most one raw source per table.
RAW_SOURCE = {
    "name_pt": "Códigos geográficos do INDEC 2022",
    "name_en": "INDEC geographic codes 2022",
    "name_es": "Códigos geográficos del INDEC 2022",
    "url": "https://datos.gob.ar/dataset/codigos-geograficos-del-indec-2022",
    "description_pt": (
        "Cinco planilhas XLSX do INDEC com os códigos das unidades "
        "geoestatísticas usadas no Censo Nacional de População, Domicílios e "
        "Habitações 2022: jurisdições, departamentos, governos locais, "
        "aglomerados e localidades censitárias. Publicadas em "
        "indec.gob.ar/ftp/cuadros/geoestadistica e catalogadas em datos.gob.ar."
    ),
    "description_en": (
        "Five INDEC XLSX workbooks with the codes of the geostatistical units "
        "used in the 2022 National Census of Population, Households and "
        "Dwellings: jurisdictions, departments, local governments, "
        "agglomerations and census localities. Published under "
        "indec.gob.ar/ftp/cuadros/geoestadistica and catalogued on datos.gob.ar."
    ),
    "description_es": (
        "Cinco planillas XLSX del INDEC con los códigos de las unidades "
        "geoestadísticas utilizadas en el Censo Nacional de Población, Hogares "
        "y Viviendas 2022: jurisdicciones, departamentos, gobiernos locales, "
        "aglomerados y localidades censales. Publicadas en "
        "indec.gob.ar/ftp/cuadros/geoestadistica y catalogadas en datos.gob.ar."
    ),
}

# table -> names, descriptions, and the observation level with the column that
# identifies it. One level per table: the table's own grain.
TABLES = {
    "jurisdiccion": {
        "name_pt": "Jurisdição",
        "name_en": "Jurisdiction",
        "name_es": "Jurisdicción",
        "description_pt": (
            "Diretório das 24 jurisdições da Argentina: as 23 províncias e a "
            "Cidade Autônoma de Buenos Aires, com o código de dois dígitos do "
            "INDEC, o nome oficial completo e a sigla ISO 3166-2:AR."
        ),
        "description_en": (
            "Directory of Argentina's 24 jurisdictions: the 23 provinces and "
            "the Autonomous City of Buenos Aires, with INDEC's two-digit code, "
            "the full official name and the ISO 3166-2:AR abbreviation."
        ),
        "description_es": (
            "Directorio de las 24 jurisdicciones de Argentina: las 23 "
            "provincias y la Ciudad Autónoma de Buenos Aires, con el código de "
            "dos dígitos del INDEC, el nombre oficial completo y la sigla ISO "
            "3166-2:AR."
        ),
        "levels": {"province": "id_jurisdiccion"},
        "primary_key": "id_jurisdiccion",
    },
    "departamento": {
        "name_pt": "Departamento",
        "name_en": "Department",
        "name_es": "Departamento",
        "description_pt": (
            "Diretório dos 529 departamentos da Argentina, com o código de "
            "cinco dígitos e a jurisdição à qual pertencem. A unidade chama-se "
            "departamento em 21 províncias, partido na província de Buenos "
            "Aires e comuna na Cidade Autônoma de Buenos Aires. É a tabela de "
            "referência para cruzar qualquer conjunto de dados argentino no "
            "nível departamental."
        ),
        "description_en": (
            "Directory of Argentina's 529 departments, with the five-digit code "
            "and the jurisdiction they belong to. The unit is called "
            "departamento in 21 provinces, partido in Buenos Aires province and "
            "comuna in the Autonomous City of Buenos Aires. It is the reference "
            "table for joining any Argentine dataset at department level."
        ),
        "description_es": (
            "Directorio de los 529 departamentos de Argentina, con el código de "
            "cinco dígitos y la jurisdicción a la que pertenecen. La unidad se "
            "denomina departamento en 21 provincias, partido en la provincia de "
            "Buenos Aires y comuna en la Ciudad Autónoma de Buenos Aires. Es la "
            "tabla de referencia para cruzar cualquier conjunto de datos "
            "argentino a nivel departamental."
        ),
        "levels": {"department": "id_departamento"},
        "primary_key": "id_departamento",
    },
    "gobierno_local": {
        "name_pt": "Governo local",
        "name_en": "Local government",
        "name_es": "Gobierno local",
        "description_pt": (
            "Diretório dos 2.315 governos locais da Argentina, com o código de "
            "seis dígitos estabelecido pela Resolução INDEC 144/2022 e a "
            "categoria segundo o regime municipal de cada jurisdição. O governo "
            "local não se aninha no departamento: 24 abrangem mais de um, de "
            "modo que a tabela carrega apenas a chave da jurisdição. Inclui 11 "
            "registros «Sin Gobierno Local» e 24 «Indeterminado», ambos sem "
            "categoria."
        ),
        "description_en": (
            "Directory of Argentina's 2,315 local governments, with the "
            "six-digit code set by INDEC Resolution 144/2022 and the category "
            "under each jurisdiction's municipal regime. The local government "
            "does not nest in the department: 24 span more than one, so the "
            "table carries the jurisdiction key only. It includes 11 «Sin "
            "Gobierno Local» and 24 «Indeterminado» records, both without a "
            "category."
        ),
        "description_es": (
            "Directorio de los 2.315 gobiernos locales de Argentina, con el "
            "código de seis dígitos establecido por la Resolución INDEC "
            "144/2022 y su categoría según el régimen municipal de cada "
            "jurisdicción. El gobierno local no anida en el departamento: 24 "
            "abarcan más de uno, de modo que la tabla solo lleva la llave de la "
            "jurisdicción. Incluye 11 registros «Sin Gobierno Local» y 24 "
            "«Indeterminado», ambos sin categoría."
        ),
        "levels": {"municipality": "id_gobierno_local"},
        "primary_key": "id_gobierno_local",
    },
    "aglomerado": {
        "name_pt": "Aglomerado",
        "name_en": "Agglomeration",
        "name_es": "Aglomerado",
        "description_pt": (
            "Diretório dos 3.706 aglomerados da Argentina. O aglomerado agrupa "
            "as localidades censitárias que formam uma mesma mancha urbana; 119 "
            "agrupam mais de uma localidade e carregam a etiqueta publicada "
            "pelo INDEC, enquanto os 3.587 restantes são de uma só localidade e "
            "tomam o nome dela. A tabela não carrega chaves geográficas porque "
            "14 aglomerados abrangem mais de uma jurisdição e 62 mais de um "
            "departamento."
        ),
        "description_en": (
            "Directory of Argentina's 3,706 agglomerations. An agglomeration "
            "groups the census localities forming one continuous urban area; "
            "119 group more than one locality and carry the label INDEC "
            "publishes, while the remaining 3,587 hold a single locality and "
            "take its name. The table carries no geographic keys because 14 "
            "agglomerations span more than one jurisdiction and 62 more than "
            "one department."
        ),
        "description_es": (
            "Directorio de los 3.706 aglomerados de Argentina. El aglomerado "
            "agrupa las localidades censales que forman una misma mancha "
            "urbana; 119 agrupan más de una localidad y llevan la etiqueta "
            "publicada por el INDEC, mientras que los 3.587 restantes son de "
            "una sola localidad y toman su nombre. La tabla no lleva llaves "
            "geográficas porque 14 aglomerados abarcan más de una jurisdicción "
            "y 62 más de un departamento."
        ),
        "levels": {"agglomeration": "id_aglomerado"},
        "primary_key": "id_aglomerado",
    },
    "localidad": {
        "name_pt": "Localidade",
        "name_en": "Locality",
        "name_es": "Localidad",
        "description_pt": (
            "Diretório das 4.023 localidades censitárias da Argentina, com o "
            "código de oito dígitos e as chaves do departamento, da jurisdição, "
            "do governo local e do aglomerado aos quais pertence. É o nível "
            "mais fino do diretório."
        ),
        "description_en": (
            "Directory of Argentina's 4,023 census localities, with the "
            "eight-digit code and the keys of the department, jurisdiction, "
            "local government and agglomeration it belongs to. It is the "
            "directory's finest level."
        ),
        "description_es": (
            "Directorio de las 4.023 localidades censales de Argentina, con su "
            "código de ocho dígitos y las llaves del departamento, la "
            "jurisdicción, el gobierno local y el aglomerado a los que "
            "pertenece. Es el nivel más fino del directorio."
        ),
        "levels": {"locality": "id_localidad"},
        "primary_key": "id_localidad",
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Etiquetas dos valores codificados do diretório: a categoria de "
            "governo local e o tipo de localidade. As 20 categorias são "
            "transcritas da aba «Definiciones» do arquivo de governos locais do "
            "INDEC; 18 aparecem no Censo 2022. Não cobre as colunas resolvidas "
            "por chave, cuja fonte de verdade são as demais tabelas deste "
            "diretório."
        ),
        "description_en": (
            "Labels for the directory's coded values: the local government "
            "category and the locality type. The 20 categories are transcribed "
            "from the «Definiciones» sheet of INDEC's local governments "
            "workbook; 18 appear in the 2022 census. It does not cover the "
            "columns resolved by key, whose source of truth is the other tables "
            "of this directory."
        ),
        "description_es": (
            "Etiquetas de los valores codificados del directorio: la categoría "
            "de gobierno local y el tipo de localidad. Las 20 categorías están "
            "transcritas de la hoja «Definiciones» del archivo de gobiernos "
            "locales del INDEC; 18 aparecen en el Censo 2022. No cubre las "
            "columnas resueltas por llave, cuya fuente de verdad son las demás "
            "tablas de este directorio."
        ),
        "levels": {},
        "primary_key": None,
    },
}


def arch_order(table: str) -> list[str]:
    """Column names of one table, in architecture order."""
    with open(os.path.join(ARCH, f"{table}.csv"), encoding="utf-8") as fh:
        return [r["name"] for r in csv.DictReader(fh)]


def columns_json(table: str) -> str:
    """Build the bulk_upsert payload for one table from its architecture CSV."""
    out = []
    with open(os.path.join(ARCH, f"{table}.csv"), encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r["description"] not in tr.DESCRIPTIONS:
                raise KeyError(
                    f"{table}.{r['name']}: no translation for description "
                    f"{r['description']!r}"
                )
            pt, en = tr.DESCRIPTIONS[r["description"]]
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": pt,
                "description_en": en,
                "description_es": r["description"],
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
            }
            if r["directory_column"]:
                col["directory_column"] = r["directory_column"]
            if r["measurement_unit"]:
                col["measurement_unit"] = r["measurement_unit"]
            if r["observations"]:
                if r["observations"] not in tr.OBSERVATIONS:
                    raise KeyError(
                        f"{table}.{r['name']}: no translation for observations "
                        f"{r['observations']!r}"
                    )
                opt, oen = tr.OBSERVATIONS[r["observations"]]
                col["observations_pt"] = opt
                col["observations_en"] = oen
                col["observations_es"] = r["observations"]
            if r["temporal_coverage"]:
                col["temporal_coverage"] = r["temporal_coverage"]
            out.append(col)
    return json.dumps(out, ensure_ascii=False)


def spatial_category_id(env: str) -> str:
    """Id of the `spatial` entity category.

    Goes through server._gql rather than server.lookup_id because the MCP
    queries `allEntityCategory` while the backend exposes `allEntitycategory`,
    so lookup_id(category="entity_category", ...) and
    discover_ids(keys=["entity_category"]) both fail with HTTP 400. Remove this
    helper once the MCP is fixed.
    """
    q = """
    query { allEntitycategory(slug: "spatial") { edges { node { id slug } } } }
    """
    edges = server._gql(q, env=env, auth=False)["allEntitycategory"]["edges"]
    if not edges:
        raise RuntimeError(f"entity category 'spatial' not found on {env}")
    return edges[0]["node"]["id"].split(":")[-1]


def resolve_entities(env: str, entity_ids: dict) -> dict:
    """Return the entity ids for every level, creating the three missing ones."""
    needed = {e for spec in TABLES.values() for e in spec["levels"]}
    resolved = {}
    spatial = spatial_category_id(env)
    for slug in sorted(needed):
        if slug in entity_ids:
            resolved[slug] = entity_ids[slug]
            continue
        if slug not in NEW_ENTITIES:
            raise KeyError(
                f"entity {slug!r} is absent from the backend and is not one of "
                f"the entities this script creates"
            )
        pt, en, es = NEW_ENTITIES[slug]
        r = write.create_update_entity(
            slug=slug,
            name_pt=pt,
            name_en=en,
            name_es=es,
            category_id=spatial,
            env=env,
        )
        resolved[slug] = r["id"]
        print(f"  created entity {slug} -> {r['id']}")
    return resolved


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--publish",
        action="store_true",
        help=(
            "flip the dataset to published. Safe on dev/staging, which is not "
            "the public site; on prod, only after the PR has merged, "
            "table-approve has materialised the tables, and they are verified"
        ),
    )
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    ids = server.discover_ids(
        env=env, keys=["status", "entity", "license", "availability", "theme"]
    )
    status_under_review = ids["status"]["under_review"]
    status_published = ids["status"]["published"]
    org_id = server.lookup_id(category="organization", slug=ORG_SLUG, env=env)[
        "id"
    ]
    theme_ids = [ids["theme"][t] for t in THEME_SLUGS]
    tag_ids = [
        server.lookup_id(category="tag", slug=t, env=env)["id"]
        for t in TAG_SLUGS[env]
    ]
    area_id = server.lookup_id(category="area", slug=AREA_SLUG, env=env)["id"]
    account_id = server.get_authenticated_account(env=env)["id"]

    print(
        f"env={env} org={org_id} themes={theme_ids} "
        f"tags={len(tag_ids)} area_{AREA_SLUG}={area_id}"
    )
    if args.dry_run:
        for t in TABLE_ORDER:
            cols = json.loads(columns_json(t))
            missing = [
                e for e in TABLES[t]["levels"] if e not in ids["entity"]
            ]
            print(
                f"  {t}: {len(cols)} columns, "
                f"levels={list(TABLES[t]['levels'])}"
                + (f", entities to create: {missing}" if missing else "")
            )
        return

    entity = resolve_entities(env, ids["entity"])

    existing = server.get_dataset(slug=DATASET_SLUG, env=env)
    ds = write.create_update_dataset(
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

    # Key on url, not name: get_raw_data_sources returns only the Portuguese
    # name, so matching on name_en never hits and every run would create a
    # second copy of the source.
    prior_sources = {
        s["url"]: s["id"]
        for s in write.get_raw_data_sources(dataset_slug=DATASET_SLUG, env=env)
        if s.get("url")
    }
    source = write.create_update_raw_data_source(
        dataset_id=dataset_id,
        **RAW_SOURCE,
        license_id=ids["license"]["cc_by"],
        availability_id=ids["availability"]["online"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        status_id=status_published,
        id=prior_sources.get(RAW_SOURCE["url"]),
        env=env,
    )
    print(f"  raw source -> {source['id']}")

    for table in TABLE_ORDER:
        spec = TABLES[table]
        # Re-read per table rather than from one pre-loop snapshot: a partial
        # run leaves records behind, and create_update_* duplicates observation
        # levels, cloud tables and coverages when called without an id.
        prior = (
            server.get_dataset(slug=DATASET_SLUG, env=env)
            .get("tables", {})
            .get(table, {})
        )
        names = {
            k: spec[k]
            for k in (
                "name_pt",
                "name_en",
                "name_es",
                "description_pt",
                "description_en",
                "description_es",
            )
        }
        # is_directory is load-bearing, not cosmetic: the backend only accepts a
        # directoryPrimaryKey whose target is the primary key of a table flagged
        # this way. Without it bulk_upsert_columns has its FK rejected and
        # silently retries the column without one, so every id_* link is lost
        # and no error is reported. dicionario is not a directory table.
        is_directory = spec["primary_key"] is not None
        t = write.create_update_table(
            slug=table,
            # pyrefly: ignore [bad-argument-type]
            **names,
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            is_directory=is_directory,
            id=prior.get("id"),
            env=env,
        )
        table_id = t["id"]
        print(f"\ntable {table} -> {table_id}")

        prior_ols = {
            o["entity_slug"]: o["id"]
            for o in prior.get("observation_levels", [])
        }
        ol_ids = {}
        for ent_slug in spec["levels"]:
            o = write.create_update_observation_level(
                table_id=table_id,
                entity_id=entity[ent_slug],
                id=prior_ols.get(ent_slug),
                env=env,
            )
            ol_ids[ent_slug] = o["id"]
        if ol_ids:
            write.reorder_observation_levels(
                table_id=table_id,
                ol_ids=[ol_ids[e] for e in spec["levels"]],
                env=env,
            )
        print(f"  observation levels: {list(spec['levels'])}")

        res = write.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_json(table), env=env
        )
        print(
            f"  columns: created={res['created']} "
            f"updated={res['updated']} errors={res['errors']}"
        )
        # Fail here rather than a few lines down: an unupserted column makes the
        # observation-level link raise a bare KeyError on a name the backend
        # does not have, after the table is already partly registered.
        if res["errors"]:
            raise RuntimeError(
                f"{table}: column upsert errors {res['errors']}"
            )

        # bulk_upsert appends a column it has to retry, so the stored order can
        # drift from the architecture. Restore it explicitly.
        write.reorder_columns(
            table_id=table_id, column_names=arch_order(table), env=env
        )

        # Link the identifying column to its observation level (without this the
        # site renders the level's columns as "Não informado") and mark it the
        # primary key in the same call: update_column's booleans default to
        # False, so a second call would clear whatever the first one set. This
        # is a directory dataset, the one place is_primary_key belongs. No table
        # here is partitioned, so is_partition stays False.
        if spec["levels"]:
            cols = {
                c["name"]: c["id"]
                for c in server.get_dataset(slug=DATASET_SLUG, env=env)[
                    "tables"
                ][table]["columns"]
            }
            for ent_slug, col_name in spec["levels"].items():
                write.update_column(
                    column_id=cols[col_name],
                    column_name=col_name,
                    table_id=table_id,
                    observation_level_id=ol_ids[ent_slug],
                    is_primary_key=(col_name == spec["primary_key"]),
                    env=env,
                )
            print(
                f"  linked {len(spec['levels'])} identifying column(s) to "
                f"their level; primary key {spec['primary_key']}"
            )

        prior_ct = prior.get("cloud_tables", [])
        write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=prior_ct[0]["id"] if prior_ct else None,
            env=env,
        )

        # Coverage for Argentina, with no datetime range: the directory is a
        # static catalog of the 2022 census coding, not a temporal series. Same
        # shape as br_bd_diretorios_cl, which carries no Update record either.
        prior_cov = prior.get("coverages", [])
        write.create_update_coverage(
            table_id=table_id,
            area_id=area_id,
            id=prior_cov[0]["id"] if prior_cov else None,
            env=env,
        )

        write.create_update_table(
            slug=table,
            # pyrefly: ignore [bad-argument-type]
            **names,
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=[source["id"]],
            id=table_id,
            env=env,
        )
        print("  cloud table, coverage and raw source linked")

    write.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
