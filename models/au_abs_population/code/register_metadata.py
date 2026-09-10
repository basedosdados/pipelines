"""Register au_abs_population metadata in the Data Basis backend.

Idempotent: every record is looked up first and its id passed back on update,
because create_update_* duplicates observation levels, cloud tables, coverages
and updates when called without one.

Column specs come from the architecture CSVs (the source of truth for names,
order, types, units and directory links) and are translated per language by
translations.py, so descriptions land in all three languages rather than
Portuguese-only.

Usage:
    python register_metadata.py [--env staging|prod] [--dry-run]
"""

import argparse
import csv
import json
import os
import sys

MCP = os.path.expanduser(
    "~/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"
)
sys.path.insert(0, MCP)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import server  # noqa: E402
import translations as tr  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.join(HERE, "architecture")

DATASET_SLUG = "population"
GCP_DATASET_ID = "au_abs_population"
ORG_SLUG = "abs"
THEME_SLUGS = ["population"]
# The tag vocabularies are slugged in different languages per backend: staging
# is Portuguese, prod is English (895 tags, none of the Portuguese slugs). The
# same eight concepts therefore resolve under different slugs, and looking up
# the wrong set silently yields an untagged dataset.
TAG_SLUGS = {
    "staging": [
        "demografia",
        "migracao",
        "fertilidade",
        "mortalidade",
        "nascimento",
        "obito",
        "projecao",
        "idade",
    ],
    "prod": [
        "demographics",
        "migration",
        "fertility",
        "mortality",
        "birth",
        "death",
        "projection",
        "age",
    ],
}

TABLE_ORDER = [
    "national_state",
    "erp_age_sex",
    "regional_sa2",
    "regional_lga",
    "projection",
    "series",
]

DATASET = {
    "name_pt": "População: Estimativas e Projeções (Austrália)",
    "name_en": "Population: Estimates and Projections (Australia)",
    "name_es": "Población: Estimaciones y Proyecciones (Australia)",
    "description_pt": (
        "Estimativas e projeções da população australiana publicadas pelo "
        "Australian Bureau of Statistics (ABS), reunindo três produtos: a "
        "população residente estimada trimestral da Austrália e dos estados e "
        "territórios, com os componentes da variação populacional, desde 1981 "
        "(antigo catálogo 3101.0); a população residente estimada anual por "
        "área estatística de nível 2 (SA2) e por área de governo local (LGA), "
        "desde 2001 (antigo catálogo 3218.0); e as projeções populacionais por "
        "idade simples e sexo até 2071, nas séries alta, média e baixa (antigo "
        "catálogo 3222.0). Inclui ainda a população residente estimada anual "
        "por idade simples e sexo desde 1971."
    ),
    "description_en": (
        "Australian population estimates and projections published by the "
        "Australian Bureau of Statistics (ABS), bringing together three "
        "products: the quarterly estimated resident population of Australia and "
        "the states and territories, with the components of population change, "
        "from 1981 (former catalogue 3101.0); the annual estimated resident "
        "population by Statistical Area Level 2 (SA2) and Local Government Area "
        "(LGA), from 2001 (former catalogue 3218.0); and the population "
        "projections by single year of age and sex to 2071, in the high, medium "
        "and low series (former catalogue 3222.0). It also includes the annual "
        "estimated resident population by single year of age and sex from 1971."
    ),
    "description_es": (
        "Estimaciones y proyecciones de la población australiana publicadas por "
        "la Oficina Australiana de Estadística (ABS), que reúnen tres productos: "
        "la población residente estimada trimestral de Australia y de los "
        "estados y territorios, con los componentes de la variación poblacional, "
        "desde 1981 (antiguo catálogo 3101.0); la población residente estimada "
        "anual por área estadística de nivel 2 (SA2) y por área de gobierno "
        "local (LGA), desde 2001 (antiguo catálogo 3218.0); y las proyecciones "
        "de población por edad simple y sexo hasta 2071, en las series alta, "
        "media y baja (antiguo catálogo 3222.0). Incluye además la población "
        "residente estimada anual por edad simple y sexo desde 1971."
    ),
}

RAW_SOURCES = {
    "national_state": {
        "name_pt": "População nacional, estadual e territorial (3101.0)",
        "name_en": "National, state and territory population (3101.0)",
        "name_es": "Población nacional, estatal y territorial (3101.0)",
        "url": "https://www.abs.gov.au/statistics/people/population/national-state-and-territory-population",
        "description_pt": (
            "Planilhas de séries temporais trimestrais do ABS com a população "
            "residente estimada e os componentes da variação populacional, além "
            "das planilhas anuais por idade simples e sexo."
        ),
        "description_en": (
            "ABS quarterly time-series spreadsheets of the estimated resident "
            "population and the components of population change, plus the annual "
            "spreadsheets by single year of age and sex."
        ),
        "description_es": (
            "Planillas de series temporales trimestrales del ABS con la población "
            "residente estimada y los componentes de la variación poblacional, "
            "además de las planillas anuales por edad simple y sexo."
        ),
    },
    "regional": {
        "name_pt": "População regional (3218.0)",
        "name_en": "Regional population (3218.0)",
        "name_es": "Población regional (3218.0)",
        "url": "https://www.abs.gov.au/statistics/people/population/regional-population",
        "description_pt": (
            "Data cubes anuais do ABS com a população residente estimada e os "
            "componentes da variação populacional por SA2, LGA e níveis "
            "geográficos superiores do ASGS."
        ),
        "description_en": (
            "ABS annual data cubes of the estimated resident population and the "
            "components of population change by SA2, LGA and higher ASGS "
            "geographic levels."
        ),
        "description_es": (
            "Data cubes anuales del ABS con la población residente estimada y los "
            "componentes de la variación poblacional por SA2, LGA y niveles "
            "geográficos superiores del ASGS."
        ),
    },
    "projection": {
        "name_pt": "Projeções populacionais (3222.0)",
        "name_en": "Population projections (3222.0)",
        "name_es": "Proyecciones de población (3222.0)",
        "url": "https://www.abs.gov.au/statistics/people/population/population-projections-australia",
        "description_pt": (
            "Planilhas de séries temporais do ABS com as projeções populacionais "
            "por idade simples e sexo, nas séries alta (A), média (B) e baixa (C)."
        ),
        "description_en": (
            "ABS time-series spreadsheets of the population projections by single "
            "year of age and sex, in the high (A), medium (B) and low (C) series."
        ),
        "description_es": (
            "Planillas de series temporales del ABS con las proyecciones de "
            "población por edad simple y sexo, en las series alta (A), media (B) "
            "y baja (C)."
        ),
    },
}

# table -> (names, description trio, raw source keys, observation-level entity
# slugs, the column that identifies each level, and the temporal coverage)
TABLES = {
    "national_state": {
        "name_pt": "População nacional, estadual e territorial (trimestral)",
        "name_en": "National, state and territory population (quarterly)",
        "name_es": "Población nacional, estatal y territorial (trimestral)",
        "description_pt": (
            "População residente estimada e componentes da variação populacional, "
            "trimestrais, para a Austrália e para cada estado e território, desde "
            "o segundo trimestre de 1981. Uma linha por trimestre, região, sexo e "
            "medida, identificada pelo código de série do ABS. A unidade varia por "
            "linha e é dada pela coluna unit."
        ),
        "description_en": (
            "Quarterly estimated resident population and components of population "
            "change for Australia and for each state and territory, from the June "
            "quarter 1981. One row per quarter, region, sex and measure, keyed on "
            "the ABS Series ID. The unit varies by row and is given by the unit "
            "column."
        ),
        "description_es": (
            "Población residente estimada y componentes de la variación "
            "poblacional, trimestrales, para Australia y para cada estado y "
            "territorio, desde el segundo trimestre de 1981. Una fila por "
            "trimestre, región, sexo y medida, identificada por el código de serie "
            "del ABS. La unidad varía por fila y la indica la columna unit."
        ),
        "sources": ["national_state"],
        "levels": {"quarter": "quarter", "state": "state_id", "sex": "sex"},
        "coverage": dict(
            start_year=1981, start_month=6, end_year=2025, end_month=12
        ),
        "update": ("quarter", 1, 2),
    },
    "erp_age_sex": {
        "name_pt": "População residente estimada por idade e sexo",
        "name_en": "Estimated resident population by age and sex",
        "name_es": "Población residente estimada por edad y sexo",
        "description_pt": (
            "População residente estimada por idade simples e sexo, em 30 de "
            "junho, para a Austrália e para cada estado e território, desde 1971. "
            "Uma linha por ano, região, sexo e idade simples. É a população "
            "efetivamente observada; a tabela projection traz a população "
            "projetada no mesmo nível de observação."
        ),
        "description_en": (
            "Estimated resident population by single year of age and sex, at 30 "
            "June, for Australia and for each state and territory, from 1971. One "
            "row per year, region, sex and single year of age. This is the "
            "historical actual population; the projection table holds the "
            "projected population on the same grain."
        ),
        "description_es": (
            "Población residente estimada por edad simple y sexo, al 30 de junio, "
            "para Australia y para cada estado y territorio, desde 1971. Una fila "
            "por año, región, sexo y edad simple. Es la población efectivamente "
            "observada; la tabla projection contiene la población proyectada en el "
            "mismo nivel de observación."
        ),
        "sources": ["national_state"],
        "levels": {
            "year": "year",
            "state": "state_id",
            "sex": "sex",
            "age": "age",
        },
        "coverage": dict(start_year=1971, end_year=2025),
        "update": ("year", 1, 1),
    },
    "regional_sa2": {
        "name_pt": "População regional por SA2",
        "name_en": "Regional population by SA2",
        "name_es": "Población regional por SA2",
        "description_pt": (
            "População residente estimada e componentes da variação populacional "
            "por área estatística de nível 2 (SA2), desde 2001. Uma linha por SA2 "
            "e ano, carregando toda a hierarquia do ASGS, de modo que os totais de "
            "SA3, SA4, GCCSA e estado podem ser recompostos por soma: verificou-se "
            "que todo agregado publicado pelo ABS é exatamente a soma de suas SA2, "
            "em todos os níveis e anos. Os componentes da variação são publicados "
            "apenas para os quatro anos fiscais mais recentes."
        ),
        "description_en": (
            "Estimated resident population and components of population change by "
            "Statistical Area Level 2 (SA2), from 2001. One row per SA2 and year, "
            "carrying the full ASGS hierarchy so that SA3, SA4, GCCSA and state "
            "totals can be recomputed by summing: every aggregate ABS publishes was "
            "verified to equal the sum of its SA2s exactly, at every level and in "
            "every year. Components of change are published for the four most "
            "recent financial years only."
        ),
        "description_es": (
            "Población residente estimada y componentes de la variación poblacional "
            "por área estadística de nivel 2 (SA2), desde 2001. Una fila por SA2 y "
            "año, con toda la jerarquía del ASGS, de modo que los totales de SA3, "
            "SA4, GCCSA y estado pueden recomponerse por suma: se verificó que todo "
            "agregado publicado por el ABS es exactamente la suma de sus SA2, en "
            "todos los niveles y años. Los componentes de la variación se publican "
            "solo para los cuatro años fiscales más recientes."
        ),
        "sources": ["regional"],
        "levels": {"year": "year", "region": "sa2_id"},
        "coverage": dict(start_year=2001, end_year=2025),
        "update": ("year", 1, 1),
    },
    "regional_lga": {
        "name_pt": "População regional por LGA",
        "name_en": "Regional population by LGA",
        "name_es": "Población regional por LGA",
        "description_pt": (
            "População residente estimada e componentes da variação populacional "
            "por área de governo local (LGA), desde 2001. Uma linha por LGA e ano. "
            "Os componentes da variação são publicados apenas para os quatro anos "
            "fiscais mais recentes. O ABS reexpressa a série nos limites vigentes "
            "na divulgação, que estão à frente do diretório de LGA do ASGS 2021."
        ),
        "description_en": (
            "Estimated resident population and components of population change by "
            "Local Government Area (LGA), from 2001. One row per LGA and year. "
            "Components of change are published for the four most recent financial "
            "years only. ABS restates the series onto the boundaries current at the "
            "release, which run ahead of the ASGS 2021 LGA directory."
        ),
        "description_es": (
            "Población residente estimada y componentes de la variación poblacional "
            "por área de gobierno local (LGA), desde 2001. Una fila por LGA y año. "
            "Los componentes de la variación se publican solo para los cuatro años "
            "fiscales más recientes. El ABS reexpresa la serie en los límites "
            "vigentes en la divulgación, que van por delante del directorio de LGA "
            "del ASGS 2021."
        ),
        "sources": ["regional"],
        "levels": {"year": "year", "local_government_area": "lga_id"},
        "coverage": dict(start_year=2001, end_year=2025),
        "update": ("year", 1, 1),
    },
    "projection": {
        "name_pt": "Projeções populacionais por idade e sexo",
        "name_en": "Population projections by age and sex",
        "name_es": "Proyecciones de población por edad y sexo",
        "description_pt": (
            "Projeções populacionais por idade simples e sexo, em 30 de junho, "
            "para a Austrália e para cada estado e território. Uma linha por ano "
            "projetado, série, região, sexo e idade, cobrindo as três séries "
            "publicadas: alta (série 1(A) do ABS), média (29(B)) e baixa (45(C)). "
            "São projeções, não estimativas: resultam das hipóteses declaradas de "
            "fecundidade, mortalidade e migração, e não são uma previsão. A "
            "população efetivamente observada, no mesmo nível de observação, está "
            "na tabela erp_age_sex."
        ),
        "description_en": (
            "Population projections by single year of age and sex, at 30 June, for "
            "Australia and for each state and territory. One row per projected "
            "year, series, region, sex and age, covering the three published "
            "series: high (ABS series 1(A)), medium (29(B)) and low (45(C)). These "
            "are projections, not estimates: they are the arithmetic consequence of "
            "the stated fertility, mortality and migration assumptions, not a "
            "forecast. The historical actual population on the same grain is in the "
            "erp_age_sex table."
        ),
        "description_es": (
            "Proyecciones de población por edad simple y sexo, al 30 de junio, para "
            "Australia y para cada estado y territorio. Una fila por año proyectado, "
            "serie, región, sexo y edad, que cubre las tres series publicadas: alta "
            "(serie 1(A) del ABS), media (29(B)) y baja (45(C)). Son proyecciones, "
            "no estimaciones: resultan de los supuestos declarados de fecundidad, "
            "mortalidad y migración, y no son un pronóstico. La población "
            "efectivamente observada, en el mismo nivel de observación, está en la "
            "tabla erp_age_sex."
        ),
        "sources": ["projection"],
        "levels": {
            "year": "year",
            "state": "state_id",
            "sex": "sex",
            "age": "age",
        },
        "coverage": dict(start_year=2022, end_year=2071),
        "update": ("year", 5, 0),
    },
    "series": {
        "name_pt": "Dicionário de séries do ABS",
        "name_en": "ABS series dictionary",
        "name_es": "Diccionario de series del ABS",
        "description_pt": (
            "Tabela de dimensão das séries temporais do ABS que alimentam este "
            "conjunto, uma linha por código de série, cobrindo as estimativas "
            "nacionais e estaduais (antigo catálogo 3101.0) e as projeções "
            "populacionais (3222.0). Traz descrição, unidade, frequência, tabela de "
            "origem e período de observação da série. O código de série é estável "
            "entre publicações e produtos do ABS, sendo a chave para relacionar "
            "estas tabelas a outras publicações do órgão."
        ),
        "description_en": (
            "Dimension table of the ABS time series that feed this dataset, one row "
            "per ABS Series ID, covering the national and state population estimates "
            "(former catalogue 3101.0) and the population projections (3222.0). "
            "Carries the series description, unit, frequency, source table and "
            "observation span. The Series ID is stable across ABS releases and "
            "products, so it is the key for joining these tables to other ABS output."
        ),
        "description_es": (
            "Tabla de dimensión de las series temporales del ABS que alimentan este "
            "conjunto, una fila por código de serie, que cubre las estimaciones "
            "nacionales y estatales (antiguo catálogo 3101.0) y las proyecciones de "
            "población (3222.0). Incluye descripción, unidad, frecuencia, tabla de "
            "origen y período de observación de la serie. El código de serie es "
            "estable entre publicaciones y productos del ABS, por lo que es la clave "
            "para relacionar estas tablas con otras publicaciones del organismo."
        ),
        "sources": ["national_state", "projection"],
        "levels": {"series": "series_id"},
        "coverage": None,
        "update": ("year", 1, 1),
    },
}


def columns_json(table: str) -> str:
    """Build the bulk_upsert payload for one table from its architecture CSV."""
    out = []
    with open(os.path.join(ARCH, f"{table}.csv"), encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            pt, es = tr.DESCRIPTIONS[r["description"]]
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": pt,
                "description_en": r["description"],
                "description_es": es,
                "covered_by_dictionary": r["covered_by_dictionary"] == "yes",
                "has_sensitive_data": r["has_sensitive_data"] == "yes",
            }
            if r["directory_column"]:
                col["directory_column"] = r["directory_column"]
            if r["measurement_unit"]:
                col["measurement_unit"] = r["measurement_unit"]
            if r["observations"]:
                opt, oes = tr.OBSERVATIONS[r["observations"]]
                col["observations_pt"] = opt
                col["observations_en"] = r["observations"]
                col["observations_es"] = oes
            if r["temporal_coverage"]:
                col["temporal_coverage"] = r["temporal_coverage"]
            out.append(col)
    return json.dumps(out, ensure_ascii=False)


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
    entity = ids["entity"]
    org_id = server.lookup_id(category="organization", slug=ORG_SLUG, env=env)[
        "id"
    ]
    theme_ids = [ids["theme"][t] for t in THEME_SLUGS]
    tag_slugs = TAG_SLUGS[env]
    tag_ids = [
        server.lookup_id(category="tag", slug=t, env=env)["id"]
        for t in tag_slugs
    ]
    area_au = server.lookup_id(category="area", slug="au", env=env)["id"]
    account = server.get_authenticated_account(env=env)
    account_id = account["id"]

    print(
        f"env={env} org={org_id} theme={theme_ids} tags={len(tag_ids)} area_au={area_au}"
    )
    if args.dry_run:
        for t in TABLE_ORDER:
            cols = json.loads(columns_json(t))
            print(
                f"  {t}: {len(cols)} columns, levels={list(TABLES[t]['levels'])}"
            )
        return

    existing = server.get_dataset(slug=DATASET_SLUG, env=env)
    ds = server.create_update_dataset(
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
    # second copy of all three sources.
    prior_sources = {
        s["url"]: s["id"]
        for s in server.get_raw_data_sources(
            dataset_slug=DATASET_SLUG, env=env
        )
        if s.get("url")
    }
    source_ids = {}
    for key, spec in RAW_SOURCES.items():
        r = server.create_update_raw_data_source(
            dataset_id=dataset_id,
            **spec,
            license_id=ids["license"]["cc_by"],
            availability_id=ids["availability"]["online"],
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            status_id=status_published,
            id=prior_sources.get(spec["url"]),
            env=env,
        )
        source_ids[key] = r["id"]
        print(f"  raw source {key} -> {r['id']}")

    for table in TABLE_ORDER:
        spec = TABLES[table]
        # Re-read per table rather than from one pre-loop snapshot: a partial
        # run leaves records behind, and create_update_* duplicates observation
        # levels, cloud tables, coverages and updates when called without an id.
        prior = (
            server.get_dataset(slug=DATASET_SLUG, env=env)
            .get("tables", {})
            .get(table, {})
        )
        t = server.create_update_table(
            slug=table,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            description_en=spec["description_en"],
            description_es=spec["description_es"],
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
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
            o = server.create_update_observation_level(
                table_id=table_id,
                entity_id=entity[ent_slug],
                id=prior_ols.get(ent_slug),
                env=env,
            )
            ol_ids[ent_slug] = o["id"]
        server.reorder_observation_levels(
            table_id=table_id,
            ol_ids=[ol_ids[e] for e in spec["levels"]],
            env=env,
        )
        print(f"  observation levels: {list(spec['levels'])}")

        res = server.bulk_upsert_columns(
            table_id=table_id, columns_json=columns_json(table), env=env
        )
        print(
            f"  columns: created={res['created']} updated={res['updated']} errors={res['errors']}"
        )
        # Fail here rather than a few lines down: an unupserted column makes the
        # observation-level link raise a bare KeyError on a name the backend
        # does not have, after the table is already partly registered.
        if res["errors"]:
            raise RuntimeError(
                f"{table}: column upsert errors {res['errors']}"
            )

        # Link each identifying column to its observation level, and re-assert
        # is_partition in the same call: update_column's booleans default to
        # False and would otherwise clear the flag.
        cols = {
            c["name"]: c["id"]
            for c in server.get_dataset(slug=DATASET_SLUG, env=env)["tables"][
                table
            ]["columns"]
        }
        for ent_slug, col_name in spec["levels"].items():
            server.update_column(
                column_id=cols[col_name],
                column_name=col_name,
                table_id=table_id,
                observation_level_id=ol_ids[ent_slug],
                is_partition=(col_name == "year"),
                env=env,
            )
        if "year" in cols and "year" not in spec["levels"].values():
            server.update_column(
                column_id=cols["year"],
                column_name="year",
                table_id=table_id,
                is_partition=True,
                env=env,
            )
        print(
            f"  linked {len(spec['levels'])} identifying columns to their levels"
        )

        prior_ct = prior.get("cloud_tables", [])
        server.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=prior_ct[0]["id"] if prior_ct else None,
            env=env,
        )

        prior_cov = prior.get("coverages", [])
        cov = server.create_update_coverage(
            table_id=table_id,
            area_id=area_au,
            id=prior_cov[0]["id"] if prior_cov else None,
            env=env,
        )
        if spec["coverage"]:
            prior_dr = (
                prior_cov[0].get("datetime_ranges", []) if prior_cov else []
            )
            server.create_update_datetime_range(
                coverage_id=cov["id"],
                interval=1,
                id=prior_dr[0]["id"] if prior_dr else None,
                env=env,
                **spec["coverage"],
            )

        ent, freq, lag = spec["update"]
        prior_up = prior.get("updates", [])
        server.create_update_update(
            entity_id=entity[ent],
            frequency=freq,
            lag=lag,
            latest=NOW,
            table_id=table_id,
            id=prior_up[0]["id"] if prior_up else None,
            env=env,
        )

        server.create_update_table(
            slug=table,
            name_pt=spec["name_pt"],
            name_en=spec["name_en"],
            name_es=spec["name_es"],
            description_pt=spec["description_pt"],
            description_en=spec["description_en"],
            description_es=spec["description_es"],
            dataset_id=dataset_id,
            status_id=status_published,
            published_by_ids=[account_id],
            data_cleaned_by_ids=[account_id],
            raw_data_source_ids=[source_ids[s] for s in spec["sources"]],
            id=table_id,
            env=env,
        )
        print("  cloud table, coverage, update and raw sources linked")

    server.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


# The backend field is a DateTime, so a bare date is rejected.
NOW = __import__("datetime").datetime.now().replace(microsecond=0).isoformat()

if __name__ == "__main__":
    main()
