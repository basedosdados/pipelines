"""Register se_gu_vdem (backend slug `varieties_of_democracy_v_dem`) metadata.

Idempotent: every record is looked up first and its id passed back on update,
because create_update_* duplicates observation levels, cloud tables, coverages,
datetime ranges and updates when called without one.

The dataset record already exists on prod and staging (with no tables), as does
one raw data source pointing at the v10 download page; both are updated in place.

Column specs come from the architecture CSVs, which carry the English and
Spanish descriptions and observations next to the Portuguese ones.

The dataset is registered `under_review`. `--publish` flips it to `published`:
safe on dev/staging, which are not the public site; on prod only after the PR has
merged, table-approve has materialised the tables, and they are verified.

Usage (from the repo root, with an interpreter that has databasis_mcp):
    python models/se_gu_vdem/code/register_metadata.py \
        [--env dev|staging|prod] [--dry-run] [--publish]
"""

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any

import databasis_mcp.tools.metadata as bd_mcp_metadata
import databasis_mcp.tools.write as bd_mcp_write

ARCH_DIR = Path(__file__).resolve().parent / "architecture"
DATASET_SLUG = "varieties_of_democracy_v_dem"
GCP_DATASET_ID = "se_gu_vdem"
ORG_SLUG = "university_of_gothenburg"
THEME_SLUGS = ["politics"]
AREA_SLUG = "world"
LICENSE_SLUG = "cc_by_sa"
LANGUAGES = ["en"]
FIRST_YEAR, LAST_YEAR = 1789, 2025

# Subject tags, English slugs on prod. No place, theme or organization tags.
TAG_SLUGS = [
    "democracy",
    "authoritarianism",
    "elections",
    "electoral-system",
    "corruption",
    "human-rights",
    "freedom_of_expression",
    "judiciary",
    "legislature",
]
# The staging backend is an older snapshot whose tag vocabulary is still
# Portuguese; the records (and ids) are the same.
STAGING_TAG_SLUGS = {
    "democracy": "democracia",
    "authoritarianism": "autoritarismo",
    "elections": "eleicao",
    "electoral-system": "sistema_eleitoral",
    "corruption": "corrupcao",
    "human-rights": "direitos_humanos",
    "freedom_of_expression": "liberdade_de_expressao",
    "judiciary": "judiciario",
    "legislature": "legislatura",
}

TABLE_ORDER = [
    "country_year_index",
    "country_year",
    "indicator",
    "country",
    "dicionario",
]

DATASET = {
    "name_pt": "Varieties of Democracy (V-Dem)",
    "name_en": "Varieties of Democracy (V-Dem)",
    "name_es": "Variedades de Democracia (V-Dem)",
    "description_pt": (
        "O Varieties of Democracy (V-Dem), do V-Dem Institute da Universidade de "
        "Gotemburgo, mede a democracia em 202 países e unidades políticas de 1789 "
        "a 2025. A versão 16 reúne 1.203 variáveis por país e ano: indicadores "
        "factuais, avaliações de especialistas agregadas por um modelo bayesiano "
        "de medição, com intervalos de credibilidade, e índices que medem cinco "
        "princípios de democracia (eleitoral, liberal, participativo, deliberativo "
        "e igualitário). Inclui também índices de outras fontes, como Polity e "
        "Freedom House."
    ),
    "description_en": (
        "Varieties of Democracy (V-Dem), from the V-Dem Institute at the "
        "University of Gothenburg, measures democracy in 202 countries and "
        "political units from 1789 to 2025. Version 16 holds 1,203 country-year "
        "variables: factual indicators, expert ratings aggregated by a Bayesian "
        "measurement model with credible intervals, and indices of five "
        "principles of democracy (electoral, liberal, participatory, deliberative "
        "and egalitarian). It also includes indices from other sources, such as "
        "Polity and Freedom House."
    ),
    "description_es": (
        "Varieties of Democracy (V-Dem), del V-Dem Institute de la Universidad de "
        "Gotemburgo, mide la democracia en 202 países y unidades políticas de 1789 "
        "a 2025. La versión 16 reúne 1.203 variables por país y año: indicadores "
        "factuales, calificaciones de expertos agregadas por un modelo bayesiano "
        "de medición, con intervalos de credibilidad, e índices de cinco "
        "principios de la democracia (electoral, liberal, participativo, "
        "deliberativo e igualitario). Incluye también índices de otras fuentes, "
        "como Polity y Freedom House."
    ),
}

RAW_SOURCE = {
    "name_pt": "V-Dem Dataset (V-Dem Institute)",
    "name_en": "V-Dem Dataset (V-Dem Institute)",
    "name_es": "V-Dem Dataset (V-Dem Institute)",
    "url": "https://v-dem.net/data/the-v-dem-dataset/",
    "description_pt": (
        "Página de download do V-Dem, com os arquivos país-ano (Full+Others e "
        "Core), país-data e por codificador, além do livro de códigos e da "
        "metodologia. A versão 16 foi publicada em março de 2026 sob licença "
        "CC BY-SA 4.0. Os mesmos dados país-ano são distribuídos no pacote R "
        "vdemdata (github.com/vdeminstitute/vdemdata), usado nesta carga."
    ),
    "description_en": (
        "V-Dem download page, with the country-year (Full+Others and Core), "
        "country-date and coder-level files, the codebook and the methodology. "
        "Version 16 was released in March 2026 under a CC BY-SA 4.0 license. The "
        "same country-year data ship in the vdemdata R package "
        "(github.com/vdeminstitute/vdemdata), used for this load."
    ),
    "description_es": (
        "Página de descarga de V-Dem, con los archivos país-año (Full+Others y "
        "Core), país-fecha y por codificador, además del libro de códigos y la "
        "metodología. La versión 16 se publicó en marzo de 2026 bajo licencia "
        "CC BY-SA 4.0. Los mismos datos país-año se distribuyen en el paquete R "
        "vdemdata (github.com/vdeminstitute/vdemdata), usado en esta carga."
    ),
}

YEARS = (FIRST_YEAR, LAST_YEAR)
TABLES: dict[str, dict[str, Any]] = {
    "country_year_index": {
        "name_pt": "Índices por país e ano",
        "name_en": "Indices by country-year",
        "name_es": "Índices por país y año",
        "description_pt": (
            "Os 118 índices de alto nível do V-Dem (v2x*), como os de democracia "
            "eleitoral e liberal e os Regimes do Mundo, uma linha por país e ano "
            "de 1789 a 2025, com o projeto, o nome histórico e o código Correlates "
            "of War. Traz só as estimativas pontuais; intervalos e demais variantes "
            "estão na tabela country_year."
        ),
        "description_en": (
            "The 118 high-level V-Dem indices (v2x*), such as electoral and "
            "liberal democracy and Regimes of the World, one row per country and "
            "year from 1789 to 2025, with the coding project, historical name and "
            "Correlates of War code. Point estimates only; intervals and other "
            "variants are in the country_year table."
        ),
        "description_es": (
            "Los 118 índices de alto nivel de V-Dem (v2x*), como los de democracia "
            "electoral y liberal y los Regímenes del Mundo, una fila por país y año "
            "de 1789 a 2025, con el proyecto, el nombre histórico y el código "
            "Correlates of War. Solo trae las estimaciones puntuales; los "
            "intervalos y demás variantes están en la tabla country_year."
        ),
        "levels": {"country": "country_id", "year": "year"},
        "partition": [],
        "coverage": YEARS,
    },
    "country_year": {
        "name_pt": "Variáveis por país e ano",
        "name_en": "Variables by country-year",
        "name_es": "Variables por país y año",
        "description_pt": (
            "Todas as 1.203 variáveis do V-Dem em formato longo, uma linha por "
            "país, ano e variável de 1789 a 2025. Cada linha traz as variantes "
            "publicadas: estimativa pontual ou valor observado, intervalo de 68% e "
            "desvio padrão, versões na escala original e ordinal, média dos "
            "especialistas e número de especialistas. As variáveis estão descritas "
            "na tabela indicator."
        ),
        "description_en": (
            "All 1,203 V-Dem variables in long format, one row per country, year "
            "and variable from 1789 to 2025. Each row carries the published "
            "variants: point estimate or observed value, 68% interval and standard "
            "deviation, original-scale and ordinal versions, the experts' mean and "
            "the number of experts. Variables are described in the indicator table."
        ),
        "description_es": (
            "Todas las 1.203 variables de V-Dem en formato largo, una fila por "
            "país, año y variable de 1789 a 2025. Cada fila trae las variantes "
            "publicadas: estimación puntual o valor observado, intervalo del 68% y "
            "desviación estándar, versiones en la escala original y ordinal, media "
            "de los expertos y número de expertos. Las variables se describen en "
            "la tabla indicator."
        ),
        "levels": {
            "country": "country_id",
            "year": "year",
            "series": "variable_id",
        },
        "partition": ["year"],
        "coverage": YEARS,
    },
    "indicator": {
        "name_pt": "Variáveis",
        "name_en": "Variables",
        "name_es": "Variables",
        "description_pt": (
            "Uma linha por variável do V-Dem, com a entrada do livro de códigos: "
            "nome, tipo, pergunta, opções de resposta, escala, métodos de "
            "agregação e anos cobertos. Os textos do livro de códigos estão em "
            "inglês."
        ),
        "description_en": (
            "One row per V-Dem variable, with its codebook entry: name, type, "
            "question, response options, scale, aggregation methods and years "
            "covered."
        ),
        "description_es": (
            "Una fila por variable de V-Dem, con la entrada del libro de códigos: "
            "nombre, tipo, pregunta, opciones de respuesta, escala, métodos de "
            "agregación y años cubiertos. Los textos del libro de códigos están en "
            "inglés."
        ),
        "levels": {"series": "variable_id"},
        "partition": [],
        "coverage": None,
    },
    "country": {
        "name_pt": "Países",
        "name_en": "Countries",
        "name_es": "Países",
        "description_pt": (
            "Uma linha por país ou unidade política codificada pelo V-Dem, com "
            "código alfabético, nome e períodos de codificação, inclusive "
            "unidades históricas que não existem mais."
        ),
        "description_en": (
            "One row per country or political unit coded by V-Dem, with its "
            "alphabetic code, name and coding periods, including historical units "
            "that no longer exist."
        ),
        "description_es": (
            "Una fila por país o unidad política codificada por V-Dem, con código "
            "alfabético, nombre y períodos de codificación, incluidas unidades "
            "históricas que ya no existen."
        ),
        "levels": {"country": "country_id"},
        "partition": [],
        "coverage": None,
    },
    "dicionario": {
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Correspondência entre as chaves codificadas das colunas do conjunto "
            "e o seu significado."
        ),
        "description_en": (
            "Mapping between the coded keys of the dataset's columns and their "
            "meaning."
        ),
        "description_es": (
            "Correspondencia entre las claves codificadas de las columnas del "
            "conjunto y su significado."
        ),
        "levels": {},
        "partition": [],
        "coverage": None,
    },
}


def read_arch(table: str) -> list[dict[str, str]]:
    """Return a table's architecture rows, in column order."""
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def columns_json(table: str) -> str:
    """Build the bulk_upsert payload for one table from its architecture CSV."""
    out = []
    for r in read_arch(table):
        if not (r["description_en"] and r["description_es"]):
            raise ValueError(f"{table}.{r['name']}: missing a translation")
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
                raise ValueError(
                    f"{table}.{r['name']}: untranslated observation"
                )
            col["observations_pt"] = r["observations"]
            col["observations_en"] = r["observations_en"]
            col["observations_es"] = r["observations_es"]
        out.append(col)
    return json.dumps(out, ensure_ascii=False)


def main() -> None:
    """Register (or update) the dataset, raw source and the five tables."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--publish", action="store_true")
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    for t in TABLE_ORDER:
        print(f"{t}: {len(json.loads(columns_json(t)))} columns")

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
    entity = {e: ids["entity"][e] for e in ("country", "year", "series")}
    print(
        f"env={env} org={org_id} themes={theme_ids} tags={len(tag_ids)} "
        f"area={area_id} languages={language_ids} entities={entity}"
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

    # Reuse the pre-existing source (it points at the v10 page) rather than
    # adding a second one: a table with two raw sources cannot run a pipeline.
    prior_sources = bd_mcp_write.get_raw_data_sources(
        dataset_slug=DATASET_SLUG, env=env
    )
    if len(prior_sources) > 1:
        raise RuntimeError(f"expected at most one raw source: {prior_sources}")
    source = bd_mcp_write.create_update_raw_data_source(
        dataset_id=dataset_id,
        **RAW_SOURCE,
        license_id=ids["license"][LICENSE_SLUG],
        availability_id=ids["availability"]["online"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        language_ids=language_ids,
        status_id=status_published,
        id=prior_sources[0]["id"] if prior_sources else None,
        env=env,
    )
    print(f"raw source -> {source['id']}")

    # latest is a DateTime; a naive value is read as Sao Paulo time, so pass an offset
    today = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat()
    for table in TABLE_ORDER:
        spec = TABLES[table]
        prior = (
            bd_mcp_metadata.get_dataset(slug=DATASET_SLUG, env=env)
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
        if ol_ids:
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

        # Observation-level links and the partition flag, in one call per column:
        # update_column's booleans default to False, so separate calls clobber.
        cols = {
            c["name"]: c["id"]
            for c in bd_mcp_metadata.get_dataset(slug=DATASET_SLUG, env=env)[
                "tables"
            ][table]["columns"]
        }
        flagged = set(spec["levels"].values()) | set(spec["partition"])
        level_of = {col: ent for ent, col in spec["levels"].items()}
        for col in flagged:
            kwargs = {}
            if col in level_of:
                kwargs["observation_level_id"] = ol_ids[level_of[col]]
            bd_mcp_write.update_column(
                column_id=cols[col],
                column_name=col,
                table_id=table_id,
                is_partition=col in spec["partition"],
                env=env,
                **kwargs,
            )
        print(f"  flagged {sorted(flagged)}")

        prior_ct = prior.get("cloud_tables", [])
        bd_mcp_write.create_update_cloud_table(
            table_id=table_id,
            gcp_project_id=gcp_project,
            gcp_dataset_id=GCP_DATASET_ID,
            gcp_table_id=table,
            id=prior_ct[0]["id"] if prior_ct else None,
            env=env,
        )

        if spec["coverage"]:
            first, last = spec["coverage"]
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
            **common, raw_data_source_ids=[source["id"]], id=table_id
        )
        print(
            f"  cloud table, coverage {spec['coverage']}, update and source linked"
        )

    bd_mcp_write.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
