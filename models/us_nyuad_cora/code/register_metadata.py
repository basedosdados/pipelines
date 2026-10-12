"""Register us_nyuad_cora (backend slug `cora`) metadata in the Data Basis backend.

Idempotent: every record is looked up first and its id passed back on update,
because create_update_* duplicates observation levels, cloud tables, coverages,
datetime ranges and updates when called without one.

Column specs come from the architecture CSV. Its observations are written in
Portuguese; OBSERVATIONS below supplies English and Spanish, and the script fails
rather than register a column whose note has no translation.

The dataset is registered `under_review`. `--publish` flips it to `published`:
safe on dev/staging, which are not the public site; on prod only after the PR has
merged, table-approve has materialised the tables, and they are verified.

Usage (from the repo root, with an interpreter that has databasis_mcp):
    PYTHONPATH=.:<mcp>/src python models/us_nyuad_cora/code/register_metadata.py \
        [--env dev|staging|prod] [--dry-run] [--publish] [--aux-url]
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
DATASET_SLUG = "cora"
GCP_DATASET_ID = "us_nyuad_cora"
ORG_SLUG = "nyu"
THEME_SLUGS = ["politics", "history"]
AREA_SLUG = "us"
LICENSE_SLUG = "cc0"
FIRST_DATE, LAST_DATE = dt.date(1873, 3, 4), dt.date(2026, 3, 16)
AUX_URL = (
    "https://storage.googleapis.com/basedosdados-public/auxiliary_files/"
    "us_nyuad_cora/speech/auxiliary_files.zip"
)

# Subject tags, English slugs on prod. No place, theme or organization tags.
TAG_SLUGS = [
    "congress",
    "legislative",
    "speech",
    "text-analysis",
    "political_party",
    "bill",
    "legislation",
]
# The staging backend is an older snapshot whose tag vocabulary is still Portuguese.
STAGING_TAG_SLUGS = {
    "congress": "congresso",
    "legislative": "legislativo",
    "speech": "discurso",
    "text-analysis": "analise_de_texto",
    "political_party": "partido",
    "bill": "lei",
    "legislation": "legislacao",
}

TABLE_ORDER = ["speech"]

DATASET = {
    "name_pt": "Congressional Oratory Research Archive (CORA)",
    "name_en": "Congressional Oratory Research Archive (CORA)",
    "name_es": "Congressional Oratory Research Archive (CORA)",
    "description_pt": (
        "O Congressional Oratory Research Archive (CORA), elaborado por Shahid "
        "Rabbani e Aaron R. Kaufman na New York University Abu Dhabi, reúne "
        "15.215.977 discursos do Congressional Record dos Estados Unidos de "
        "1873 a março de 2026 (43ª a 119ª legislaturas). Cada discurso traz o "
        "texto integral, a câmara, a data, o orador com seu identificador "
        "Bioguide, partido, estado e gênero, os projetos de lei e resoluções "
        "citados e um tema do Comparative Agendas Project atribuído por modelo."
    ),
    "description_en": (
        "The Congressional Oratory Research Archive (CORA), compiled by Shahid "
        "Rabbani and Aaron R. Kaufman at New York University Abu Dhabi, holds "
        "15,215,977 speeches from the United States Congressional Record from "
        "1873 to March 2026 (43rd to 119th Congresses). Each speech carries its "
        "full text, chamber, date, the speaker with Bioguide identifier, party, "
        "state and gender, the bills and resolutions it cites, and a "
        "model-assigned Comparative Agendas Project topic."
    ),
    "description_es": (
        "El Congressional Oratory Research Archive (CORA), elaborado por Shahid "
        "Rabbani y Aaron R. Kaufman en la New York University Abu Dhabi, reúne "
        "15.215.977 discursos del Congressional Record de Estados Unidos de 1873 "
        "a marzo de 2026 (43.ª a 119.ª legislaturas). Cada discurso incluye el "
        "texto completo, la cámara, la fecha, el orador con su identificador "
        "Bioguide, partido, estado y género, los proyectos de ley y resoluciones "
        "citados y un tema del Comparative Agendas Project asignado por modelo."
    ),
}

RAW_SOURCE = {
    "name_pt": "CORA: U.S. Congressional Record Speeches, 1873-2025 (figshare)",
    "name_en": "CORA: U.S. Congressional Record Speeches, 1873-2025 (figshare)",
    "name_es": "CORA: U.S. Congressional Record Speeches, 1873-2025 (figshare)",
    "url": "https://doi.org/10.6084/m9.figshare.33321423",
    "description_pt": (
        "Página do conjunto no figshare (doi:10.6084/m9.figshare.33321423), com "
        "um arquivo zip de 4,1 GB contendo um arquivo JSON Lines por ano. A "
        "versão 1 foi publicada em 2026-08-24 sob licença CC0 e acompanha um "
        "artigo na Scientific Data."
    ),
    "description_en": (
        "figshare page of the dataset (doi:10.6084/m9.figshare.33321423), with a "
        "4.1 GB zip archive holding one JSON Lines file per year. Version 1 was "
        "released on 2026-08-24 under a CC0 license and accompanies an article "
        "in Scientific Data."
    ),
    "description_es": (
        "Página del conjunto en figshare (doi:10.6084/m9.figshare.33321423), con "
        "un archivo zip de 4,1 GB que contiene un archivo JSON Lines por año. La "
        "versión 1 se publicó el 2026-08-24 bajo licencia CC0 y acompaña un "
        "artículo en Scientific Data."
    ),
}

TABLES = {
    "speech": {
        "name_pt": "Discursos",
        "name_en": "Speeches",
        "name_es": "Discursos",
        "description_pt": (
            "Uma linha por discurso atribuído no Congressional Record, de 1873 a "
            "março de 2026, com o texto integral, metadados da sessão, "
            "identificação do orador, citações legislativas e tema do "
            "Comparative Agendas Project. A chave é speech_id."
        ),
        "description_en": (
            "One row per attributed speech in the Congressional Record, 1873 to "
            "March 2026, with full text, session metadata, speaker "
            "identification, legislative citations and Comparative Agendas "
            "Project topic. The key is speech_id."
        ),
        "description_es": (
            "Una fila por discurso atribuido en el Congressional Record, de 1873 "
            "a marzo de 2026, con el texto completo, metadatos de la sesión, "
            "identificación del orador, citas legislativas y tema del "
            "Comparative Agendas Project. La clave es speech_id."
        ),
        "levels": {
            "speech": "speech_id",
            "person": "bioguide_id",
            "date": "date",
        },
        "partition": ["year"],
    },
}

# Column name -> (English, Spanish) rendering of its Portuguese observation.
OBSERVATIONS = {
    "year": ("Derived from date", "Derivado de date"),
    "congress": (
        "Ordinal number of the Congress; each Congress lasts two years",
        "Número ordinal de la legislatura; cada legislatura dura dos años",
    ),
    "session": (
        "Null in 1452880 speeches in the source",
        "Nulo en 1452880 discursos en la fuente",
    ),
    "chamber": (
        "Extension is the Extensions of Remarks section",
        "Extension corresponde a la sección Extensions of Remarks",
    ),
    "speech_id": (
        "Year followed by a sequence number within the year; unique across the table",
        "Año seguido de un número secuencial dentro del año; único en toda la tabla",
    ),
    "bioguide_id": (
        "Filled for 47.5% of speeches; null when CORA did not link the speaker to a member of Congress. The source value None (7,665 speeches) was set to null, and R000606R (12 speeches) corrected to R000606",
        "Completado en el 47,5% de los discursos; nulo cuando CORA no vinculó al orador con un congresista. El valor None de la fuente (7.665 discursos) se convirtió en nulo, y R000606R (12 discursos) se corrigió a R000606",
    ),
    "state_id": (
        "Derived from state_abbreviation; null for the abbreviations US and DK, which have no FIPS code",
        "Derivado de state_abbreviation; nulo para las siglas US y DK, que no tienen código FIPS",
    ),
    "state_abbreviation": (
        "Source value; includes US and DK (Dakota Territory)",
        "Valor de la fuente; incluye US y DK (Territorio de Dakota)",
    ),
    "speaker_name_raw": (
        "Includes title and OCR errors (e.g. Mr. CONKLINO)",
        "Incluye tratamiento y errores de OCR (p. ej. Mr. CONKLINO)",
    ),
    "party": (
        "Source codes decoded: D = Democratic, R = Republican, I = Independent; other values kept as in the source",
        "Códigos de la fuente decodificados: D = Democratic, R = Republican, I = Independent; los demás valores se mantienen como en la fuente",
    ),
    "gender": (
        "Source codes decoded: M = Male, F = Female",
        "Códigos de la fuente decodificados: M = Male, F = Female",
    ),
    "cap_major_topic": (
        "Assigned by a classification model trained by the authors; State Government Operations holds 57% of speeches",
        "Asignado por un modelo de clasificación entrenado por los autores; State Government Operations concentra el 57% de los discursos",
    ),
    "speech_text": (
        "OCR text of the bound Congressional Record up to 1993 and GovInfo text from 1994; contains OCR errors",
        "Texto por OCR del Congressional Record encuadernado hasta 1993 y de GovInfo desde 1994; contiene errores de OCR",
    ),
    "bills": (
        "Extracted by the authors with regular expressions. Lists with more than 1000 items were set to null: they result from expanding number ranges misread by OCR (up to 55 million items in a single speech)",
        "Extraídos por los autores con expresiones regulares. Las listas con más de 1000 elementos se anularon: resultan de expandir rangos de números mal leídos por OCR (hasta 55 millones de elementos en un solo discurso)",
    ),
    "joint_resolutions": (
        "Extracted by the authors with regular expressions. Lists with more than 1000 items were set to null",
        "Extraídas por los autores con expresiones regulares. Las listas con más de 1000 elementos se anularon",
    ),
    "concurrent_resolutions": (
        "Extracted by the authors with regular expressions. Lists with more than 1000 items were set to null",
        "Extraídas por los autores con expresiones regulares. Las listas con más de 1000 elementos se anularon",
    ),
    "simple_resolutions": (
        "Extracted by the authors with regular expressions. Lists with more than 1000 items were set to null",
        "Extraídas por los autores con expresiones regulares. Las listas con más de 1000 elementos se anularon",
    ),
    "source_url": (
        "congress.gov up to 1993 and govinfo.gov from 1994",
        "congress.gov hasta 1993 y govinfo.gov desde 1994",
    ),
}


def translate(name: str) -> tuple[str, str]:
    """Return the English and Spanish renderings of a column's observation."""
    if name not in OBSERVATIONS:
        raise KeyError(f"no translation for the observation of {name!r}")
    return OBSERVATIONS[name]


def read_arch(table: str) -> list[dict[str, str]]:
    """Return a table's architecture rows, in column order."""
    with open(ARCH_DIR / f"{table}.csv", newline="") as f:
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
            en, es = translate(r["name"])
            col["observations_pt"] = r["observations"]
            col["observations_en"] = en
            col["observations_es"] = es
        out.append(col)
    return json.dumps(out, ensure_ascii=False)


def main() -> None:
    """Register (or update) the dataset, raw source and both tables."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument(
        "--aux-url",
        action="store_true",
        help="set auxiliary_files_url on speech (only once it returns 200)",
    )
    args = ap.parse_args()
    env = args.env
    gcp_project = "basedosdados" if env == "prod" else "basedosdados-dev"

    for t in TABLE_ORDER:
        n = len(
            json.loads(columns_json(t))
        )  # also validates every translation
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
    lang = ids["language"]
    english = lang.get("en") or lang.get("english") or lang.get("ingles")
    account_id = bd_mcp_metadata.get_authenticated_account(env=env)["id"]
    entity = {
        e: ids["entity"][e] for e in ("speech", "person", "date", "year")
    }
    print(
        f"env={env} org={org_id} themes={theme_ids} tags={len(tag_ids)} "
        f"area={area_id} english={english} entities={entity}"
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
    source = bd_mcp_write.create_update_raw_data_source(
        dataset_id=dataset_id,
        **RAW_SOURCE,
        license_id=ids["license"][LICENSE_SLUG],
        availability_id=ids["availability"]["online"],
        has_structured_data=True,
        is_free=True,
        contains_api=False,
        requires_registration=False,
        language_ids=[english] if english else None,
        status_id=status_published,
        id=prior_sources.get(RAW_SOURCE["url"]),
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
        names = {
            k: spec[k]
            for k in (
                "name_pt", "name_en", "name_es",
                "description_pt", "description_en", "description_es",
            )
        }  # fmt: skip
        aux = AUX_URL if args.aux_url else ""
        common: dict[str, Any] = {
            "slug": table,
            **names,
            "dataset_id": dataset_id,
            "status_id": status_published,
            "published_by_ids": [account_id],
            "data_cleaned_by_ids": [account_id],
            "auxiliary_files_url": aux,
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
            start_year=FIRST_DATE.year,
            start_month=FIRST_DATE.month,
            start_day=FIRST_DATE.day,
            end_year=LAST_DATE.year,
            end_month=LAST_DATE.month,
            end_day=LAST_DATE.day,
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
            f"  cloud table, coverage {FIRST_DATE}..{LAST_DATE}, update and raw source linked"
        )

    bd_mcp_write.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
