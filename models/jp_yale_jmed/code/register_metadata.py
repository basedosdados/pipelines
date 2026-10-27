"""Register jp_yale_jmed (backend slug `jmed`) metadata in the Data Basis backend.

Idempotent: every record is looked up first and its id passed back on update,
because create_update_* duplicates observation levels, cloud tables, coverages,
datetime ranges and updates when called without one.

Column specs come from the architecture CSVs. Their observations are written in
Portuguese; OBSERVATIONS below supplies English and Spanish, and the script fails
rather than register a column whose note has no translation.

The dataset is registered `under_review`. `--publish` flips it to `published`:
safe on dev/staging, which are not the public site; on prod only after the PR has
merged, table-approve has materialised the tables, and they are verified.

Usage (from the repo root, with an interpreter that has databasis_mcp):
    PYTHONPATH=.:<mcp>/src python models/jp_yale_jmed/code/register_metadata.py \
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
DATASET_SLUG = "jmed"
GCP_DATASET_ID = "jp_yale_jmed"
ORG_SLUG = "yale_university"
THEME_SLUGS = ["politics", "government"]
AREA_SLUG = "jp"
LICENSE_SLUG = "cc_by"
LANGUAGES = ["ja", "en"]
AUX_URL = (
    "https://storage.googleapis.com/basedosdados-public/auxiliary_files/"
    "jp_yale_jmed/{table}/auxiliary_files.zip"
)

# Subject tags, English slugs on prod. No place, theme or organization tags.
TAG_SLUGS = [
    "elections",
    "candidacy",
    "vote",
    "political_party",
    "municipality",
    "representativeness",
    "gender",
]
# The staging backend is an older snapshot whose tag vocabulary is still Portuguese.
STAGING_TAG_SLUGS = {
    "elections": "eleicao",
    "candidacy": "candidatura",
    "vote": "voto",
    "political_party": "partido",
    "municipality": "municipio",
    "representativeness": "representatividade",
    "gender": "genero",
}

TABLE_ORDER = ["candidate", "municipal_history", "dicionario"]

DATASET = {
    "name_pt": "Japanese Municipal Elections Dataset (JMED)",
    "name_en": "Japanese Municipal Elections Dataset (JMED)",
    "name_es": "Japanese Municipal Elections Dataset (JMED)",
    "description_pt": (
        "O Japanese Municipal Elections Dataset (JMED), elaborado por Charles T. "
        "McClean na Yale University, reúne 223.561 candidaturas em 21.111 "
        "eleições para assembleias municipais e prefeituras em 2.027 municípios "
        "do Japão de 2005 a 2025. Cada candidatura traz nome, idade, sexo, "
        "partido, situação de incumbência, votos e resultado, além do "
        "comparecimento e das eleições sem disputa. Um conjunto complementar "
        "registra as fusões, incorporações, desmembramentos e mudanças de nome, "
        "código e limites dos municípios no período."
    ),
    "description_en": (
        "The Japanese Municipal Elections Dataset (JMED), compiled by Charles T. "
        "McClean at Yale University, holds 223,561 candidacies in 21,111 "
        "municipal assembly and mayoral elections across 2,027 Japanese "
        "municipalities from 2005 to 2025. Each candidacy carries the candidate's "
        "name, age, sex, party, incumbency status, vote total and outcome, along "
        "with turnout and uncontested elections. A companion dataset records the "
        "mergers, absorptions, splits and changes to names, codes and boundaries "
        "of municipalities over the period."
    ),
    "description_es": (
        "El Japanese Municipal Elections Dataset (JMED), elaborado por Charles T. "
        "McClean en la Yale University, reúne 223.561 candidaturas en 21.111 "
        "elecciones de asambleas municipales y alcaldías en 2.027 municipios de "
        "Japón de 2005 a 2025. Cada candidatura incluye nombre, edad, sexo, "
        "partido, situación de titularidad, votos y resultado, además de la "
        "participación y las elecciones sin competencia. Un conjunto "
        "complementario registra las fusiones, absorciones, divisiones y cambios "
        "de nombre, código y límites de los municipios en el período."
    ),
}

RAW_SOURCE = {
    "name_pt": "Japanese Municipal Elections Dataset (JMED) (Harvard Dataverse)",
    "name_en": "Japanese Municipal Elections Dataset (JMED) (Harvard Dataverse)",
    "name_es": "Japanese Municipal Elections Dataset (JMED) (Harvard Dataverse)",
    "url": "https://doi.org/10.7910/DVN/E9ZUNV",
    "description_pt": (
        "Página do conjunto no Harvard Dataverse (doi:10.7910/DVN/E9ZUNV), com os "
        "arquivos CSV e RDS do JMED e do Municipal History Dataset, o livro de "
        "códigos em PDF e um README. A versão 1.0 foi publicada em 2026-10-23 "
        "sob licença CC BY 4.0."
    ),
    "description_en": (
        "Harvard Dataverse page of the dataset (doi:10.7910/DVN/E9ZUNV), with CSV "
        "and RDS files for JMED and the Municipal History Dataset, a PDF codebook "
        "and a README. Version 1.0 was released on 2026-10-23 under a CC BY 4.0 "
        "license."
    ),
    "description_es": (
        "Página del conjunto en Harvard Dataverse (doi:10.7910/DVN/E9ZUNV), con "
        "archivos CSV y RDS de JMED y del Municipal History Dataset, el libro de "
        "códigos en PDF y un README. La versión 1.0 se publicó el 2026-10-23 "
        "bajo licencia CC BY 4.0."
    ),
}

TABLES: dict[str, dict[str, Any]] = {
    "candidate": {
        "name_pt": "Candidatos",
        "name_en": "Candidates",
        "name_es": "Candidatos",
        "description_pt": (
            "Uma linha por candidato em cada eleição para assembleia municipal ou "
            "prefeitura no Japão, de 2005 a 2025, com dados do município, da "
            "eleição e do distrito, características do candidato, votos e "
            "resultado. A chave é election_id e candidate_id."
        ),
        "description_en": (
            "One row per candidate per municipal assembly or mayoral election in "
            "Japan, 2005 to 2025, with municipality, election and district "
            "details, candidate characteristics, votes and outcome. The key is "
            "election_id and candidate_id."
        ),
        "description_es": (
            "Una fila por candidato en cada elección de asamblea municipal o "
            "alcaldía en Japón, de 2005 a 2025, con datos del municipio, de la "
            "elección y del distrito, características del candidato, votos y "
            "resultado. La clave es election_id y candidate_id."
        ),
        "levels": {
            "election": "election_id",
            "person": "candidate_id",
            "municipality": "municipality_id",
        },
        "partition": ["year"],
        "coverage": (dt.date(2005, 1, 16), dt.date(2025, 12, 28)),
        "aux": True,
    },
    "municipal_history": {
        "name_pt": "Histórico municipal",
        "name_en": "Municipal history",
        "name_es": "Historial municipal",
        "description_pt": (
            "Uma linha por ligação entre um município antecessor e um sucessor "
            "em cada fusão, incorporação, desmembramento ou mudança de nome, "
            "código ou limites no Japão, de 2005 a 2025, com os identificadores "
            "do JMED."
        ),
        "description_en": (
            "One row per link between a predecessor and a successor municipality "
            "in each merger, absorption, split, or change of name, code or "
            "boundaries in Japan, 2005 to 2025, using JMED identifiers."
        ),
        "description_es": (
            "Una fila por vínculo entre un municipio antecesor y uno sucesor en "
            "cada fusión, absorción, división o cambio de nombre, código o "
            "límites en Japón, de 2005 a 2025, con los identificadores de JMED."
        ),
        "levels": {"municipality": "predecessor_id", "date": "change_date"},
        "partition": [],
        "coverage": (dt.date(2005, 1, 1), dt.date(2025, 5, 30)),
        "aux": True,
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
        "aux": False,
    },
}

# Column name -> (English, Spanish) rendering of its Portuguese observation.
OBSERVATIONS = {
    "municipality_id": (
        "Linked to territorial changes through the municipal_history table",
        "Vinculado a los cambios territoriales mediante la tabla municipal_history",
    ),
    "municipality_code": (
        "May change over time for the same municipality_id; see municipal_history",
        "Puede cambiar con el tiempo para el mismo municipality_id; ver municipal_history",
    ),
    "prefecture_code": (
        "Zero-padded; the source publishes the code as an integer (1 to 47)",
        "Completado con cero a la izquierda; la fuente publica el código como entero (1 a 47)",
    ),
    "candidate_id": (
        "The two digits after C identify the prefecture where the candidate first entered the dataset",
        "Los dos dígitos después de C identifican la prefectura en que el candidato ingresó por primera vez al conjunto de datos",
    ),
    "district": (
        "Null for 1.35% of candidates in the source",
        "Nulo para el 1,35% de los candidatos en la fuente",
    ),
    "turnout": (
        "Available for contested elections only",
        "Disponible solo para elecciones con competencia",
    ),
    "candidate_name": (
        "The listed name can vary across elections for the same person",
        "El nombre registrado puede variar entre elecciones para la misma persona",
    ),
    "party_en": (
        "Parties and groups without a translation appear as [untranslated party/group]",
        "Los partidos y grupos sin traducción aparecen como [untranslated party/group]",
    ),
    "incumbent": (
        "Includes sitting officeholders (現職) and immediately previous officeholders (前職) whose seat lapsed before polling through dissolution, term expiration, merger or resignation",
        "Incluye a los ocupantes actuales (現職) y a los ocupantes inmediatamente anteriores (前職) cuyo escaño caducó antes de la votación por disolución, fin del mandato, fusión o renuncia",
    ),
    "number_of_votes": (
        "Fractional values reflect the proportional allocation of ballots with ambiguous names (按分票)",
        "Los valores fraccionarios reflejan la asignación proporcional de boletas con nombres ambiguos (按分票)",
    ),
    "vote_share": (
        "Rounded to two decimal places in the source",
        "Redondeado a dos decimales en la fuente",
    ),
    "elected": (
        "Includes uncontested winners; minimum-vote requirements can leave seats unfilled",
        "Incluye a los ganadores sin competencia; los requisitos de votación mínima pueden dejar escaños vacantes",
    ),
    "predecessor_id": (
        "Matches municipality_id in the candidate table",
        "Corresponde a municipality_id en la tabla candidate",
    ),
    "successor_id": (
        "Matches municipality_id in the candidate table",
        "Corresponde a municipality_id en la tabla candidate",
    ),
    "change_type": (
        "For name, code and boundary changes, predecessor and successor are the same continuing municipality",
        "En los cambios de nombre, código y límites, antecesor y sucesor son el mismo municipio continuo",
    ),
}


def translate(name: str) -> tuple[str, str]:
    """Return the English and Spanish renderings of a column's observation."""
    if name not in OBSERVATIONS:
        raise KeyError(f"no translation for the observation of {name!r}")
    return OBSERVATIONS[name]


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
            en, es = translate(r["name"])
            col["observations_pt"] = r["observations"]
            col["observations_en"] = en
            col["observations_es"] = es
        out.append(col)
    return json.dumps(out, ensure_ascii=False)


def main() -> None:
    """Register (or update) the dataset, raw source and the three tables."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument(
        "--aux-url",
        action="store_true",
        help="set auxiliary_files_url (only once each URL returns 200)",
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
    language_ids = [ids["language"][lang] for lang in LANGUAGES]
    account_id = bd_mcp_metadata.get_authenticated_account(env=env)["id"]
    entity = {
        e: ids["entity"][e]
        for e in ("election", "person", "municipality", "date", "year")
    }
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
        contains_api=True,
        requires_registration=False,
        language_ids=language_ids,
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
        aux = (
            AUX_URL.format(table=table) if args.aux_url and spec["aux"] else ""
        )
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
                start_year=first.year,
                start_month=first.month,
                start_day=first.day,
                end_year=last.year,
                end_month=last.month,
                end_day=last.day,
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
            f"  cloud table, coverage {spec['coverage']}, update and raw source linked"
        )

    bd_mcp_write.reorder_tables(
        dataset_slug=DATASET_SLUG, table_slugs=TABLE_ORDER, env=env
    )
    print(f"\n=== METADATA REGISTRATION COMPLETE (env={env}) ===")


if __name__ == "__main__":
    main()
