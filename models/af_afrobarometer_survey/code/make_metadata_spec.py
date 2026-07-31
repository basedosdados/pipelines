#!/usr/bin/env python3
"""Emit metadata_spec.json: everything the metadata-registration step needs
(reference IDs + per-table names/descriptions/coverage/raw-source URLs),
computed from profile_cache.json + common.ROUNDS. English-source descriptions,
translated to PT/ES with consistent terminology.

Usage: .venv/bin/python models/af_afrobarometer_survey/code/make_metadata_spec.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import ROUNDS

CODE = Path(__file__).resolve().parent
prof = json.loads((CODE / "profile_cache.json").read_text())

REF = {
    "env": "staging",
    "account_id": "57",
    "dataset_slug": "afrobarometer",
    "dataset_id": "292a33be-2f98-4fc1-8c6b-7d3c3f167047",
    "gcp_project_id": "basedosdados-dev",
    "gcp_dataset_id": "af_afrobarometer_survey",
    "org_id": "6eb24939-f6c4-4200-9a1f-c363e9d80104",
    "status_published": "e16221de-ac30-4926-83d3-de219998dab3",
    "theme_politics": "c4e75f8c-29de-4b76-9607-657d4ac7f490",
    "theme_government": "6dd730bb-89ab-4dba-a1bf-a25ca1c35003",
    "tag_barometro": "d8789974-425f-4525-9bdd-2c52b9ab1df0",
    "area_af": "45553ff0-fcbd-4a2f-87d9-e62ed7b1f8c9",
    "entity_person": "b4e76213-888b-40ea-b877-d82ce76d71a2",
    "entity_year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "license_unknown": "77dfe32b-6a14-4490-806f-22af1f26c425",
    "availability_online": "dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe",
}

# keep the existing (well-written) dataset descriptions verbatim
DATASET = {
    "name_pt": "Afrobarometer",
    "name_en": "Afrobarometer",
    "name_es": "Afrobarómetro",
    "description_pt": (
        "O Afrobarometer é uma instituição de pesquisa "
        "pan-africana não partidária que realiza pesquisas de atitude pública "
        "sobre democracia, governança, economia e sociedade em mais de 30 "
        "países, repetidas em um ciclo regular. Somos a principal fonte mundial "
        "de dados de alta qualidade sobre o que os africanos estão pensando."
    ),
    "description_en": (
        "Afrobarometer is a non-partisan pan-African research "
        "institution that conducts public attitude surveys on democracy, "
        "governance, economy and society in over 30 countries, repeated on a "
        "regular cycle. We are the world's leading source of high-quality data "
        "on what Africans are thinking."
    ),
    "description_es": (
        "Afrobarómetro es una institución de investigación "
        "panafricana no partidista que realiza encuestas de opinión pública "
        "sobre democracia, gobernanza, economía y sociedad en más de 30 países, "
        "repetidas en un ciclo regular. Somos la principal fuente mundial de "
        "datos de alta calidad sobre lo que piensan los africanos."
    ),
}


def yr(c):
    a, b = c
    return str(a) if a == b else f"{a}-{b}"


def round_row(rnd):
    slug = rnd["slug"]
    n = rnd["num"]
    p = prof[slug]
    cov = p["coverage"]
    nc = p["n_countries"]
    y = yr(cov)
    return {
        "slug": slug,
        "num": n,
        "coverage": cov,
        "n_countries": nc,
        "raw_url": rnd["url"],
        "arch_csv": f"models/af_afrobarometer_survey/code/architecture/{slug}.csv",
        "name_pt": f"Rodada {n} ({y})",
        "name_en": f"Round {n} ({y})",
        "name_es": f"Ronda {n} ({y})",
        "description_pt": (
            f"Microdados da Rodada {n} do Afrobarometer ({y}), uma linha por "
            f"respondente em {nc} países africanos, sobre atitudes públicas "
            f"em relação à democracia, governança, economia e sociedade. Os "
            f"códigos originais das variáveis do Afrobarometer são preservados "
            f"como nomes de colunas; os valores codificados são documentados "
            f"na tabela dicionario."
        ),
        "description_en": (
            f"Afrobarometer merged Round {n} survey microdata ({y}), one row "
            f"per respondent across {nc} African countries, on public attitudes "
            f"toward democracy, governance, the economy and society. Original "
            f"Afrobarometer variable codes are preserved as column names; coded "
            f"values are documented in the dicionario table."
        ),
        "description_es": (
            f"Microdatos de la Ronda {n} del Afrobarómetro ({y}), una fila por "
            f"encuestado en {nc} países africanos, sobre actitudes públicas "
            f"hacia la democracia, la gobernanza, la economía y la sociedad. "
            f"Los códigos originales de las variables del Afrobarómetro se "
            f"conservan como nombres de columnas; los valores codificados se "
            f"documentan en la tabla dicionario."
        ),
        "raw_name_pt": f"Dados mesclados da Rodada {n} (SPSS)",
        "raw_name_en": f"Merged Round {n} data (SPSS)",
        "raw_name_es": f"Datos combinados de la Ronda {n} (SPSS)",
    }


rounds = [round_row(r) for r in ROUNDS]
overall = [
    min(r["coverage"][0] for r in rounds),
    max(r["coverage"][1] for r in rounds),
]
dicionario = {
    "slug": "dicionario",
    "coverage": overall,
    "arch_csv": "models/af_afrobarometer_survey/code/architecture/dicionario.csv",
    "name_pt": "Dicionário",
    "name_en": "Dictionary",
    "name_es": "Diccionario",
    "description_pt": (
        "Dicionário de rótulos de valores das tabelas round1 a "
        "round9: para cada tabela e coluna, o significado de cada valor "
        "codificado presente nos microdados."
    ),
    "description_en": (
        "Value-label dictionary for tables round1 through round9: "
        "for each table and column, the meaning of each coded value present in "
        "the microdata."
    ),
    "description_es": (
        "Diccionario de etiquetas de valores para las tablas "
        "round1 a round9: para cada tabla y columna, el significado de cada "
        "valor codificado presente en los microdatos."
    ),
}

spec = {
    "ref": REF,
    "dataset": DATASET,
    "rounds": rounds,
    "dicionario": dicionario,
    "overall_coverage": overall,
}
(CODE / "metadata_spec.json").write_text(
    json.dumps(spec, ensure_ascii=False, indent=1)
)
print(
    f"wrote metadata_spec.json: {len(rounds)} rounds + dicionario; "
    f"overall coverage {yr(overall)}"
)
