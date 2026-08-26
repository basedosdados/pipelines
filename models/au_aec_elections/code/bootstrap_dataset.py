"""Create the au_aec_elections organization, dataset and raw data sources.

Run once per backend before ``register_metadata.py``, which registers the tables into
an already-existing dataset.

    uv run --with fastmcp python models/au_aec_elections/code/bootstrap_dataset.py --env prod

**Slugs differ between backends and must be resolved per environment.** Staging names
Australian agencies `au_abs` / `au_ato` / `au_rba` and tags in Portuguese; prod drops
the country prefix (`abs`, `ato`, `rba`) and uses English tags. The organization
records are genuinely distinct — different UUIDs, not one record with two slugs — so
nothing here may be copied across by id.

The dataset is created as `under_review`, which hides it from the production
frontend until it is deliberately published.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

MCP_DIR = Path.home() / "Dropbox" / "BD" / "mcp"
sys.path.insert(0, str(MCP_DIR))

import server  # noqa: E402  — the databasis MCP module

# Organization slug per backend, following each one's own convention.
ORG_SLUG = {"prod": "aec", "staging": "au_aec", "dev": "au_aec"}

# Tag slug per backend. Same subject matter, different vocabulary.
TAG_SLUGS = {
    "prod": [
        "elections",
        "vote",
        "candidacy",
        "political_party",
        "donation",
        "campaign-finance",
        "electoral-system",
        "transparency",
        "parliament",
        "referendum",
    ],
    "staging": [
        "eleicao",
        "voto",
        "candidatura",
        "partido",
        "doacao",
        "campaign-finance",
        "sistema_eleitoral",
        "transparencia",
        "parlamento",
        "referendum",
    ],
}

# Tags that may not exist yet on a given backend. Slug is English and kebab-case;
# names are lowercase in all three languages.
CREATABLE_TAGS = {
    "referendum": ("referendo", "referendum", "referendo"),
}

DATASET = {
    "slug": "elections",
    "name_pt": "Eleições Federais e Transparência do Financiamento Político",
    "name_en": "Federal Elections and Political Finance Disclosures",
    "name_es": "Elecciones Federales y Transparencia del Financiamiento Político",
    "description_pt": (
        "Resultados das eleições federais australianas apurados pela Comissão "
        "Eleitoral Australiana (AEC), para a Câmara dos Representantes e o Senado, no "
        "nível da divisão eleitoral e do local de votação individual. Cobre as oito "
        "eleições gerais de 2004 a 2025, 24 eleições suplementares, a eleição de "
        "Senado da Austrália Ocidental de 2014 e o referendo de 2023. Inclui também "
        "as declarações de financiamento político do Transparency Register da AEC — "
        "doações, recebimentos, despesas eleitorais e totais das declarações anuais — "
        "dos exercícios financeiros de 1998-99 a 2024-25."
    ),
    "description_en": (
        "Australian federal election results counted by the Australian Electoral "
        "Commission (AEC), for the House of Representatives and the Senate, at both "
        "electoral division and individual polling place level. Covers the eight "
        "general elections from 2004 to 2025, 24 by-elections, the 2014 Western "
        "Australia Senate election and the 2023 referendum. Also includes the "
        "political finance disclosures from the AEC Transparency Register — "
        "donations, receipts, electoral expenditure and annual return totals — for "
        "financial years 1998-99 to 2024-25."
    ),
    "description_es": (
        "Resultados de las elecciones federales australianas escrutados por la "
        "Comisión Electoral Australiana (AEC), para la Cámara de Representantes y el "
        "Senado, a nivel de división electoral y de local de votación individual. "
        "Cubre las ocho elecciones generales de 2004 a 2025, 24 elecciones parciales, "
        "la elección de Senado de Australia Occidental de 2014 y el referendo de "
        "2023. Incluye además las declaraciones de financiamiento político del "
        "Transparency Register de la AEC — donaciones, ingresos, gastos electorales y "
        "totales de las declaraciones anuales — de los ejercicios financieros 1998-99 "
        "a 2024-25."
    ),
}

ORGANIZATION = {
    "name_pt": "Comissão Eleitoral Australiana (AEC)",
    "name_en": "Australian Electoral Commission (AEC)",
    "name_es": "Comisión Electoral Australiana (AEC)",
    "description_pt": (
        "Agência federal independente responsável por conduzir as eleições federais e "
        "os referendos da Austrália, manter o cadastro eleitoral e administrar o "
        "regime de transparência do financiamento político."
    ),
    "description_en": (
        "Independent federal agency responsible for conducting Australia's federal "
        "elections and referendums, maintaining the electoral roll, and administering "
        "the political finance disclosure regime."
    ),
    "description_es": (
        "Agencia federal independiente responsable de conducir las elecciones "
        "federales y los referendos de Australia, mantener el padrón electoral y "
        "administrar el régimen de transparencia del financiamiento político."
    ),
    "website": "https://www.aec.gov.au",
}

RAW_SOURCES = [
    {
        "name_pt": "Arquivo de Resultados Eleitorais da AEC (Tally Room)",
        "name_en": "AEC Tally Room election results archive",
        "name_es": "Archivo de Resultados Electorales de la AEC (Tally Room)",
        "description_pt": (
            "Arquivo público da AEC com os resultados finais de cada evento eleitoral "
            "federal, publicados em CSV por evento, nos níveis nacional, estadual, de "
            "divisão e de local de votação."
        ),
        "description_en": (
            "Public AEC archive of the final results of every federal electoral "
            "event, published as per-event CSV at national, state, division and "
            "polling place level."
        ),
        "description_es": (
            "Archivo público de la AEC con los resultados finales de cada evento "
            "electoral federal, publicados en CSV por evento, a nivel nacional, "
            "estatal, de división y de local de votación."
        ),
        "url": "https://results.aec.gov.au/",
    },
    {
        "name_pt": "Transparency Register da AEC",
        "name_en": "AEC Transparency Register",
        "name_es": "Transparency Register de la AEC",
        "description_pt": (
            "Registro público da AEC com as declarações de financiamento político: "
            "declarações anuais de partidos, entidades associadas, terceiros e "
            "doadores, e declarações referentes a eleições e referendos. Oferece "
            "exportação em massa de todas as declarações em CSV."
        ),
        "description_en": (
            "Public AEC register of political finance disclosures: annual returns "
            "from parties, associated entities, third parties and donors, and returns "
            "relating to elections and referendums. Offers a bulk export of all "
            "disclosures as CSV."
        ),
        "description_es": (
            "Registro público de la AEC con las declaraciones de financiamiento "
            "político: declaraciones anuales de partidos, entidades asociadas, "
            "terceros y donantes, y declaraciones referidas a elecciones y "
            "referendos. Ofrece exportación masiva de todas las declaraciones en CSV."
        ),
        "url": "https://transparency.aec.gov.au/",
    },
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default="staging")
    args = ap.parse_args()
    env = args.env

    ids = server.discover_ids(
        env=env,
        keys=["status", "theme", "license", "availability", "language", "tag"],
    )
    under_review = ids["status"]["under_review"]
    published = ids["status"]["published"]

    org_slug = ORG_SLUG[env]
    org = server.create_update_organization(
        slug=org_slug,
        area_id=server.lookup_id(env=env, slug="au", category="area")["id"],
        env=env,
        **ORGANIZATION,
    )
    print(f"organization {org_slug}: {org['id']}")

    tag_ids = []
    for slug in TAG_SLUGS[env]:
        tag_id = ids["tag"].get(slug)
        if not tag_id:
            if slug not in CREATABLE_TAGS:
                raise SystemExit(
                    f"tag {slug!r} missing on {env} and not creatable"
                )
            pt, en, es = CREATABLE_TAGS[slug]
            tag_id = server.create_update_tag(
                slug=slug, name_pt=pt, name_en=en, name_es=es, env=env
            )["id"]
            print(f"  created tag {slug}: {tag_id}")
        tag_ids.append(tag_id)
    print(f"tags: {len(tag_ids)}")

    existing = server.get_dataset(DATASET["slug"], env=env)
    dataset = server.create_update_dataset(
        id=existing["id"] if existing.get("found") else None,
        organization_ids=[org["id"]],
        theme_ids=[ids["theme"]["politics"], ids["theme"]["government"]],
        tag_ids=tag_ids,
        # under_review keeps the dataset off the production frontend until the prod
        # tables are verified and it is deliberately published.
        status_id=under_review,
        env=env,
        **DATASET,
    )
    print(f"dataset {dataset['slug']}: {dataset['id']} (under_review)")

    existing_sources = {
        s["url"]: s["id"]
        for s in server.get_raw_data_sources(DATASET["slug"], env=env)
    }
    for src in RAW_SOURCES:
        res = server.create_update_raw_data_source(
            id=existing_sources.get(src["url"]),
            dataset_id=dataset["id"],
            license_id=ids["license"]["cc_by"],
            availability_id=ids["availability"]["online"],
            language_ids=[ids["language"]["en"]],
            status_id=published,
            has_structured_data=True,
            is_free=True,
            contains_api=False,
            requires_registration=False,
            env=env,
            **src,
        )
        print(f"raw source {src['url']}: {res['id']}")

    print(f"\ndataset-id {dataset['id']}")


if __name__ == "__main__":
    main()
