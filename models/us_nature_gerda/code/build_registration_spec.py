#!/usr/bin/env python3
"""Build a machine-readable registration spec (one entry per table) for the
metadata step: trilingual name+description, observation-level entity IDs,
coverage year range (from the parquet), gcp cloud-table ids, and the columns.json
path. Written to metadata/registration_spec.json.

Run: cd models/us_nature_gerda/code && python3 build_registration_spec.py
"""

import json
import os

import pyarrow.compute as pc
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..", "..")

ENT = {
    "municipality": "460cf58b-63a7-4fb7-910f-4ca8ea58c25e",
    "county": "01069a8a-5c20-4969-b295-a55082a828e8",
    "state": "839765a7-9c7a-44bd-bb88-357cedba03f6",
    "year": "e1bf146e-b6bb-4b65-bee7-c800876e80a5",
    "party": "fcee5475-ec7c-46c9-8000-b223e892932c",
    "district": "031c0045-f144-4aea-b294-dcf91d0ac9c8",
}

MODULE = {
    "federal": (
        "Eleições federais (Bundestag)",
        "Federal elections (Bundestag)",
        "Elecciones federales (Bundestag)",
    ),
    "state": (
        "Eleições estaduais (Landtag)",
        "State elections (Landtag)",
        "Elecciones estatales (Landtag)",
    ),
    "municipal": (
        "Eleições municipais (Gemeinderat)",
        "Municipal elections (Gemeinderat)",
        "Elecciones municipales (Gemeinderat)",
    ),
    "county_council": (
        "Eleições para conselho de condado (Kreistag)",
        "County council elections (Kreistag)",
        "Elecciones al consejo de condado (Kreistag)",
    ),
    "european": (
        "Eleições europeias",
        "European elections",
        "Elecciones europeas",
    ),
}
LEVEL = {
    "municipality": (
        "nível municipal",
        "municipality level",
        "nivel municipal",
    ),
    "county": ("nível de condado", "county level", "nivel de condado"),
    "constituency": (
        "nível de distrito eleitoral",
        "constituency level",
        "nivel de circunscripción",
    ),
}


def harm_phrase(slug):
    if "harmonized_2021" in slug:
        return (
            "harmonizadas para limites municipais de 2021",
            "harmonized to 2021 municipal boundaries",
            "armonizadas a límites municipales de 2021",
        )
    if "harmonized_2023" in slug:
        return (
            "harmonizadas para limites municipais de 2023",
            "harmonized to 2023 municipal boundaries",
            "armonizadas a límites municipales de 2023",
        )
    if "harmonized_2025" in slug:
        return (
            "harmonizadas para limites municipais de 2025",
            "harmonized to 2025 municipal boundaries",
            "armonizadas a límites municipales de 2025",
        )
    if "2021_on_2025" in slug:
        return (
            "recalculadas sobre os limites de distritos de 2025",
            "recomputed on 2025 constituency boundaries",
            "recalculadas sobre los límites de circunscripción de 2025",
        )
    return (
        "com limites originais",
        "on original boundaries",
        "con límites originales",
    )


# gerda table -> (module_key, level_key or None, unit_pt/en/es for the "one row per" phrase)
GERDA = {
    "federal_municipality": ("federal", "municipality"),
    "federal_municipality_harmonized_2021": ("federal", "municipality"),
    "federal_municipality_harmonized_2025": ("federal", "municipality"),
    "federal_county": ("federal", "county"),
    "federal_county_harmonized_2021": ("federal", "county"),
    "federal_constituency": ("federal", "constituency"),
    "federal_constituency_2021_on_2025": ("federal", "constituency"),
    "state_municipality": ("state", "municipality"),
    "state_municipality_harmonized_2021": ("state", "municipality"),
    "state_municipality_harmonized_2023": ("state", "municipality"),
    "state_municipality_harmonized_2025": ("state", "municipality"),
    "state_constituency": ("state", "constituency"),
    "municipal": ("municipal", "municipality"),
    "municipal_harmonized_2021": ("municipal", "municipality"),
    "municipal_harmonized_2025": ("municipal", "municipality"),
    "county_council_municipality": ("county_council", "municipality"),
    "county_council_municipality_harmonized_2021": (
        "county_council",
        "municipality",
    ),
    "county_council_county_harmonized_2021": ("county_council", "county"),
    "european_municipality": ("european", "municipality"),
    "european_municipality_harmonized_2021": ("european", "municipality"),
}

OL_BY_LEVEL = {
    "municipality": ["municipality", "year", "party"],
    "county": ["county", "year", "party"],
    "constituency": ["district", "year", "party"],
}

# which id column identifies each level's geographic observation level
GEO_COL = {
    "municipality": ("id_municipality", "municipality"),
    "county": ("id_county", "county"),
    "constituency": ("id_constituency", "district"),
}

# directory table -> (name_pt, name_en, name_es, desc_pt, desc_en, desc_es, [entities])
DIRS = {
    "state": (
        "Estados (Bundesländer)",
        "States (Bundesländer)",
        "Estados (Bundesländer)",
        "Diretório dos 16 estados alemães (Bundesländer).",
        "Directory of the 16 German federal states (Bundesländer).",
        "Directorio de los 16 estados federados alemanes (Bundesländer).",
        ["state"],
    ),
    "county": (
        "Condados (Kreise)",
        "Counties (Kreise)",
        "Condados (Kreise)",
        "Diretório dos condados alemães (Kreise e kreisfreie Städte) com limites de 2021.",
        "Directory of German counties (Kreise and kreisfreie Städte) at 2021 boundaries.",
        "Directorio de los condados alemanes (Kreise y kreisfreie Städte) con límites de 2021.",
        ["county"],
    ),
    "municipality": (
        "Municípios (Gemeinden)",
        "Municipalities (Gemeinden)",
        "Municipios (Gemeinden)",
        "Diretório dos municípios alemães (Gemeinden) com limites de 2021.",
        "Directory of German municipalities (Gemeinden) at 2021 boundaries.",
        "Directorio de los municipios alemanes (Gemeinden) con límites de 2021.",
        ["municipality"],
    ),
    "constituency": (
        "Distritos eleitorais (Wahlkreise)",
        "Electoral constituencies (Wahlkreise)",
        "Circunscripciones electorales (Wahlkreise)",
        "Diretório dos distritos eleitorais alemães (Wahlkreise), federais e estaduais.",
        "Directory of German electoral constituencies (Wahlkreise), federal and state.",
        "Directorio de las circunscripciones electorales alemanas (Wahlkreise), federales y estatales.",
        ["district"],
    ),
    "party": (
        "Partidos",
        "Parties",
        "Partidos",
        "Diretório de partidos e listas alemães (nomes normalizados do GERDA) com atributos do ParlGov.",
        "Directory of German parties and lists (GERDA normalized names) with ParlGov attributes.",
        "Directorio de partidos y listas alemanes (nombres normalizados de GERDA) con atributos de ParlGov.",
        ["party"],
    ),
}


def year_range(pq_path):
    t = pq.read_table(pq_path, columns=["year"])
    ys = pc.cast(t["year"], "int64")
    # pyrefly: ignore [missing-attribute]
    return int(pc.min(ys).as_py()), int(pc.max(ys).as_py())


def build():
    # gerda
    gerda = []
    for slug, (mod, lvl) in GERDA.items():
        p = os.path.join(ROOT, "us_nature_gerda", "output", f"{slug}.parquet")
        y0, y1 = year_range(p)
        mp, me, ms = MODULE[mod]
        lp, le, ls = LEVEL[lvl]
        hp, he, hs = harm_phrase(slug)
        name = (f"{mp}, {lp}", f"{me}, {le}", f"{ms}, {ls}")
        if "harmonized" in slug or "2021_on_2025" in slug:
            name = (
                f"{name[0]} ({hp})",
                f"{name[1]} ({he})",
                f"{name[2]} ({hs})",
            )
        desc = (
            f"Resultados de {mp.lower()} no {lp}, {hp}, em formato longo por partido (uma linha por unidade, eleição e partido). Cobertura {y0}-{y1}.",
            f"{me} results at the {le}, {he}, in long format by party (one row per unit, election and party). Coverage {y0}-{y1}.",
            f"Resultados de {ms.lower()} a {ls}, {hs}, en formato largo por partido (una fila por unidad, elección y partido). Cobertura {y0}-{y1}.",
        )
        gerda.append(
            {
                "slug": slug,
                "gcp_table_id": slug,
                "name_pt": name[0],
                "name_en": name[1],
                "name_es": name[2],
                "description_pt": desc[0],
                "description_en": desc[1],
                "description_es": desc[2],
                "ol_entity_ids": [ENT[e] for e in OL_BY_LEVEL[lvl]],
                "coverage_start": y0,
                "coverage_end": y1,
                "columns_json": os.path.abspath(
                    os.path.join(
                        ROOT,
                        "us_nature_gerda",
                        "metadata",
                        f"{slug}.columns.json",
                    )
                ),
                "partition_column": "year",
                "column_ol_links": {
                    GEO_COL[lvl][0]: GEO_COL[lvl][1],
                    "year": "year",
                    "party": "party",
                },
            }
        )
    # county_council_seats: a county-year seat-composition panel, long by party
    y0, y1 = year_range(
        os.path.join(
            ROOT, "us_nature_gerda", "output", "county_council_seats.parquet"
        )
    )
    gerda.append(
        {
            "slug": "county_council_seats",
            "gcp_table_id": "county_council_seats",
            "name_pt": "Composição de cadeiras dos conselhos de condado (Kreistag)",
            "name_en": "County council seat composition (Kreistag)",
            "name_es": "Composición de escaños de los consejos de condado (Kreistag)",
            "description_pt": f"Composição de cadeiras dos conselhos de condado (Kreistag) e das câmaras das kreisfreie Städte, painel anual em formato longo por partido. Cobertura {y0}-{y1}.",
            "description_en": f"Seat composition of county councils (Kreistag) and the councils of kreisfreie Städte, an annual panel in long format by party. Coverage {y0}-{y1}.",
            "description_es": f"Composición de escaños de los consejos de condado (Kreistag) y de los ayuntamientos de las kreisfreie Städte, panel anual en formato largo por partido. Cobertura {y0}-{y1}.",
            "ol_entity_ids": [ENT["county"], ENT["year"], ENT["party"]],
            "coverage_start": y0,
            "coverage_end": y1,
            "columns_json": os.path.abspath(
                os.path.join(
                    ROOT,
                    "us_nature_gerda",
                    "metadata",
                    "county_council_seats.columns.json",
                )
            ),
            "partition_column": "year",
            "column_ol_links": {
                "id_county": "county",
                "year": "year",
                "party": "party",
            },
        }
    )
    with open(
        os.path.join(
            ROOT, "us_nature_gerda", "metadata", "registration_spec.json"
        ),
        "w",
    ) as fh:
        json.dump(
            {
                "dataset_id": "ef590f85-48db-45a8-99f5-8f5e23c4e06f",
                "gcp_dataset_id": "us_nature_gerda",
                "tables": gerda,
            },
            fh,
            ensure_ascii=False,
            indent=1,
        )
    # directories
    dirs = []
    for slug, (np_, ne, ns, dp, de, ds, ents) in DIRS.items():
        dirs.append(
            {
                "slug": slug,
                "gcp_table_id": slug,
                "name_pt": np_,
                "name_en": ne,
                "name_es": ns,
                "description_pt": dp,
                "description_en": de,
                "description_es": ds,
                "ol_entity_ids": [ENT[e] for e in ents],
                "coverage_start": None,
                "coverage_end": None,
                "columns_json": os.path.abspath(
                    os.path.join(
                        ROOT,
                        "br_bd_diretorios_de",
                        "metadata",
                        f"{slug}.columns.json",
                    )
                ),
                "is_directory": True,
                "primary_key": f"id_{slug}" if slug != "party" else "id_party",
                "column_ol_links": {
                    (f"id_{slug}" if slug != "party" else "id_party"): ents[0]
                },
            }
        )
    with open(
        os.path.join(
            ROOT, "br_bd_diretorios_de", "metadata", "registration_spec.json"
        ),
        "w",
    ) as fh:
        json.dump(
            {
                "dataset_id": "5ffefea7-9367-4a28-bebd-96908b894f48",
                "gcp_dataset_id": "br_bd_diretorios_de",
                "tables": dirs,
            },
            fh,
            ensure_ascii=False,
            indent=1,
        )
    print(
        f"wrote registration_spec.json: {len(gerda)} gerda tables, {len(dirs)} directory tables"
    )


if __name__ == "__main__":
    build()
