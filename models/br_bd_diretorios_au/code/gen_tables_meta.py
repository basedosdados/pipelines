"""Generate tables_meta.json: per-table registration metadata for
br_bd_diretorios_au (staging). Names/descriptions in PT/EN/ES, OL entity id,
raw-data-source id, is_directory flag, and PK column(s). Consumed by the
metadata-registration step alongside columns_json/<table>.json.

Run:  python gen_tables_meta.py
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# --- resolved staging IDs (see session notes) ---
SRC_2021 = "d39291af-a44c-4dc3-a2b9-ac8477dc6698"
SRC_2016 = "ab86ad0d-ba9f-49a1-97eb-9dabf29eea44"
ENT = {
    "state": "839765a7-9c7a-44bd-bb88-357cedba03f6",
    "region": "a84789e9-45a7-45da-a2cc-771c4f3997a4",
    "local_government_area": "612797af-512a-43b0-ab56-f883de6b8a34",
    "zip_code": "441dbe6c-706f-436f-9939-897d4a239fff",
    "neighborhood": "56a93e2a-adec-48f8-930e-88e224760fc5",
    "district": "031c0045-f144-4aea-b294-dcf91d0ac9c8",
    "other": "1b3a7364-3e76-4416-8af7-d52824da2d24",
}

UNITS = [
    "sa1",
    "sa2",
    "sa3",
    "sa4",
    "gccsa",
    "lga",
    "postal_area",
    "suburb",
    "commonwealth_electoral_division",
    "state_electoral_division",
]

# unit -> (name_short, ol_entity_key, phrase_pt, phrase_en, phrase_es)
U = {
    "sa1": (
        "SA1 — Statistical Area Level 1",
        "region",
        "das Statistical Areas Level 1 (SA1)",
        "Statistical Areas Level 1 (SA1)",
        "las Statistical Areas Level 1 (SA1)",
    ),
    "sa2": (
        "SA2 — Statistical Area Level 2",
        "region",
        "das Statistical Areas Level 2 (SA2)",
        "Statistical Areas Level 2 (SA2)",
        "las Statistical Areas Level 2 (SA2)",
    ),
    "sa3": (
        "SA3 — Statistical Area Level 3",
        "region",
        "das Statistical Areas Level 3 (SA3)",
        "Statistical Areas Level 3 (SA3)",
        "las Statistical Areas Level 3 (SA3)",
    ),
    "sa4": (
        "SA4 — Statistical Area Level 4",
        "region",
        "das Statistical Areas Level 4 (SA4)",
        "Statistical Areas Level 4 (SA4)",
        "las Statistical Areas Level 4 (SA4)",
    ),
    "gccsa": (
        "GCCSA — Greater Capital City Statistical Area",
        "region",
        "das Greater Capital City Statistical Areas (GCCSA)",
        "Greater Capital City Statistical Areas (GCCSA)",
        "las Greater Capital City Statistical Areas (GCCSA)",
    ),
    "lga": (
        "LGA — Local Government Area",
        "local_government_area",
        "das Local Government Areas (LGA)",
        "Local Government Areas (LGA)",
        "las Local Government Areas (LGA)",
    ),
    "postal_area": (
        "Postal Areas",
        "zip_code",
        "das áreas postais (POA)",
        "postal areas (POA)",
        "las áreas postales (POA)",
    ),
    "suburb": (
        "Suburbs and Localities",
        "neighborhood",
        "dos subúrbios e localidades",
        "suburbs and localities",
        "los suburbios y localidades",
    ),
    "commonwealth_electoral_division": (
        "Commonwealth Electoral Divisions",
        "district",
        "das divisões eleitorais federais (CED)",
        "Commonwealth electoral divisions (CED)",
        "las divisiones electorales federales (CED)",
    ),
    "state_electoral_division": (
        "State Electoral Divisions",
        "district",
        "das divisões eleitorais estaduais (SED)",
        "state electoral divisions (SED)",
        "las divisiones electorales estatales (SED)",
    ),
}
NAME_PT = {  # localized short names where different from English
    "postal_area": "Áreas Postais",
    "suburb": "Subúrbios e Localidades",
    "commonwealth_electoral_division": "Divisões Eleitorais Federais",
    "state_electoral_division": "Divisões Eleitorais Estaduais",
    "lga": "LGA — Área de Governo Local",
}


def unit_table(u, year):
    short, ol, ppt, pen, pes = U[u]
    name_en = f"{short} ({year})"
    name_pt = f"{NAME_PT.get(u, short)} ({year})"
    name_es = name_en
    return {
        "slug": f"{u}_{year}",
        "name_pt": name_pt,
        "name_en": name_en,
        "name_es": name_es,
        "description_pt": f"Diretório {ppt} da Austrália na edição de {year} do ASGS, com código, nome, hierarquia superior e área.",
        "description_en": f"Directory of {pen} in Australia in the {year} ASGS edition, with code, name, parent hierarchy and area.",
        "description_es": f"Directorio de {pes} en Australia en la edición {year} del ASGS, con código, nombre, jerarquía superior y área.",
        "raw_data_source_id": SRC_2021 if year == "2021" else SRC_2016,
        "ol_entity_id": ENT[ol],
        "is_directory": True,
        "pk_columns": [f"id_{u}"],
    }


def corr(u):
    up = u.upper()
    return {
        "slug": f"correspondence_{u}_2016_2021",
        "name_pt": f"Correspondência {up} 2016–2021",  # noqa: RUF001
        "name_en": f"{up} correspondence 2016–2021",  # noqa: RUF001
        "name_es": f"Correspondencia {up} 2016–2021",  # noqa: RUF001
        "description_pt": f"Correspondência (crosswalk) entre as {up} do ASGS de 2016 e 2021, com a razão de aporcionamento populacional e o indicador de qualidade do Australian Bureau of Statistics.",
        "description_en": f"Correspondence (crosswalk) between ABS ASGS {up} of 2016 and 2021, with the population apportionment ratio and the Australian Bureau of Statistics quality indicator.",
        "description_es": f"Correspondencia (crosswalk) entre las {up} del ASGS de 2016 y 2021, con la razón de asignación poblacional y el indicador de calidad de la Australian Bureau of Statistics.",
        "raw_data_source_id": SRC_2021,
        "ol_entity_id": ENT["other"],
        "is_directory": False,
        "pk_columns": [],
    }


def main():
    tables = [
        {
            "slug": "state",
            "name_pt": "Estados e territórios",
            "name_en": "States and territories",
            "name_es": "Estados y territorios",
            "description_pt": "Diretório de estados e territórios da Austrália, com código, sigla, nome e área.",
            "description_en": "Directory of Australian states and territories, with code, abbreviation, name and area.",
            "description_es": "Directorio de estados y territorios de Australia, con código, sigla, nombre y área.",
            "raw_data_source_id": SRC_2021,
            "ol_entity_id": ENT["state"],
            "is_directory": True,
            "pk_columns": ["id_state"],
        }
    ]
    for y in ("2021", "2016"):
        for u in UNITS:
            tables.append(unit_table(u, y))
    tables.append(corr("sa2"))
    tables.append(corr("lga"))
    with open(os.path.join(HERE, "tables_meta.json"), "w") as f:
        json.dump(tables, f, ensure_ascii=False, indent=2)
    print(f"wrote tables_meta.json with {len(tables)} tables")


if __name__ == "__main__":
    main()
