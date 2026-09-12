"""Generate tables_meta.json: per-table registration metadata for
au_abs_community_profiles. Names/descriptions PT/EN/ES, the observation-level
entity id + the column to link to it, and is_directory=False.

Observation level = one per table: the geographic entity for geo tables (linked
on id_<level>), `other` for national (linked on census_year) and auxiliary_info
(linked on cell_code). Entity ids are the shared backend entities resolved during
the br_bd_diretorios_au build (STAGING values; substitute local_government_area
612797af -> e0e38686 for PROD).

raw_data_source_id is left as a placeholder — the ABS DataPacks raw source is
created at registration time and substituted in.

Run:  python gen_tables_meta.py
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))

ENT = {  # shared backend entity ids (staging)
    "state": "839765a7-9c7a-44bd-bb88-357cedba03f6",
    "region": "a84789e9-45a7-45da-a2cc-771c4f3997a4",
    "local_government_area": "612797af-512a-43b0-ab56-f883de6b8a34",
    "zip_code": "441dbe6c-706f-436f-9939-897d4a239fff",
    "neighborhood": "56a93e2a-adec-48f8-930e-88e224760fc5",
    "district": "031c0045-f144-4aea-b294-dcf91d0ac9c8",
    "other": "1b3a7364-3e76-4416-8af7-d52824da2d24",
}
SRC_PLACEHOLDER = "REPLACE_WITH_ABS_DATAPACKS_RAW_SOURCE_ID"

# geo table -> (id column, entity key, name pt/en/es)
GEO = {
    "state": (
        "id_state",
        "state",
        "estados e territórios",
        "states and territories",
        "estados y territorios",
    ),
    "sa1": (
        "id_sa1",
        "region",
        "Statistical Areas Level 1 (SA1)",
        "Statistical Areas Level 1 (SA1)",
        "Statistical Areas Level 1 (SA1)",
    ),
    "sa2": (
        "id_sa2",
        "region",
        "Statistical Areas Level 2 (SA2)",
        "Statistical Areas Level 2 (SA2)",
        "Statistical Areas Level 2 (SA2)",
    ),
    "sa3": (
        "id_sa3",
        "region",
        "Statistical Areas Level 3 (SA3)",
        "Statistical Areas Level 3 (SA3)",
        "Statistical Areas Level 3 (SA3)",
    ),
    "sa4": (
        "id_sa4",
        "region",
        "Statistical Areas Level 4 (SA4)",
        "Statistical Areas Level 4 (SA4)",
        "Statistical Areas Level 4 (SA4)",
    ),
    "gccsa": (
        "id_gccsa",
        "region",
        "Greater Capital City Statistical Areas (GCCSA)",
        "Greater Capital City Statistical Areas (GCCSA)",
        "Greater Capital City Statistical Areas (GCCSA)",
    ),
    "lga": (
        "id_lga",
        "local_government_area",
        "áreas de governo local (LGA)",
        "local government areas (LGA)",
        "áreas de gobierno local (LGA)",
    ),
    "suburb": (
        "id_suburb",
        "neighborhood",
        "subúrbios e localidades",
        "suburbs and localities",
        "suburbios y localidades",
    ),
    "postal_area": (
        "id_postal_area",
        "zip_code",
        "áreas postais (POA)",
        "postal areas (POA)",
        "áreas postales (POA)",
    ),
    "commonwealth_electoral_division": (
        "id_commonwealth_electoral_division",
        "district",
        "divisões eleitorais federais (CED)",
        "Commonwealth electoral divisions (CED)",
        "divisiones electorales federales (CED)",
    ),
    "state_electoral_division": (
        "id_state_electoral_division",
        "district",
        "divisões eleitorais estaduais (SED)",
        "state electoral divisions (SED)",
        "divisiones electorales estatales (SED)",
    ),
}


def geo_meta(t):
    id_col, ent, npt, nen, nes = GEO[t]
    return {
        "slug": t,
        "name_pt": f"Perfis por {npt}",
        "name_en": f"Profiles by {nen}",
        "name_es": f"Perfiles por {nes}",
        "description_pt": f"Perfis da comunidade do Censo australiano (GCP e TSP) no nível de {npt}, em formato longo (uma célula por linha).",
        "description_en": f"Australian Census Community Profiles (GCP and TSP) at the {nen} level, in long format (one cell per row).",
        "description_es": f"Perfiles de la comunidad del Censo australiano (GCP y TSP) al nivel de {nes}, en formato largo (una celda por fila).",
        "raw_data_source_id": SRC_PLACEHOLDER,
        "ol_entity_id": ENT[ent],
        "ol_link_column": id_col,
        "is_directory": False,
    }


def main():
    tables = [
        {
            "slug": "national",
            "name_pt": "Perfis nacionais",
            "name_en": "National profiles",
            "name_es": "Perfiles nacionales",
            "description_pt": "Perfis da comunidade do Censo australiano (GCP e TSP) no nível nacional (Austrália), em formato longo.",
            "description_en": "Australian Census Community Profiles (GCP and TSP) at the national (Australia) level, in long format.",
            "description_es": "Perfiles de la comunidad del Censo australiano (GCP y TSP) a nivel nacional (Australia), en formato largo.",
            "raw_data_source_id": SRC_PLACEHOLDER,
            "ol_entity_id": ENT["other"],
            "ol_link_column": "census_year",
            "is_directory": False,
        }
    ]
    for t in GEO:
        tables.append(geo_meta(t))
    tables.append(
        {
            "slug": "auxiliary_info",
            "name_pt": "Catálogo de células (auxiliary_info)",
            "name_en": "Cell catalogue (auxiliary_info)",
            "name_es": "Catálogo de celdas (auxiliary_info)",
            "description_pt": "Catálogo das células (variáveis) dos perfis: descrição, tabela, população, tipo de estatística e unidade de medida.",
            "description_en": "Catalogue of the profile cells (variables): description, table, population, statistic type and measurement unit.",
            "description_es": "Catálogo de las celdas (variables) de los perfiles: descripción, tabla, población, tipo de estadística y unidad de medida.",
            "raw_data_source_id": SRC_PLACEHOLDER,
            "ol_entity_id": ENT["other"],
            "ol_link_column": "cell_code",
            "is_directory": False,
        }
    )
    with open(os.path.join(HERE, "tables_meta.json"), "w") as f:
        json.dump(tables, f, ensure_ascii=False, indent=2)
    print(f"wrote tables_meta.json with {len(tables)} tables")


if __name__ == "__main__":
    main()
