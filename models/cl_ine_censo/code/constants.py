"""Shared constants for the cl_ine_censo (INE Chile, Censo 2024) onboarding.

Source: the public, anonymously readable bucket ``gs://bktdescargascenso2024``
published by the Instituto Nacional de Estadisticas de Chile under
CC BY-SA 4.0 (see ``../README.md``).
"""

from __future__ import annotations

import os
from pathlib import Path

# --- Scratch data location -------------------------------------------------
# Raw downloads and cleaned parquet NEVER live in the repo or under Dropbox.
DATA_ROOT = Path(
    os.environ.get(
        "CL_INE_CENSO_DATA", Path.home() / "Downloads" / "cl_ine_censo_data"
    )
)
INPUT_DIR = DATA_ROOT / "input"
OUTPUT_DIR = DATA_ROOT / "output"

MICRODATA_DIR = INPUT_DIR / "extracted"
CARTOGRAPHY_DIR = INPUT_DIR / "cartografia"
REDATAM_DIR = INPUT_DIR / "redatam" / "Microdatos_Redatam_Censo2024"

ARCHITECTURE_DIR = Path(__file__).resolve().parent / "architecture"

# --- Source URLs -----------------------------------------------------------
BUCKET = "https://storage.googleapis.com/bktdescargascenso2024"

URL_MICRODATA = f"{BUCKET}/viv_hog_per_censo2024.zip"
URL_REDATAM = f"{BUCKET}/Microdatos_Redatam_Censo2024.zip"
URL_CARTOGRAPHY = (
    f"{BUCKET}/Cartografia/GEOPARQUET/Cartografia_censo2024_Pais.zip"
)
URL_AGG_DICTIONARY = (
    f"{BUCKET}/Datos_agregados/diccionario_variables_glosas_censo2024.xlsx"
)

# --- Dataset identity ------------------------------------------------------
DATASET_ID = "cl_ine_censo"
CENSUS_YEAR = 2024

TABLES = (
    "persona",
    "hogar",
    "vivienda",
    "manzana_entidad",
    "zona_localidad",
    "dicionario",
)

# --- Sentinels -------------------------------------------------------------
# Verified empirically across every column of all three microdata tables
# (2026-09-23). The scheme is uniform: two negative codes plus SQL NULL.
#
#   -99  "No respuesta"                          - question asked, not answered
#   -66  "Valor suprimido por anonimizacion"     - disclosure control
#   NULL "No aplica"                             - question not asked of this record
#
# NOTE: the Redatam dictionary (CPV2024.dicX) declares DIFFERENT codes for the
# same concepts (e.g. EDAD 96 = suppressed, notappl/missing 99/100), because the
# Redatam build recodes them. Those codes do NOT appear in the parquet microdata.
# Use the Redatam dictionary for value LABELS only, never for sentinel codes.
SENTINEL_NO_RESPONSE = -99
SENTINEL_ANONYMISED = -66

SENTINEL_LABELS = {
    "-99": "No respuesta",
    "-66": "Valor suprimido por anonimizacion",
    # Present as a positive code in the occupation/activity classifications.
    "999": "Ignorado o no clasificable",
}

# Columns that are genuine quantities: the sentinels above are mapped to NULL so
# that arithmetic (mean age, mean years of schooling) is not poisoned. Every one
# of these is documented in the architecture `observations` field.
QUANTITY_COLUMNS = {
    "persona": (
        "edad",
        "escolaridad",
        "p46a_tot_hijs_nac",
        "p46b_hijas_nac",
        "p46c_hijos_nac",
        "p47a_tot_hijs_sobrev",
        "p47b_hijas_sobrev",
        "p47c_hijos_sobrev",
        "p48_anio_nac_uh",
        "p48_mes_nac_uh",
    ),
    "hogar": (),
    "vivienda": (
        "cant_hog",
        "cant_per",
        "p5_num_dormitorios",
        "p11a_num_personas",
        "p11c_num_hogar",
    ),
}

# Geography keys are renamed to the house `id_` convention and widened to the
# zero-padded CUT strings that br_bd_diretorios_cl uses as its primary keys.
GEOGRAPHY_RENAMES = {
    "region": "id_region",
    "provincia": "id_provincia",
    "comuna": "id_comuna",
}
CUT_WIDTHS = {"id_region": 2, "id_provincia": 3, "id_comuna": 5}

# --- Cartography -----------------------------------------------------------
# Each of the two aggregated bases is the union of two GeoParquet layers. INE
# publishes them under exactly these names ("Base manzana-entidad",
# "Base zona-localidad"); the union is INE's own product shape, not ours.
# The layer with the wider schema goes first; the narrower layer's rows take
# NULL for the extra columns.
CARTOGRAPHY_UNIONS = {
    "manzana_entidad": ("Manzanas", "Entidades"),
    "zona_localidad": ("Zonal", "Localidades"),
}

# The union needs a column saying which layer each row came from, since the two
# layers cover disjoint territory (urban blocks vs rural entities). Mapped
# explicitly to the Spanish singular: naive de-pluralising turns "Localidades"
# into the Portuguese-looking "localidade".
CARTOGRAPHY_LEVEL_NAMES = {
    "Manzanas": "manzana",
    "Entidades": "entidad",
    "Zonal": "zona",
    "Localidades": "localidad",
}

# INE ships the geographic NAMES under bare column names that collide with the
# microdata's geographic CODES (`comuna` is the code there, the name here).
# Renamed to nombre_* so the two never get confused in a join.
CARTOGRAPHY_NAME_RENAMES = {
    "region": "nombre_region",
    "provincia": "nombre_provincia",
    "comuna": "nombre_comuna",
    "localidad": "nombre_localidad",
    "entidad": "nombre_entidad",
    "distrito": "nombre_distrito",
}

# Layers deliberately NOT onboarded, and why.
CARTOGRAPHY_SKIPPED = {
    "Comunal": "codes already in br_bd_diretorios_cl.comuna; 345 rows / 126 MB of coastline polygons",
    "Provincial": "codes already in br_bd_diretorios_cl.provincia; 56 rows / 110 MB",
    "Regional": "codes already in br_bd_diretorios_cl.region; 16 rows / 105 MB",
    "Distrital": "pure geometry, carries none of the 189 aggregate variables",
    "Aldeas": "subset of the entity layer; retained inside manzana_entidad via CATEGORIA",
    "Limite_Urbano": "urban-boundary envelope, duplicates aggregates already in zona_localidad",
}

# The 189 aggregate variables share the same definitions across both bases.
AGGREGATE_PREFIXES = ("n_", "prom_")

# Geometry is stored as WKT and cast to GEOGRAPHY in the dbt model.
# Source CRS is SIRGAS 2000 (EPSG:4674); it differs from WGS 84 (EPSG:4326) by
# well under a metre in Chile, so no reprojection is applied. Documented in the
# table notes rather than silently ignored.
GEOMETRY_COLUMN = "geometria"
SOURCE_CRS = "EPSG:4674"
WKT_ROUNDING_PRECISION = 6
