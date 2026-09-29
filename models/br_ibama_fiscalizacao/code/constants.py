"""Shared constants for the br_ibama_fiscalizacao onboarding."""

from __future__ import annotations

import os
from pathlib import Path

DATASET_ID = "br_ibama_fiscalizacao"

DATA_DIR = Path(
    os.environ.get(
        "IBAMA_DATA_DIR",
        Path.home() / "Downloads" / "br_ibama_fiscalizacao_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# `dicionario` is a dbt model derived from the fact models, so it has no staging
# table and is never uploaded. See feedback_derive_dictionaries_from_the_models.
TABLES = ("auto_infracao", "area_embargada")

BLOB_BASE = (
    "https://stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados"
)

SOURCE_FILES = {
    "auto_infracao": f"{BLOB_BASE}/SIFISC/auto_infracao/auto_infracao/auto_infracao_csv.zip",
    # The CKAN record for the embargo file points at TERMOS/TERMO_EMBARGO/, which
    # 404s. The live path is TERMOS_DE_EMBARGO/TERMO_EMBARGO/.
    "termo_de_embargo": f"{BLOB_BASE}/TERMOS_DE_EMBARGO/TERMO_EMBARGO/termo_de_embargo.csv",
    "multas_bens_tutelados": (
        f"{BLOB_BASE}/MULTAS_AMBIENTAIS_DISTRIBUIDAS_POR_BENS_TUTELADOS"
        "/multas_ambientais_distribuidas_por_bens_tutelados_csv.zip"
    ),
}
