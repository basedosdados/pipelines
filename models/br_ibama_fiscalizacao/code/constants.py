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

# The source URLs live with the pipeline:
# pipelines/datasets/br_ibama_fiscalizacao/constants.py (SOURCE_FILES).
