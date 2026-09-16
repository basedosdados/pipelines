"""Constants for the au_rba_statistical_tables recurring pipeline (Prefect 3).

Reserve Bank of Australia statistical tables, published as one CSV per table at
predictable URLs. See models/au_rba_statistical_tables/ for the onboarding code
and LICENCE.md for the redistribution scope.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the schema source of truth
# shared with the one-shot bootstrap under models/).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the au_rba_statistical_tables pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "au_rba_statistical_tables"

    # Tables in build order. `data` first so the biggest upload fails fast.
    ALL_TABLES = ["data", "series", "series_break", "dicionario"]

    # The table the source poll is anchored to.
    POLL_TABLE = "data"

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "au_rba_statistical_tables"
        / "code"
        / "architecture"
    )
