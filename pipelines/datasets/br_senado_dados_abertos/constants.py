"""Constants for the br_senado_dados_abertos recurring pipeline (Prefect 3).

Senado Federal legislative open data (senators, votes, bills, committees,
parties, blocs, leaderships, Directing Board), sourced from the public
Legislative Open Data API. See models/br_senado_dados_abertos/ for the design
and the architecture source of truth (code/architecture_spec.py).
"""

from enum import Enum
from pathlib import Path

from pipelines.datasets.br_senado_dados_abertos.utils import (
    ALL_TABLES,
    DIMS,
    PARTITIONED,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_senado_dados_abertos pipeline (lowercase per the
    repo-wide convention for dataset constant enums)."""

    DATASET_ID = "br_senado_dados_abertos"

    ALL_TABLES = ALL_TABLES
    DIMS = DIMS
    PARTITIONED = PARTITIONED

    # Routine runs re-extract the current year plus this many prior years for
    # the time-series tables (catches late edits); older partitions are left
    # untouched in staging. Dimensions are always rebuilt in full.
    REFRESH_PRIOR_YEARS = 1

    # Architecture source of truth (column order + types), also used by the
    # one-shot onboarding under models/.
    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "br_senado_dados_abertos" / "code"
    )
