"""Constants for the au_abs_labour_force pipeline (Prefect 3).

Labour Force, Australia (ABS cat. 6202.0), monthly. Two source mechanisms:

- the ABS Data API (SDMX-CSV) for the status and underutilisation cubes — one
  ``all`` query returns the full monthly history, so the pipeline query is
  month-agnostic;
- the ABS time-series Excel spreadsheets for the hours-worked distribution and
  status-in-employment, which the curated API does not serve; these live under a
  month-stamped release path.

See models/au_abs_labour_force/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the au_abs_labour_force pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/au_abs_labour_force/code/`` — the schema source of truth for both
    this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "au_abs_labour_force"

    # ── ABS Data API (SDMX-CSV) ──────────────────────────────────────────────
    API_BASE = "https://data.api.abs.gov.au/rest/data"
    # SDMX-CSV with both code and label in each cell (e.g. "M1: Employed - full-time").
    SDMX_ACCEPT = "application/vnd.sdmx.data+csv;labels=both"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )
    # SDMX dataflows used. LF = status by state (age total); LF_AGES = national by
    # age (adds Not-in-labour-force + Civilian population); LF_UNDER = under-
    # utilisation by state and age.
    SDMX_FLOWS = ["LF", "LF_AGES", "LF_UNDER"]

    # ── ABS time-series Excel spreadsheets ───────────────────────────────────
    # Month-stamped release path, e.g. .../labour-force-australia/jun-2026/62020018.xlsx
    EXCEL_BASE = (
        "https://www.abs.gov.au/statistics/labour/employment-and-unemployment/"
        "labour-force-australia/{month}"
    )
    # slug -> filename. Table 18 = hours-worked distribution (national, by sex);
    # Table 19 = status in employment (national); SEM1 = status in employment by
    # state (pivot).
    EXCEL_FILES = {
        "hours_worked": "62020018.xlsx",
        "status_national": "62020019.xlsx",
        "status_states": "SEM1.xlsx",
    }

    # Data tables (partitioned parquet). No dicionario — all categories are
    # decoded to readable English labels.
    DATA_TABLES = [
        "labour_force_status",
        "hours_worked",
        "status_in_employment",
        "underutilisation",
    ]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "au_abs_labour_force" / "code" / "architecture"
    )
