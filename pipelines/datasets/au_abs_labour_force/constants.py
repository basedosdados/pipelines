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

from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
)

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


# Table ids. Kept here as plain literals (not Enum members) so `tasks.py`/
# `flows.py` can import them directly — one independent check_update/
# extract_and_load pair per table (the repo-wide one-pipeline-per-table_id
# shape; see `pipelines.utils.stage_dispatch.pipeline_factory`).
LABOUR_FORCE_STATUS_TABLE_ID = "labour_force_status"
HOURS_WORKED_TABLE_ID = "hours_worked"
STATUS_IN_EMPLOYMENT_TABLE_ID = "status_in_employment"
UNDERUTILISATION_TABLE_ID = "underutilisation"

# Coverage spec per table, used by `ExtractAndLoad.coverage` (tasks.py) and,
# through it, by `register_table_materialization_task`/`build_and_promote`.
#
# Every table refreshes monthly, so every table gets the BD Pro rolling
# window: the most recent 6 months are pro-only, older data is free. Each run
# recomputes free_end = source_end - 6 months, rewrites both DateTimeRanges,
# and re-issues the BigQuery Row Access Policies, so the window rolls forward
# on its own each run.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. Onboarding created only the free Coverage, so
# the pro Coverage must be created by hand before the first armed
# (update_metadata) run.
_PART_BDPRO = PartBdpro(
    date_column=YearMonth(year="year", month="month"),
    date_format=DateFormat.YEAR_MONTH,
    free_lag=FreeLag(unit="months", value=6),
)
COVERAGE = {
    LABOUR_FORCE_STATUS_TABLE_ID: _PART_BDPRO,
    HOURS_WORKED_TABLE_ID: _PART_BDPRO,
    STATUS_IN_EMPLOYMENT_TABLE_ID: _PART_BDPRO,
    UNDERUTILISATION_TABLE_ID: _PART_BDPRO,
}
