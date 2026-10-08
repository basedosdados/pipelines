"""Constants for the br_mf_divida_ativa recurring pipeline (Prefect 3).

PGFN "Dados Abertos da Dívida Ativa da União" — quarterly stock of active-debt
registrations across three systems (SIDA / previdenciário / FGTS). See
models/br_mf_divida_ativa/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearQuarter,
)

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table), shared with the one-shot
# bootstrap under models/br_mf_divida_ativa/code/.
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the br_mf_divida_ativa pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/br_mf_divida_ativa/code/``, the schema source of truth for both this
    pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "br_mf_divida_ativa"

    # Earliest quarter published by PGFN; the forward source probe starts here.
    FIRST_YEAR = 2020
    FIRST_QUARTER = 1

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "br_mf_divida_ativa" / "code" / "architecture"
    )

    # PGFN republishes quarterly, ~1-2 months after quarter end, on no fixed day.
    # Poll a few days each month at 15:00 BRT; the source-poll guard no-ops until
    # a genuinely new quarter appears, so off-release runs are cheap.
    SCHEDULE_CRON = "0 15 5,15,25 * *"


# Table ids. Kept here as plain literals (not imported from utils.TABLES)
# because utils.py imports `constants` from this module — importing back would
# be circular. Must stay in sync with utils.CATEGORY/utils.TABLES.
NAO_PREVIDENCIARIO_TABLE_ID = "nao_previdenciario"
PREVIDENCIARIO_TABLE_ID = "previdenciario"
FGTS_TABLE_ID = "fgts"

# All three tables refresh quarterly and paywall their most recent two
# quarters to BD Pro. free_lag = 6 months = 2 quarters: free ends at
# source_end - 6 months, pro spans the two quarters after that; the window
# rolls on its own each prod run (`register_table_materialization_task`,
# invoked by the generic build_and_promote stage). part_bdpro requires BOTH a
# free (is_closed=False) and a pro (is_closed=True) Coverage to already exist
# on the table (created at onboarding), or assert_coverage_topology raises
# before anything is written.
COVERAGE = {
    table_id: PartBdpro(
        date_column=YearQuarter(year="ano", quarter="trimestre"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    )
    for table_id in (
        NAO_PREVIDENCIARIO_TABLE_ID,
        PREVIDENCIARIO_TABLE_ID,
        FGTS_TABLE_ID,
    )
}
