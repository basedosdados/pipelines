"""Constants for the br_mf_divida_ativa recurring pipeline (Prefect 3).

PGFN "Dados Abertos da Dívida Ativa da União" — quarterly stock of active-debt
registrations across three systems (SIDA / previdenciário / FGTS). See
models/br_mf_divida_ativa/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

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
