"""Constants for the us_ed_nces_ccd recurring pipeline (Prefect 3).

NCES Common Core of Data, republished by the Urban Institute Education Data
Portal. See ``models/us_ed_nces_ccd/code/`` for the schema source of truth and
the shared cleaning transform.
"""

from enum import Enum
from pathlib import Path

#: Repo root, then the committed cleaning code the pipeline imports from.
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_ed_nces_ccd pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``CODE_DIR`` points at ``models/us_ed_nces_ccd/code/``, which holds
    ``schema.py`` and ``utils.py`` -- the column specs and the cleaning
    transform this pipeline shares with the one-shot onboarding bootstrap.
    """

    DATASET_ID = "us_ed_nces_ccd"

    CODE_DIR = _REPO_ROOT / "models" / "us_ed_nces_ccd" / "code"

    #: Materialization order, smallest first, so a schema problem surfaces on a
    #: cheap table. `dicionario` is first because the other tables' dictionary
    #: coverage tests reference it.
    ALL_TABLES = [
        "dicionario",
        "school_district",
        "district_finance",
        "school",
        "staff",
        "school_enrollment",
    ]

    #: Tables rebuilt from the year-partitioned bulk extracts on every run.
    #: `district_finance` is excluded from the annual refresh: the F-33 stops at
    #: the 2020 school year on the portal and is republished on its own,
    #: slower cadence, so it is refreshed by hand rather than polled.
    REFRESH_TABLES = [
        "school",
        "school_district",
        "staff",
        "school_enrollment",
    ]
