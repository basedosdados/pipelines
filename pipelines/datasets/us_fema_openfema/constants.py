"""Constants for the us_fema_openfema pipeline."""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

# The architecture CSVs are the single source of truth for column order and
# type, and live with the dbt models rather than here.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ARCHITECTURE_DIR = (
    _REPO_ROOT / "models" / "us_fema_openfema" / "code" / "architecture"
)

# Scratch data never goes in the repo or under Dropbox — see
# .claude/rules/onboarding-workflow.md.
_DEFAULT_DATA_ROOT = Path.home() / "Downloads" / "us_fema_openfema_data"


class constants(Enum):
    DATASET_ID = "us_fema_openfema"

    ARCHITECTURE_DIR = _ARCHITECTURE_DIR
    DATA_ROOT = Path(
        os.environ.get("US_FEMA_OPENFEMA_DATA", _DEFAULT_DATA_ROOT)
    )

    # OpenFEMA's own catalog. `lastDataSetRefresh` on a set is the timestamp
    # that moves when data is republished; `lastRefresh` is a schema/store
    # timestamp and lags it badly, so the poll reads the former.
    CATALOG_URL = "https://www.fema.gov/api/open/v1/DataSets?$top=1000"

    TERMS_URL = "https://www.fema.gov/about/openfema/terms-conditions"

    # FEMA requires this sentence verbatim wherever the data is republished.
    DISCLAIMER = (
        "This product uses the Federal Emergency Management Agency's OpenFEMA "
        "API, but is not endorsed by FEMA. The Federal Government or FEMA "
        "cannot vouch for the data or analyses derived from these data after "
        "the data have been retrieved from the Agency's website(s)."
    )

    # table slug -> (OpenFEMA set name, version, parquet URL)
    #
    # NOTE the NFIP sets are v3. `FimaNfipClaims`/`FimaNfipPolicies` v2 are
    # deprecated: removed 2026-10-15, frozen at 2026-06-01.
    SOURCES = {
        "disaster_declaration": (
            "DisasterDeclarationsSummaries",
            2,
            "https://www.fema.gov/api/open/v2/"
            "DisasterDeclarationsSummaries.parquet",
        ),
        "public_assistance_project": (
            "PublicAssistanceFundedProjectsDetails",
            2,
            "https://www.fema.gov/api/open/v2/"
            "PublicAssistanceFundedProjectsDetails.parquet",
        ),
        "nfip_claim": (
            "NfipClaims",
            3,
            "https://www.fema.gov/about/reports-and-data/openfema/v3/"
            "NfipClaimsV3.parquet",
        ),
        "nfip_policy": (
            "NfipPolicies",
            3,
            "https://www.fema.gov/about/reports-and-data/openfema/v3/"
            "NfipPoliciesV3.parquet",
        ),
    }

    TABLES = (*tuple(SOURCES), "dicionario")

    # Rows are streamed in batches of this size when re-partitioning, which
    # keeps peak memory flat on the 74M-row policy file.
    BATCH_ROWS = 250_000

    REQUEST_TIMEOUT = 900

    # www.fema.gov stalls part-way through the 3.7 GB policy file often enough
    # that a single-shot download is not reliable. The endpoint honours HTTP
    # Range, so a stalled transfer is resumed rather than restarted.
    DOWNLOAD_ATTEMPTS = 8
