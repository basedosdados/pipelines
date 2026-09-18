"""Constants for the us_fec_campaign_finance pipeline."""

from enum import Enum
from pathlib import Path

# Repo root: pipelines/datasets/<ds>/constants.py -> up four levels.
REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for us_fec_campaign_finance."""

    DATASET_ID = "us_fec_campaign_finance"

    # FEC bulk downloads are served from a GovCloud S3 bucket. The www.fec.gov
    # /files/... paths 302 here, so hitting it directly avoids a redirect hop on
    # multi-GB transfers.
    BULK_BASE = (
        "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1"
        ".amazonaws.com/bulk-downloads"
    )

    USER_AGENT = "basedosdados-pipelines/1.0 (+https://basedosdados.org)"

    # The architecture CSVs are the single source of truth for column names,
    # order and types. They live with the onboarding code and are read from
    # here by a repo-relative path rather than duplicated.
    ARCHITECTURE_DIR = (
        REPO_ROOT
        / "models"
        / "us_fec_campaign_finance"
        / "code"
        / "architecture"
    )

    # Tables refreshed by the recurring pipeline, in dependency-friendly order.
    # dicionario is static (it encodes the FEC's published code lists), so it is
    # rebuilt only at onboarding and is not in this list.
    ALL_TABLES = [
        "candidate",
        "committee",
        "candidate_committee_link",
        "contribution_individual",
        "contribution_committee",
        "committee_transaction",
        "disbursement",
    ]

    # The FEC republishes the current cycle daily and freezes past cycles, so a
    # scheduled run re-pulls only the current cycle's partition.
    FIRST_CYCLE = 1980
