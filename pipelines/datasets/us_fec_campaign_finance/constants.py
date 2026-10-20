"""Constants for the us_fec_campaign_finance pipeline."""

from enum import Enum
from pathlib import Path

from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
    YearOnly,
)

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


# Table ids. One independent check_update/extract_and_load pair per table (see
# flows.py's module banner) — kept as plain literals, not Enum members, so
# tasks.py/flows.py can import them directly (same convention as au_ato_abr).
CANDIDATE_TABLE_ID = "candidate"
COMMITTEE_TABLE_ID = "committee"
CANDIDATE_COMMITTEE_LINK_TABLE_ID = "candidate_committee_link"
CONTRIBUTION_INDIVIDUAL_TABLE_ID = "contribution_individual"
CONTRIBUTION_COMMITTEE_TABLE_ID = "contribution_committee"
COMMITTEE_TRANSACTION_TABLE_ID = "committee_transaction"
DISBURSEMENT_TABLE_ID = "disbursement"

# Coverage spec per table, used by `ExtractAndLoad.coverage` (tasks.py) and,
# through it, by `register_table_materialization_task`/`build_and_promote`.
#
# The four transaction tables are high-frequency, so they carry the BD Pro rolling
# window: the most recent 6 months are pro-only, everything older is free.
# register_table_materialization_task recomputes free_end and re-issues the
# BigQuery Row Access Policies on every run, so the window rolls forward by itself
# and the dbt models stay untouched. part_bdpro requires BOTH a free
# (is_closed=False) and a pro (is_closed=True) Coverage to already exist on the
# table, or assert_coverage_topology raises before anything is written — both are
# created at onboarding.
#
# candidate / committee / candidate_committee_link are registration snapshots with
# no date column, so a rolling window is not defined for them and they stay
# AllFree on the cycle. Note tasks.py's check_update does NOT compare against this
# Coverage for these three tables (compare_against="table_update" instead) — year
# granularity is stable for the whole 2-year cycle and would never look "stale"
# within it. dicionario is static and is not refreshed at all, so it has no entry
# here.
_FREE_LAG = FreeLag(unit="months", value=6)

_PART_BDPRO = PartBdpro(
    date_column=DateOnly(col="transaction_date"),
    date_format=DateFormat.YEAR_MD,
    free_lag=_FREE_LAG,
)

COVERAGE = {
    CONTRIBUTION_INDIVIDUAL_TABLE_ID: _PART_BDPRO,
    CONTRIBUTION_COMMITTEE_TABLE_ID: _PART_BDPRO,
    COMMITTEE_TRANSACTION_TABLE_ID: _PART_BDPRO,
    DISBURSEMENT_TABLE_ID: _PART_BDPRO,
    # Registration snapshots: their only temporal column is `year`, the two-year
    # election cycle labelled by the even year it ends in, so coverage is
    # expressed on it at year granularity.
    CANDIDATE_TABLE_ID: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    COMMITTEE_TABLE_ID: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    CANDIDATE_COMMITTEE_LINK_TABLE_ID: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}
