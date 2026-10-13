"""Constants for the world_iati_activities recurring pipeline (Prefect 3).

IATI (International Aid Transparency Initiative) activity data, taken from IATI
Tables — the Secretariat's daily flattening of the Bulk Data Service XML corpus
into relational tables. See ``models/world_iati_activities/CLAUDE.md`` for the
full design, the licence filter, and the source traps the transform neutralises.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order, bigquery_type and the raw -> clean name mapping).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the world_iati_activities pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "world_iati_activities"

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "world_iati_activities"
        / "code"
        / "architecture"
    )

    # --- Sources -----------------------------------------------------------
    # IATI Tables publishes one CSV per relational table inside a single zip,
    # rebuilt daily from the previous day's Bulk Data Service snapshot.
    # stats.json carries the run timestamps and a 2,186-field dictionary
    # (table, field, type, non-null count, doc string) — it is both the poll
    # target and the provenance record for the architecture CSVs.
    TABLES_BASE_URL = "https://data.tables.iatistandard.org"
    CSV_ZIP_URL = TABLES_BASE_URL + "/iati_csv.zip"
    STATS_URL = TABLES_BASE_URL + "/stats.json"

    # The Bulk Data Service dataset index. The only place the per-dataset
    # licence lives; joined on short_name == IATI Tables' `dataset` column.
    DATASETS_MINIMAL_URL = (
        "https://bulk-data.iatistandard.org/datasets-minimal"
    )

    # Some IATI hosts 403 a bare client. A browser UA is enough; no cookies or
    # referer are needed. (dashboard.iatistandard.org's HTML root 403s even
    # with one — its /stats/ index does not. We do not read the dashboard here.)
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }

    # --- Tables ------------------------------------------------------------
    # clean table -> member name inside iati_csv.zip (without the .csv suffix).
    #
    # Note `transaction`: the Datasette instance calls this table `trans`
    # (`transaction` is reserved in SQL), but the CSV and pg_dump exports use
    # the full name. Key on the export name.
    SOURCE_TABLES = {
        "activity": "activity",
        "transaction": "transaction",
        "transaction_breakdown": "transaction_breakdown",
        "transaction_sector": "transaction_sector",
        "budget": "budget",
        "planned_disbursement": "planneddisbursement",
        "sector": "sector",
        "recipient_country": "recipientcountry",
        "recipient_region": "recipientregion",
        "participating_org": "participatingorg",
        "related_activity": "relatedactivity",
        "policy_marker": "policymarker",
        "document_link": "documentlink",
        "location": "location",
        "result": "result",
        "result_indicator": "result_indicator",
        "result_indicator_period": "result_indicator_period",
        "organisation": "organisation",
    }

    # registry_dataset is built from DATASETS_MINIMAL_URL, not from the zip.
    DERIVED_TABLES = ["registry_dataset"]

    # Build order: every table is materialised before any is tested, because
    # the relationships tests read sibling models (see the run/test split in
    # .claude/rules/prefect-pipeline-conventions.md).
    ALL_TABLES = [
        "registry_dataset",
        "activity",
        "transaction",
        "transaction_breakdown",
        "transaction_sector",
        "budget",
        "planned_disbursement",
        "sector",
        "recipient_country",
        "recipient_region",
        "participating_org",
        "related_activity",
        "policy_marker",
        "document_link",
        "location",
        "result",
        "result_indicator",
        "result_indicator_period",
        "organisation",
    ]

    # Tables partitioned by `year`, and the clean date column the year is
    # derived from. Everything else is unpartitioned: it has no date of its
    # own, and inventing one from the parent activity would be a fabrication.
    PARTITION_SOURCE = {
        "transaction": "transaction_date",
        "transaction_breakdown": "transaction_date",
        "budget": "period_start_date",
        "planned_disbursement": "period_start_date",
        "result_indicator_period": "period_start_date",
    }

    # --- Licence -----------------------------------------------------------
    # IATI data is licensed per publisher, not per corpus: "All IATI data is
    # the property of its original reporting organisation and is released in
    # line with the open license as listed on the IATI Registry"
    # (https://web-terms.iatistandard.org/en/latest/copyright/).
    #
    # Over the 13,901 registered datasets the mix is 33.1% cc-by, 16.9%
    # other-at, 15.5% cc-zero, 10.2% other-open, 9.7% odc-by, and a tail. Only
    # these two values are non-commercial, covering 16 datasets (0.12%) from 9
    # publishers. They are dropped; every surviving row carries its licence_id
    # so a user can filter further.
    NON_COMMERCIAL_LICENCES = {"cc-nc", "other-nc"}
