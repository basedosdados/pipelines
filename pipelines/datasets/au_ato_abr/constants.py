"""Constants for the au_ato_abr recurring pipeline (Prefect 3).

Australian Business Register "ABN Bulk Extract" (data.gov.au, ATO). The source
republishes a full snapshot **weekly**; we stack each snapshot, partitioned by
``extraction_date`` (see models/au_ato_abr/ONBOARDING_PLAN.md). Each run uploads
the new snapshot to staging with ``dump_mode="overwrite"`` and the incremental
dbt models append the new ``extraction_date`` partition to the prod tables.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the au_ato_abr pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/au_ato_abr/code/`` (the schema source of truth for both this
    pipeline and the one-shot bootstrap).
    """

    DATASET_ID = "au_ato_abr"

    # data.gov.au 403s automated clients without a browser User-Agent.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )

    # CKAN package: the two split ZIPs are re-published in place each week (same
    # resource ids, new files). If the source ever changes the resource ids,
    # update these two URLs.
    CKAN_PACKAGE = "abn-bulk-extract"
    CKAN_PACKAGE_SHOW = "https://data.gov.au/data/api/3/action/package_show?id=abn-bulk-extract"
    ZIP_URLS = [
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/"
        "resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/"
        "resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip",
    ]

    # Tables (partitioned parquet) + the static dictionary.
    DATA_TABLES = ["entity", "other_name", "dgr"]
    ALL_TABLES = ["entity", "other_name", "dgr", "dicionario"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "au_ato_abr" / "code" / "architecture"
    )
