"""Constants for the ``us_osha_enforcement`` dataset."""

from __future__ import annotations

from enum import Enum
from pathlib import Path

# pipelines/datasets/us_osha_enforcement/constants.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Source layout, table list and repo-relative paths."""

    DATASET_ID = "us_osha_enforcement"

    #: The DOL Open Data Portal's bulk-download endpoint. Recovered from the
    #: portal's own "Download Complete Dataset" link; it needs no API key,
    #: unlike ``apiprod.dol.gov/v4/get/...``. The retired
    #: ``enforcedata.dol.gov`` host now 301s to the portal's SPA shell.
    BULK_URL = "https://data.dol.gov/data-catalog/OSHA/{file}/OSHA_{file}.zip"

    #: Per-table data dictionary, used to check the source schema has not moved.
    CATALOG_URL = "https://apiprod.dol.gov/v4/datasets/{dataset_id}"

    #: DOL catalog dataset ids, for the schema check.
    CATALOG_IDS = {
        "inspection": 10334,
        "violation": 10338,
        "violation_event": 10357,
        "violation_gen_duty_std": 10340,
        "related_activity": 10336,
        "emphasis_codes": 10337,
        "optional_code_info": 10356,
        "accident": 10331,
        "accident_injury": 10333,
        "accident_abstract": 10332,
        "accident_lookup2": 10330,
    }

    #: Source file stem -> published table slug. The parent files come first;
    #: the cleaner needs ``inspection`` and ``accident`` before their children.
    SOURCE_FILES = [
        "inspection",
        "accident",
        "violation",
        "violation_event",
        "violation_gen_duty_std",
        "related_activity",
        "emphasis_codes",
        "optional_code_info",
        "accident_injury",
        "accident_abstract",
        "accident_lookup2",
    ]

    #: Published tables, in the order they are registered and materialised.
    TABLES = [
        "inspection",
        "violation",
        "violation_event",
        "violation_text",
        "related_activity",
        "emphasis_code",
        "optional_code_info",
        "accident",
        "accident_injury",
        "accident_narrative",
        "dicionario",
    ]

    ARCHITECTURE_DIR = REPO_ROOT / "models" / "us_osha_enforcement" / "code"

    #: Scratch root. Never inside the repo or Dropbox — the raw zips are 6 GB.
    DEFAULT_DATA_DIR = "~/Downloads/us_osha_enforcement_data"

    #: BigQuery range partitioning bounds for ``year``.
    PARTITION_START = 1970
    PARTITION_END = 2035
