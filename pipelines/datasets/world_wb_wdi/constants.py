"""Constants for the world_wb_wdi recurring pipeline (Prefect 3).

World Bank World Development Indicators (WDI). The bulk ``WDI_CSV.zip`` carries
the full history on every release, so each run is a full replace
(``dump_mode="overwrite"``). See models/world_wb_wdi/ for the one-shot bootstrap.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth — column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the world_wb_wdi pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/world_wb_wdi/code/``, the schema source of truth for both this
    pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "world_wb_wdi"

    # World Bank bulk WDI archive (CC BY 4.0). One ~270MB zip holds all six CSVs;
    # the World Bank republishes it in full on each release (~quarterly).
    SOURCE_URL = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
    ZIP_NAME = "WDI_CSV.zip"

    # Source file (inside the zip) per output table. ``data`` and ``dicionario``
    # both derive from WDICSV.csv / WDISeries.csv and are handled specially.
    SOURCE_FILES = {
        "data": "WDICSV.csv",
        "indicators": "WDISeries.csv",
        "country_indicator": "WDIcountry-series.csv",
        "footnote": "WDIfootnote.csv",
        "indicator_time": "WDIseries-time.csv",
    }

    # Tables partitioned by year (hive year=YYYY dirs); the rest are single files.
    PARTITIONED_TABLES = ["data", "footnote"]

    DATA_TABLES = [
        "data",
        "indicators",
        "country_indicator",
        "footnote",
        "indicator_time",
    ]
    ALL_TABLES = [
        "data",
        "indicators",
        "country_indicator",
        "footnote",
        "indicator_time",
        "dicionario",
    ]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "world_wb_wdi" / "code" / "architecture"
    )
