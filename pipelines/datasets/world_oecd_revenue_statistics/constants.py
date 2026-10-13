"""Constants for world_oecd_revenue_statistics (OECD Global Revenue Statistics).

Source: OECD SDMX REST API. The single comparative dataflow
``OECD.CTP.TPS,DSD_REV_COMP_GLOBAL@DF_RSGLOBAL`` carries every economy's tax and
non-tax revenue on the OECD classification, expressed as % of GDP, % of the same
institutional sector, % of the same revenue category, national currency and USD.

The architecture CSVs under ``models/world_oecd_revenue_statistics/code/architecture/``
are the single source of truth for column names, order and types.
"""

from enum import Enum
from pathlib import Path


class constants(Enum):
    DATASET_ID = "world_oecd_revenue_statistics"

    SDMX_BASE = "https://sdmx.oecd.org/public/rest"
    AGENCY = "OECD.CTP.TPS"
    DSD = "DSD_REV_COMP_GLOBAL"
    FLOW = "DF_RSGLOBAL"
    # OECD's flowRef is the full ``<DSD>@<FLOW>`` id; version is discovered dynamically
    # (the vintage trap: editions are encoded as dataflow versions).
    FLOW_REF = "OECD.CTP.TPS,DSD_REV_COMP_GLOBAL@DF_RSGLOBAL"
    DEFAULT_VERSION = "2.1"

    # sdmx.oecd.org 403s the literal ``Python-urllib/3.x`` UA; pin an explicit one.
    USER_AGENT = "basedosdados-onboarding/1.0 (contato@basedosdados.org)"

    # UNIT_MEASURE code -> wide value column. XDC/USD are absolute (unit multiplier
    # applied); the PT_* are percentages.
    UNIT_TO_COLUMN = {
        "PT_B1GQ": "pct_gdp",
        "PT_OTR_SECTOR": "pct_institutional_sector",
        "PT_OTR_REV_CAT": "pct_revenue_category",
        "XDC": "value_national_currency",
        "USD": "value_usd",
    }
    ABSOLUTE_UNITS = {"XDC", "USD"}  # apply UNIT_MULT to these

    DATA_TABLES = ["revenue"]
    # every table materialised (data + dictionary), in upload/dbt order
    ALL_TABLES = ["revenue", "dicionario"]

    # Repo-relative architecture directory (single source of truth).
    ARCHITECTURE_DIR = (
        Path(__file__).resolve().parents[3]
        / "models"
        / "world_oecd_revenue_statistics"
        / "code"
        / "architecture"
    )

    # Codelists needed for the hierarchy and the dictionary.
    CL_STANDARD_REVENUE = "CL_STANDARD_REVENUE"
    CL_SECTOR = "CL_SECTOR"
    CL_OBS_STATUS = "CL_OBS_STATUS"
    CL_AREA = "CL_AREA"
