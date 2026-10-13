"""Constants for the au_dcceew_greenhouse recurring pipeline (Prefect 3).

Australia's National Greenhouse Gas Inventory from the DCCEEW Australian National
Greenhouse Accounts (ANGA) OData API. See models/au_dcceew_greenhouse/ for the
one-shot onboarding; the cleaning transform lives once, in this package's
``utils.py``, and both the bootstrap and this pipeline import it.

The ingested feed is the **annual** UNFCCC inventory (annual ``InventoryYear``),
which advances once a year with each National Inventory Report. The separate
NGGI *quarterly* fast-estimate product is not ingested. The flow is therefore
polled on the annual max year and merely *checked* on a quarterly cadence: most
scheduled runs no-op until a new annual year appears, at which point the full
history is re-materialized (the source restates prior years each release).
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the au_dcceew_greenhouse pipeline (lowercase per repo convention)."""

    DATASET_ID = "au_dcceew_greenhouse"

    # dcceew.gov.au 403s scripts, but the ANGA OData host serves fine with a
    # browser User-Agent. The server ignores $top and rejects $count, so each
    # call streams the whole entity set.
    BASE_URL = "https://greenhouseaccounts.climatechange.gov.au/OData"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    # OData family prefix -> our table slug.
    FAMILIES = {
        "AR5_ParisInventory": "inventory_unfccc",
        "AR5_ANZSIC": "inventory_anzsic",
        "AR5_ScopeTwo": "inventory_scope2",
    }

    # OData entity-set jurisdiction token -> our lowercase geography code.
    JURISDICTIONS = {
        "AUSTRALIA": "australia",
        "ACT": "act",
        "ET": "et",
        "NSW": "nsw",
        "NT": "nt",
        "QLD": "qld",
        "SA": "sa",
        "TAS": "tas",
        "VIC": "vic",
        "WA": "wa",
    }

    # (category-hierarchy prefix, max levels, carries gas hierarchy) per table.
    TABLE_SCHEME = {
        "inventory_unfccc": ("UNFCCC_Level_", 11, True),
        "inventory_anzsic": ("ANZSIC_Level_", 7, True),
        "inventory_scope2": ("ScopeTwo_Level_", 3, False),
    }
    GAS_MAX_LEVELS = 4

    DATA_TABLES = ["inventory_unfccc", "inventory_anzsic", "inventory_scope2"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "au_dcceew_greenhouse"
        / "code"
        / "architecture"
    )
