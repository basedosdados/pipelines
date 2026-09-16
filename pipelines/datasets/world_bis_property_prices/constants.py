"""Constants for the world_bis_property_prices recurring pipeline (Prefect 3).

BIS Selected residential property prices (dataset code ``WS_SPP``), the single
bulk flat CSV published at data.bis.org. See
models/world_bis_property_prices/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSV (the single schema source of
# truth — column order + bigquery_type for the one table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the world_bis_property_prices pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSV under
    ``models/world_bis_property_prices/code/``, the schema source of truth for
    both this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "world_bis_property_prices"

    # BIS Data Portal bulk download: the "selected" residential property prices
    # dataset, flat (long) CSV inside a zip. A single file carries all series.
    BULK_URL = "https://data.bis.org/static/bulk/WS_SPP_csv_flat.zip"
    CSV_NAME = "WS_SPP_csv_flat.csv"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    TABLES = ["price_index"]

    # BIS VALUE dimension code -> readable label.
    VALUE_LABEL = {"N": "Nominal", "R": "Real"}
    # BIS UNIT_MEASURE code -> the statistic reported (the `measure` column).
    MEASURE_BY_UNIT = {"628": "index", "771": "year-on-year change"}
    # BIS reference-area aggregates (no ISO3 country; country_id stays NULL).
    AGGREGATES = {
        "4T": "Emerging market economies (aggregate)",
        "5R": "Advanced economies",
        "XM": "Euro area",
        "XW": "World",
    }
    # BIS reference-area ISO2 -> ISO3, for the FK to br_bd_diretorios_mundo.pais.
    # Sourced from the world country directory (sigla_iso2 -> sigla_iso3); every
    # economy BIS currently publishes maps cleanly. A future BIS economy absent
    # here gets a NULL country_id and is logged by the transform.
    COUNTRY_ISO3 = {
        "AT": "AUT",
        "AU": "AUS",
        "BE": "BEL",
        "BG": "BGR",
        "BR": "BRA",
        "CA": "CAN",
        "CH": "CHE",
        "CL": "CHL",
        "CN": "CHN",
        "CO": "COL",
        "CY": "CYP",
        "CZ": "CZE",
        "DE": "DEU",
        "DK": "DNK",
        "EE": "EST",
        "ES": "ESP",
        "FI": "FIN",
        "FR": "FRA",
        "GB": "GBR",
        "GR": "GRC",
        "HK": "HKG",
        "HR": "HRV",
        "HU": "HUN",
        "ID": "IDN",
        "IE": "IRL",
        "IL": "ISR",
        "IN": "IND",
        "IS": "ISL",
        "IT": "ITA",
        "JP": "JPN",
        "KR": "KOR",
        "LT": "LTU",
        "LU": "LUX",
        "LV": "LVA",
        "MA": "MAR",
        "MK": "MKD",
        "MT": "MLT",
        "MX": "MEX",
        "MY": "MYS",
        "NL": "NLD",
        "NO": "NOR",
        "NZ": "NZL",
        "PE": "PER",
        "PH": "PHL",
        "PL": "POL",
        "PT": "PRT",
        "RO": "ROU",
        "RS": "SRB",
        "RU": "RUS",
        "SE": "SWE",
        "SG": "SGP",
        "SI": "SVN",
        "SK": "SVK",
        "TH": "THA",
        "TR": "TUR",
        "US": "USA",
        "ZA": "ZAF",
    }

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "world_bis_property_prices"
        / "code"
        / "architecture"
    )
