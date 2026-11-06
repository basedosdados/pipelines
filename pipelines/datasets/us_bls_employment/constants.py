"""Constants for the us_bls_employment recurring pipeline (Prefect 3).

Three BLS employment programs published as `time.series` flat files:
Current Employment Statistics national (`ce`) and state/metro (`sm`), Local Area
Unemployment Statistics (`la`), and the Job Openings and Labor Turnover Survey
(`jt`). See models/us_bls_employment/ONBOARDING_PLAN.md for the full design.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_bls_employment pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/us_bls_employment/code/``, the schema source of truth for both this
    pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "us_bls_employment"

    # download.bls.gov 403s without a browser User-Agent; BLS asks for a contact
    # email in the UA string.
    BASE_URL = "https://download.bls.gov/pub/time.series"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # table slug -> BLS program directory
    PROGRAMS = {
        "ces_national": "ce",
        "ces_state_metro": "sm",
        "laus": "la",
        "jolts": "jt",
    }

    DATA_TABLES = ["ces_national", "ces_state_metro", "laus", "jolts"]
    ALL_TABLES = [*DATA_TABLES, "dicionario"]

    # Small dimension/lookup files pulled per program. Every program ships
    # `.series`, `.period`, `.seasonal` and `.footnote`; the rest differ.
    DIM_FILES = {
        "ce": [
            "series",
            "period",
            "seasonal",
            "footnote",
            "datatype",
            "supersector",
            "industry",
        ],
        "sm": [
            "series",
            "period",
            "seasonal",
            "footnote",
            "data_type",
            "supersector",
            "industry",
            "area",
            "state",
        ],
        "la": [
            "series",
            "period",
            "seasonal",
            "footnote",
            "area",
            "area_type",
            "measure",
            "state_region_division",
        ],
        "jt": [
            "series",
            "period",
            "seasonal",
            "footnote",
            "industry",
            "state",
            "area",
            "sizeclass",
            "dataelement",
            "ratelevel",
        ],
    }

    # Which observation files make a complete, minimal-overlap history.
    #
    # `ce` and `jt` each publish one all-series file; their numbered siblings are
    # slices of it. `sm` and `la` publish per-state files (the documented
    # complete set) — `sm.data.1.AllData` is an undocumented union of them and
    # `la.data.0.CurrentU*` covers only 1990 forward, so neither is used.
    # Observations are still deduplicated on the natural key, because the
    # per-state files overlap the statewide and region/division files.
    DATA_FILES = {
        "ce": ["ce.data.0.AllCESSeries"],
        "jt": ["jt.data.1.AllItems"],
        "sm": r"sm\.data\.\d+[a-c]?\.(?!.*\.)(?!AllData$)(?!Current$)",
        "la": r"la\.data\.(([7-9]|[1-9]\d)\.|[1-5]\.(CurrentS|AllStates|Region))",
    }

    # Natural key an observation is deduplicated on before the dimension join.
    OBS_KEY = ["series_id", "year", "period"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_bls_employment" / "code" / "architecture"
    )

    # Per-row measurement unit, by the program dimension that fixes the unit.
    # A long fact table mixes units in one `value` column, so the unit travels
    # with the row rather than sitting in column-level metadata.
    CE_UNITS = {
        "01": "thousand_person",
        "02": "hour",
        "03": "USD",
        "04": "hour",
        "06": "thousand_person",
        "07": "hour",
        "08": "USD",
        "09": "hour",
        "10": "thousand_person",
        "11": "USD",
        "12": "USD",
        "13": "USD",
        "15": "USD",
        "16": "index",
        "17": "index",
        "19": "hour",
        "20": "hour",
        "21": "index",
        "22": "index",
        "23": "index",
        "24": "index",
        "25": "thousand_person",
        "26": "thousand_person",
        "30": "USD",
        "31": "USD",
        "32": "USD",
        "33": "USD",
        "34": "index",
        "35": "index",
        "36": "hour",
        "37": "hour",
        "56": "thousand_hour",
        "57": "thousand_USD",
        "58": "thousand_hour",
        "81": "thousand_hour",
        "82": "thousand_USD",
        "83": "thousand_hour",
        "C1": "percent",
        "C2": "percent",
        "C3": "percent",
        "RR": "percent",
    }
    SM_UNITS = {
        "01": "thousand_person",
        "02": "hour",
        "03": "USD",
        "06": "thousand_person",
        "07": "hour",
        "08": "USD",
        "11": "USD",
        "21": "index",
        "22": "index",
        "23": "index",
        "24": "index",
        "26": "thousand_person",
        "30": "USD",
    }
    # LAUS publishes levels as persons, not thousands.
    LA_UNITS = {
        "03": "percent",
        "04": "person",
        "05": "person",
        "06": "person",
        "07": "percent",
        "08": "percent",
        "09": "person",
    }
    # sm.area codes that are not CBSA codes, beyond the metropolitan divisions
    # (which are identified from their own names): 00000 is the statewide
    # aggregate, 99999 all metropolitan areas, and 93561 a BLS-specific New
    # York City code with no CBSA equivalent.
    SM_NON_CBSA_AREAS = ["00000", "99999", "93561"]

    # Census regions JOLTS stores in its state_code field.
    JT_REGIONS = ["MW", "NE", "SO", "WE"]

    # JOLTS levels are in thousands; rates are percent of employment. The
    # unemployed-per-job-opening series (UO) is a dimensionless ratio.
    JT_UNITS = {"L": "thousand_person", "R": "percent"}
