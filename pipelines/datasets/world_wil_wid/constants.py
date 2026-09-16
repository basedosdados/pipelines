"""Constants for the world_wil_wid recurring pipeline (Prefect 3).

World Inequality Database (WID.world), full bulk dataset. See
models/world_wil_wid/ONBOARDING_PLAN.md for the full design and for the four
source traps the cleaning code works around.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (the single schema source of
# truth -- column order + bigquery_type per table).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the world_wil_wid pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/world_wil_wid/code/``, which are the schema source of truth for
    both this pipeline and the one-shot bootstrap.
    """

    DATASET_ID = "world_wil_wid"

    # The single "download full dataset" artifact behind https://wid.world/data/.
    # ~882 MB zip, ~6.4 GB across 848 `;`-delimited CSVs. Rebuilt by WID a few
    # times a year with no published schedule; the HEAD Last-Modified is the
    # only release signal the source exposes.
    BULK_URL = "https://wid.world/bulk_download/wid_all_data.zip"
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36 rdahis@basedosdados.org"
    )

    # Zip member name prefixes.
    DATA_PREFIX = "WID_data_"
    METADATA_PREFIX = "WID_metadata_"
    COUNTRIES_MEMBER = "WID_countries.csv"

    # TRAP 1 -- the archive carries a pair of header-only artifacts dated
    # 2024-02-14, `WID_data_Al.csv` (47 bytes) and `WID_metadata_Al.csv`
    # (168 bytes), whose names differ from Albania's real `WID_data_AL.csv`
    # (19.7 MB) and `WID_metadata_AL.csv` (2.1 MB) only by case. On a
    # case-insensitive filesystem -- macOS/APFS, the default -- extracting the
    # archive overwrites BOTH of Albania's files with the stubs, and the
    # country vanishes with no error. Never extract these two members.
    JUNK_MEMBERS = frozenset({"WID_data_Al.csv", "WID_metadata_Al.csv"})

    # Table slugs, in publication order.
    TABLES = ["indicator", "series", "country", "dicionario"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "world_wil_wid" / "code" / "architecture"
    )

    # Partition range for the `indicator` table. The source spans 1800-2025;
    # the upper bound carries the house +5 years of headroom.
    YEAR_MIN = 1800
    YEAR_MAX = 2030

    # Columns of the raw data files. TRAP 2 -- the 76 regional-aggregate files
    # (WO, and the O*/Q*/X* prefixes) ship a 7-column header without
    # `data_quality`, so every read must union by name rather than pin a
    # fixed schema. Pinning it and passing `ignore_errors` silently discards
    # 36.6M aggregate rows, World included.
    DATA_COLUMNS = [
        "country",
        "variable",
        "percentile",
        "year",
        "value",
        "age",
        "pop",
        "data_quality",
    ]

    # Columns of the raw metadata files; the same 76 files omit the trailing
    # `data_quality_score`.
    METADATA_COLUMNS = [
        "country",
        "variable",
        "age",
        "pop",
        "countryname",
        "shortname",
        "simpledes",
        "technicaldes",
        "shorttype",
        "longtype",
        "shortpop",
        "longpop",
        "shortage",
        "longage",
        "unit",
        "source",
        "method",
        "extrapolation",
        "data_points",
        "data_quality_score",
    ]

    # Dictionary-covered columns and the `series` columns their labels come
    # from. Labels are lifted out of WID's own metadata rather than transcribed
    # from the website, so the dictionary cannot drift from the data.
    # `data_quality` is deliberately absent -- WID publishes no codebook for it.
    DICTIONARY_SOURCES = {
        "age": "age_description",
        "pop": "pop_description",
        "series_type": "type_name",
    }
