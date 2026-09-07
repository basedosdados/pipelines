"""Constants for the us_noaa_storm_events recurring pipeline (Prefect 3).

NOAA NCEI Storm Events Database. See ``models/us_noaa_storm_events/CLAUDE.md``
for the full design, including the four source traps the transform exists to
neutralise.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture TSVs (the single schema source of
# truth — column order, bigquery_type and the raw -> clean name mapping).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_noaa_storm_events pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture TSVs under
    ``models/us_noaa_storm_events/code/architecture``, shared with the one-shot
    bootstrap.
    """

    DATASET_ID = "us_noaa_storm_events"

    EVENT = "event"
    FATALITY = "fatality"
    EVENT_LOCATION = "event_location"
    DICIONARIO = "dicionario"
    # Order matters only in that every table is built before any is tested:
    # event's custom_dictionary_coverage test reads dicionario.
    ALL_TABLES = ["event", "fatality", "event_location", "dicionario"]

    ARCHITECTURE_DIR = (
        _REPO_ROOT
        / "models"
        / "us_noaa_storm_events"
        / "code"
        / "architecture"
    )

    # The bulk CSV directory. One gzipped CSV per (family, year), named
    # StormEvents_<family>-ftp_v1.0_d<YYYY>_c<YYYYMMDD>.csv.gz, where the `c`
    # token is the file's creation date. Per the directory's own README, a change
    # in that token is the signal that the year has been restated — which is how
    # the pipeline decides what to re-materialise. Whole years are rewritten in
    # bulk (all 77 `details` files carried c20260323 when this was built), so an
    # append-only refresh would duplicate the corpus rather than extend it.
    BASE_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
    LISTING_URL = BASE_URL + "/"

    # Source family -> clean table. The three families are linked by EVENT_ID.
    FAMILIES = {
        "details": "event",
        "fatalities": "fatality",
        "locations": "event_location",
    }

    FIRST_YEAR = 1950

    # NWS assigns its own pseudo-FIPS state codes to the territories, which do
    # not match the real FIPS codes the br_bd_diretorios_us directory keys on.
    # Verified against the corpus: every occurrence of each territory carries
    # only the NWS code, never the real one.
    #   99 PUERTO RICO -> 72, 98 GUAM -> 66,
    #   97 AMERICAN SAMOA -> 60, 96 VIRGIN ISLANDS -> 78
    # Puerto Rico's municipio codes then resolve directly (99127 SAN JUAN ->
    # 72127); Guam's and American Samoa's county-level codes are an NWS scheme
    # of their own and resolve to nothing, so they stay null.
    STATE_FIPS_REMAP = {"99": "72", "98": "66", "97": "60", "96": "78"}

    # State codes 81-95 are marine and Great Lakes zones, not states: GULF OF
    # MEXICO, ATLANTIC NORTH/SOUTH, E PACIFIC, the five Great Lakes, LAKE ST
    # CLAIR, ST LAWRENCE R, GULF OF ALASKA, HAWAII WATERS, GUAM WATERS. They
    # carry no FIPS code, so state_id and county_id are null on those rows
    # (51,642 of 2,041,816 events) while state_name keeps the published value.
    MARINE_FIPS_MIN = 81
    MARINE_FIPS_MAX = 95

    # Columns enumerated in the `dicionario` table: the coded vocabularies whose
    # stored values are abbreviations or scale labels a reader cannot interpret
    # unaided. event_type is included because its vocabulary is not stable —
    # 57 distinct values appear over the record, against the 48 the NWS
    # directive defines — and cobertura_temporal is what exposes that.
    DICT_COLUMNS = {
        "event": [
            "event_type",
            "cz_type",
            "magnitude_type",
            "flood_cause",
            "tornado_scale",
            "data_source",
        ],
        "fatality": ["fatality_type", "sex", "location"],
    }

    # Damage magnitude suffixes, as they actually occur in the corpus. The
    # documented set is K/M/B; lowercase k, a lone T and a lone H also occur.
    # T and H are NOT decoded: one row each carries "2h"/"5H" and two carry a
    # bare "K"/"M" with no number, and guessing at hundreds or trillions would
    # invent a value. Those 49 rows parse to null with the raw string kept.
    DAMAGE_MULTIPLIER = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9}
