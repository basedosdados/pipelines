"""Constants for the FBI Crime Data Explorer (us_fbi_cde) dataset."""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Source endpoints, file inventory and local paths."""

    DATASET_ID = "us_fbi_cde"

    # The CDE download page never exposes a static object URL. It asks an
    # unauthenticated endpoint for a presigned S3 URL, one object at a time, and
    # the presigned URL expires after 900 seconds. A key that does not exist is
    # simply absent from the response, which doubles as an existence check.
    SIGNED_URL_ENDPOINT = "https://cde.ucr.cjis.gov/LATEST/s3/signedurl"
    DOWNLOADS_CATALOGUE = "https://cde.ucr.cjis.gov/LATEST/webapp/assets/JSON/downloads/downloads.json"
    MASTER_FILE_CATALOGUE = "https://cde.ucr.cjis.gov/LATEST/webapp/assets/JSON/downloads/masters.json"
    LANDING_PAGE = "https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/downloads"

    # Per-state, per-year NIBRS relational bundles: one zip of normalised CSVs.
    NIBRS_INCIDENT_KEY = "nibrs/incident/{year}/{state}-{year}.zip"
    NIBRS_FIRST_YEAR = 1991

    # Return A summary master files: one fixed-width flat file per year.
    RETA_KEY = "master_files/reta/reta-{year}.zip"
    RETA_FIRST_YEAR = 1985

    # Pre-parsed CSV extracts published alongside the master files.
    HATE_CRIME_KEY = "additional-datasets/hate-crime/hate_crime.zip"
    LEE_KEY = "additional-datasets/law-enforcement/lee_1960_2025.csv"
    PARTICIPATION_KEY = (
        "additional-datasets/ucr/ucr_participation_1960_2025.csv"
    )

    # Documentation bundled as auxiliary files.
    NIBRS_DATA_DICTIONARY_KEY = "nibrs/_all/NIBRS_DataDictionary.pdf"
    NIBRS_DIAGRAM_KEY = "nibrs/_all/nibrs_diagram.pdf"
    RETA_HELP_KEY = "master_files/reta/reta-help.zip"

    # US Census ANSI county list, used to attach county FIPS codes to the
    # agency table (the CDE publishes county names only).
    CENSUS_COUNTY_ANSI = "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt"

    STATES = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "PR",
        "GM",
        "AS",
        "VI",
        "CZ",
        "NB",
    ]

    # Scratch location. Never inside the repo or Dropbox: the raw bundles are
    # ~14 GB compressed and the intermediate CSVs are far larger.
    DATA_ROOT = Path(
        os.environ.get(
            "US_FBI_CDE_DATA", Path.home() / "Downloads" / "us_fbi_cde_data"
        )
    )

    ARCHITECTURE_DIR = (
        REPO_ROOT / "models" / "us_fbi_cde" / "code" / "architecture"
    )

    TABLES = (
        "agency",
        "incident",
        "offense",
        "offender",
        "victim",
        "victim_offense",
        "victim_offender_relationship",
        "arrestee",
        "property",
        "hate_crime",
        "ucr_summary",
        "dicionario",
    )
