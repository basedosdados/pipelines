"""Constants for the us_nsf_ncses recurring pipeline (Prefect 3).

Two NCSES surveys refreshed by one flow: the Higher Education Research and
Development (HERD) public use files and the Survey of Earned Doctorates (SED)
published data tables. Both are annual. See ``models/us_nsf_ncses/README.md``.
"""

from enum import Enum


class constants(Enum):
    """Constants for the us_nsf_ncses pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_nsf_ncses"

    # Tables fed by each survey, in the order they are registered on the
    # backend. `dicionario` is written by the HERD half but is referenced by the
    # `custom_dictionary_coverage` test on every table, so it is built first.
    DICTIONARY_TABLE = "dicionario"
    HERD_TABLES = (
        "herd_institution",
        "herd_expenditure",
        "herd_personnel",
        "herd_survey_item",
    )
    SED_TABLES = ("sed_estimate", "sed_data_table")
    ALL_TABLES = (
        "dicionario",
        "herd_institution",
        "herd_expenditure",
        "herd_personnel",
        "herd_survey_item",
        "sed_estimate",
        "sed_data_table",
    )

    # The table each survey's source poll is anchored on. HERD and SED publish
    # on their own calendars, so each is polled separately and either one being
    # new is enough to justify the run.
    HERD_POLL_TABLE = "herd_expenditure"
    SED_POLL_TABLE = "sed_estimate"

    # Both surveys carry a bare year, so the poll compares years, not dates.
    # A finer format here would silently compare a year against a full date and
    # make the pipeline look annual when it is not.
    DATE_FORMAT = "%Y"
