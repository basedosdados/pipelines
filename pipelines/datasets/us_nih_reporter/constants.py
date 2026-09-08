"""Constants for the us_nih_reporter recurring pipeline (Prefect 3).

NIH RePORTER ExPORTER bulk files. See ``models/us_nih_reporter/CLAUDE.md`` for
the full design, including the source traps the transform exists to neutralise.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the us_nih_reporter pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums. ``ARCHITECTURE_DIR`` points at the architecture CSVs under
    ``models/us_nih_reporter/code/architecture``, shared with the one-shot
    bootstrap so the two can never drift.
    """

    DATASET_ID = "us_nih_reporter"

    PROJECT = "project"
    PROJECT_ABSTRACT = "project_abstract"
    PUBLICATION = "publication"
    PUBLICATION_LINK = "publication_link"
    PATENT_LINK = "patent_link"
    CLINICAL_STUDY_LINK = "clinical_study_link"
    DICIONARIO = "dicionario"

    # Order matters only in that every table is built before any is tested:
    # project's custom_dictionary_coverage test reads dicionario.
    ALL_TABLES = [
        "project",
        "project_abstract",
        "publication",
        "publication_link",
        "patent_link",
        "clinical_study_link",
        "dicionario",
    ]

    # Tables partitioned by ``year``. The other three carry no year at all: the
    # source publishes patents and clinical studies as a single all-fiscal-years
    # file, and dicionario is a register.
    PARTITIONED_TABLES = [
        "project",
        "project_abstract",
        "publication",
        "publication_link",
    ]

    ARCHITECTURE_DIR = (
        _REPO_ROOT / "models" / "us_nih_reporter" / "code" / "architecture"
    )

    REFERENCE_DIR = (
        _REPO_ROOT / "models" / "us_nih_reporter" / "code" / "reference"
    )

    BASE_URL = "https://reporter.nih.gov/exporter"

    # Source family -> (url path, clean table, year kind).
    # ``fiscal`` families are keyed by NIH fiscal year, ``calendar`` ones by the
    # calendar year of publication release, and ``none`` families ship a single
    # file covering every year.
    FAMILIES = {
        "projects": ("projects", "project", "fiscal"),
        "abstracts": ("abstracts", "project_abstract", "fiscal"),
        "publications": ("publications", "publication", "calendar"),
        "linktables": ("linktables", "publication_link", "calendar"),
        "patents": ("patents", "patent_link", "none"),
        "clinicalstudies": ("clinicalstudies", "clinical_study_link", "none"),
    }

    # The two release cadences, which is why there are two flows.
    #
    # The year-keyed families are rebuilt once a year, at the close of a fiscal
    # year, and restated in occasional sweeps: FY2008 through FY2023 all carried
    # 2025-06-16, and FY1985-FY2005 have not moved since 2010.
    #
    # The two all-fiscal-years families are rewritten roughly weekly — both
    # carried 2026-09-07 when this was built — and together weigh 16 MB, so
    # rebuilding the 3.6 GB annual corpus alongside them would be waste.
    ANNUAL_FAMILIES = ["projects", "abstracts", "publications", "linktables"]
    LINK_FAMILIES = ["patents", "clinicalstudies"]

    ANNUAL_TABLES = [
        "project",
        "project_abstract",
        "publication",
        "publication_link",
        "dicionario",
    ]
    LINK_TABLES = ["patent_link", "clinical_study_link"]

    # First year published per family. Projects and abstracts start at FY1985;
    # publications and their link tables start at calendar year 1980.
    FIRST_FISCAL_YEAR = 1985
    FIRST_CALENDAR_YEAR = 1980

    # The FY1985-FY1999 accessory file. The main project files for those years
    # carry no TOTAL_COST, FUNDING_ICS or ORG_DUNS; NIH publishes them separately
    # and documents that they join on APPLICATION_ID. Reached through the generic
    # document service rather than the /exporter/<family>/download/<year> route.
    FUNDING_SUPPLEMENT_URL = (
        "https://reporter.nih.gov/services/exporter/"
        "DownloadFromDocService?DocType=EXPFND&KeyId=1985"
    )
    FUNDING_SUPPLEMENT_YEARS = list(range(1985, 2000))

    # Header drift across eras. ExPORTER renamed two columns without changing
    # their meaning; every file is read by header name and normalised here.
    #   CFDA_CODE  -> ASSISTANCE_LISTING_NUMBER  (renamed for FY2025)
    #   FOA_NUMBER -> "OPPORTUNITY NUMBER"       (renamed for FY2006; note the
    #                                             space, which is in the file)
    HEADER_RENAME = {
        "CFDA_CODE": "ASSISTANCE_LISTING_NUMBER",
        "FOA_NUMBER": "OPPORTUNITY NUMBER",
    }

    # Columns whose stored values are codes the reader cannot interpret without
    # a label set. Everything else the source publishes is already a readable
    # label (FUNDING_MECHANISM, ED_INST_TYPE, ORG_COUNTRY, Study Status) or has
    # its label in a sibling column (STUDY_SECTION -> STUDY_SECTION_NAME).
    DICT_COLUMNS = {
        "project": [
            "application_type",
            "activity",
            "administering_ic",
            "arra_funded",
        ]
    }

    # Subset of DICT_COLUMNS asserted complete by the dbt
    # custom_dictionary_coverage test. Only the two closed label sets the source
    # documents qualify: `activity` has an official register that covers 258 of
    # the 372 observed codes and leaves contract, intramural and non-NIH codes
    # unlabelled, and `administering_ic` is labelled from the data's own
    # ic_name column, which is itself blank on some rows.
    DICT_TEST_COLUMNS = {"project": ["application_type", "arra_funded"]}

    # Documented meanings of APPLICATION_TYPE, from the ExPORTER data dictionary.
    # Values 6 and 8 occur in the data (4,885 and 2,213 rows) but the dictionary
    # does not define them; they are registered as unknown rather than dropped.
    APPLICATION_TYPE_LABELS = {
        "1": "New application",
        "2": "Competing continuation (competing renewal)",
        "3": "Application for additional (supplemental) support",
        "4": (
            "Competing extension for an R37 award, or first non-competing year "
            "of a Fast-Track SBIR/STTR award"
        ),
        "5": "Non-competing continuation",
        "6": "Not defined in the ExPORTER data dictionary",
        "7": "Change of grantee institution",
        "8": "Not defined in the ExPORTER data dictionary",
        "9": "Change of NIH awarding Institute or Division",
    }

    ARRA_FUNDED_LABELS = {
        "Y": (
            "Supported by funds appropriated through the American Recovery and "
            "Reinvestment Act of 2009"
        ),
        "N": "Not supported by American Recovery and Reinvestment Act funds",
    }

    # How many fiscal years back to re-materialise on a scheduled run. NIH
    # rebuilds the closing fiscal year's project and abstract files and updates
    # the three prior fiscal years with any award modifications, so a refresh
    # that only appended the newest year would leave those restatements behind.
    REFRESH_FISCAL_YEARS = 4

    # Calendar years of publication and link files refreshed on each run.
    REFRESH_CALENDAR_YEARS = 2

    # The exporter endpoint intermittently answers 404 for a file that exists;
    # one observed on 176 downloads, which succeeded on the next attempt.
    DOWNLOAD_ATTEMPTS = 4
    DOWNLOAD_RETRY_SLEEP = 10
    REQUEST_TIMEOUT = 900
