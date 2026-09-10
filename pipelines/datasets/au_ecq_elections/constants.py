"""Constants for au_ecq_elections — Electoral Commission of Queensland results and disclosures."""

import os
from enum import Enum
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ARCHITECTURE_DIR = (
    REPO_ROOT / "models" / "au_ecq_elections" / "code" / "architecture"
)


def data_root() -> Path:
    """Scratch location for raw downloads and cleaned parquet (never inside the repo)."""
    return Path(
        os.environ.get(
            "ECQ_DATA", Path.home() / "Downloads" / "au_ecq_elections_data"
        )
    )


class constants(Enum):
    DATASET_ID = "au_ecq_elections"
    BACKEND_SLUG = "qld_elections"

    RESULTS_BASE_URL = "https://resultsdata.elections.qld.gov.au"
    RESULTS_SITE_URL = "https://results.elections.qld.gov.au"
    DISCLOSURES_BASE_URL = "https://disclosures.ecq.qld.gov.au"

    # elections.json drives everything: it lists every event, its archive zip and its
    # electorates/boundary_venues JSON companions.
    ELECTIONS_INDEX = (
        "https://results.elections.qld.gov.au/data/elections.json"
    )

    # Events published without an `archiveXML` key. MASC23 (the 2023 Mapoon Aboriginal
    # Shire Council Councillor By-election) has no results archive at all, so it carries
    # a contest and a candidate in electorates.json but no counts anywhere. It is
    # excluded explicitly rather than dropped silently.
    EXCLUDED_ELECTION_STUBS = ["MASC23"]

    # Disclosure routes. The `/Report/<Type>Csv` pattern advertised by the site's own
    # JavaScript is dead code and 404s; these are the routes that actually serve data.
    DISCLOSURE_MAP_EXPORTS = {
        "map_gifts_state": ("/Map/ExportCsv", "State"),
        "map_gifts_local": ("/Map/ExportCsv", "Local"),
    }
    DISCLOSURE_EXPENDITURE_EXPORT = "/Expenditures/ExportCsv"
    DISCLOSURE_REPORT_EXPORTS = [
        "Gifts",
        "LoansReceived",
        "ForeignProperty",
        "PeriodicReturns",
        "ElectionSummaries",
        "AdvertisersData",
        "Expenditure",
    ]

    # Source dialects. Gifts and report dates use dashes, expenditure uses slashes.
    # Both are unambiguously day-first (17,304 gift rows have day > 12).
    DATE_FORMAT_GIFT = "%d-%m-%Y"
    DATE_FORMAT_EXPENDITURE = "%d/%m/%Y"

    # Two expenditure rows are dated 1924 and are data-entry typos for 2024: both are
    # attached to the 2024 Local Government Elections and are small consumer purchases.
    # Repaired rather than dropped, so the partition range is not dragged back a century.
    EXPENDITURE_YEAR_REPAIRS = {1924: 2024}

    # Australian Statistical Geography Standard vintage used for the district link.
    # All 93 Queensland state districts in ECQ's 2020-2026 coverage match the 2021
    # vintage exactly; the 2016 vintage matches only 77 because of the 2017 redistribution.
    SED_VINTAGE = "2021"
    LGA_VINTAGE = "2021"

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    # Every table in the dataset, in publication order.
    TABLES = [
        "election",
        "candidate",
        "enrolment_turnout",
        "result_district",
        "result_voting_centre",
        "distribution_of_preferences",
        "voting_centre",
        "disclosure_gift",
        "disclosure_expenditure",
        "disclosure_return",
        "dicionario",
    ]

    PARTITIONED_BY_YEAR = [t for t in TABLES if t != "dicionario"]
