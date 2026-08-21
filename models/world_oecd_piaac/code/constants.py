"""Source manifest for the PIAAC onboarding.

Everything that varies by cycle, round or country lives here so the download,
cleaning and architecture scripts share one definition.

PIAAC carries no year-of-collection variable, so `year` is derived from the
cycle and round a country took part in:

    Cycle 1 Round 1  data collected Aug 2011 - Mar 2012   -> 2012  (22 PUFs)
    Cycle 1 Round 2  data collected Apr 2014 - Mar 2015   -> 2015  ( 8 PUFs)
    Cycle 1 Round 3  data collected Jul - Dec 2017        -> 2017  ( 6 PUFs)
    Cycle 2 Round 1  data collected Sep 2022 - Aug 2023   -> 2023  (30 PUFs)

Australia, Cyprus and Indonesia took part in Cycle 1 but publish no international
PUF; the Netherlands Cycle 2 PUF is pending national authorisation. None of them
appear below, so the manifest is exactly what is downloadable.
"""

from __future__ import annotations

import os
from pathlib import Path

DATA_ROOT = Path(
    os.environ.get(
        "PIAAC_DATA_ROOT", Path.home() / "Downloads" / "world_oecd_piaac_data"
    )
)
INPUT_ROOT = DATA_ROOT / "input"
OUTPUT_ROOT = DATA_ROOT / "output"
DOCS_ROOT = INPUT_ROOT / "docs"

PUF_BASE = "https://webfs.oecd.org/piaac"
# www.oecd.org serves HTML behind a bot filter that rejects non-browser agents; the
# /content/dam/ document paths and webfs.oecd.org both need the same header.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

ROUND_YEAR = {
    ("1", "1"): 2012,
    ("1", "2"): 2015,
    ("1", "3"): 2017,
    ("2", "1"): 2023,
}

# ISO 3166-1 numeric (M49), used by PIAAC's CNTRYID.
COUNTRY_M49 = {
    "AUT": "040",
    "BEL": "056",
    "CAN": "124",
    "CHE": "756",
    "CHL": "152",
    "CZE": "203",
    "DEU": "276",
    "DNK": "208",
    "ECU": "218",
    "ESP": "724",
    "EST": "233",
    "FIN": "246",
    "FRA": "250",
    "GBR": "826",
    "GRC": "300",
    "HRV": "191",
    "HUN": "348",
    "IRL": "372",
    "ISR": "376",
    "ITA": "380",
    "JPN": "392",
    "KAZ": "398",
    "KOR": "410",
    "LTU": "440",
    "LVA": "428",
    "MEX": "484",
    "NLD": "528",
    "NOR": "578",
    "NZL": "554",
    "PER": "604",
    "POL": "616",
    "PRT": "620",
    "RUS": "643",
    "SGP": "702",
    "SVK": "703",
    "SVN": "705",
    "SWE": "752",
    "TUR": "792",
    "USA": "840",
}

# (iso3, cycle, round, remote file name, delimiter, zipped, is_national_format)
#
# Delimiters genuinely differ: Cycle 1 is comma, Cycle 2 is semicolon with quoted
# fields, and the US Round 3 file is pipe-delimited with uppercase column names.
# Israel and Korea Cycle 2 were re-released on 2026-01-29 as zips to correct
# earnings data; each contains a readme listing the corrected variables.
PUF_FILES: list[tuple[str, str, str, str, str, bool, bool]] = [
    # --- Cycle 1, Round 1 (2011-2012) ---
    *[
        (iso, "1", "1", f"prg{iso.lower()}p1.csv", ",", False, False)
        for iso in (
            "AUT",
            "BEL",
            "CAN",
            "CZE",
            "DEU",
            "DNK",
            "ESP",
            "EST",
            "FIN",
            "FRA",
            "GBR",
            "IRL",
            "ITA",
            "JPN",
            "KOR",
            "NLD",
            "NOR",
            "POL",
            "RUS",
            "SVK",
            "SWE",
        )
    ],
    ("USA", "1", "1", "prgusap1_2012.csv", ",", False, False),
    # --- Cycle 1, Round 2 (2014-2015) ---
    *[
        (iso, "1", "2", f"prg{iso.lower()}p1.csv", ",", False, False)
        for iso in ("CHL", "GRC", "ISR", "LTU", "NZL", "SGP", "SVN", "TUR")
    ],
    # --- Cycle 1, Round 3 (2017) ---
    *[
        (iso, "1", "3", f"prg{iso.lower()}p1.csv", ",", False, False)
        for iso in ("ECU", "HUN", "KAZ", "MEX", "PER")
    ],
    # The US Round 3 file is the US *national* PUF: it suppresses 110 international
    # variables (including AGE_R) and adds 131 US-only ones, so it cannot be stacked
    # into the harmonised table and gets its own.
    ("USA", "1", "3", "Prgusap1_2017.csv", "|", False, True),
    # --- Cycle 2, Round 1 (2022-2023) ---
    *[
        (iso, "2", "1", f"prg{iso.lower()}p2.csv", ";", False, False)
        for iso in (
            "AUT",
            "BEL",
            "CAN",
            "CHE",
            "CHL",
            "CZE",
            "DEU",
            "DNK",
            "ESP",
            "EST",
            "FIN",
            "FRA",
            "GBR",
            "HRV",
            "HUN",
            "IRL",
            "ITA",
            "JPN",
            "LTU",
            "LVA",
            "NOR",
            "NZL",
            "POL",
            "PRT",
            "SGP",
            "SVK",
            "SWE",
            "USA",
        )
    ],
    ("ISR", "2", "1", "prgisrp2_csv.zip", ";", True, False),
    ("KOR", "2", "1", "prgkorp2_csv.zip", ";", True, False),
]


def puf_url(cycle: str, remote_name: str) -> str:
    return f"{PUF_BASE}/cy{cycle}-puf-data/CSV/{remote_name}"


def local_puf_path(
    iso3: str, cycle: str, round_: str, remote_name: str
) -> Path:
    """One flat name per file, so a resumed download can tell what it already has."""
    suffix = ".zip" if remote_name.endswith(".zip") else ".csv"
    return INPUT_ROOT / f"cycle_{cycle}" / f"{iso3}_r{round_}{suffix}"


# Documents bundled into the per-table auxiliary ZIPs. Long-form reports (Technical
# Reports, Reader's Companions, Assessment Frameworks) are linked from the bundle
# README rather than rehosted -- the 2019 Technical Report alone is 14 MB.
DAM = "https://www.oecd.org/content/dam/oecd/en/about/programmes/edu/piaac"
DOCS: dict[str, list[tuple[str, str]]] = {
    "1": [
        (
            "international_codebook.xlsx",
            f"{DAM}/data-materials/International-Codebook-PIAAC-Public-use-File-Variables-and-Values_Feb2023.xlsx",
        ),
        (
            "derived_variables_codebook.docx",
            f"{DAM}/data-materials/Codebook-for-derived-Variables-16March2015.docx",
        ),
        (
            "missing_variables_by_country.xls",
            f"{DAM}/data-materials/List_of_missing_variables_in_PUF_by_country.xls/_jcr_content/renditions/original./List_of_missing_variables_in_PUF_by_country.xls",
        ),
        (
            "master_background_questionnaire.zip",
            f"{DAM}/background-questionnaire/cycle-1/Master-BQ.zip/_jcr_content/renditions/original./Master-BQ.zip",
        ),
        (
            "background_questionnaire_framework.pdf",
            f"{DAM}/background-questionnaire/cycle-1/PIAAC2011_MS_BQ_ConceptualFramework.pdf/_jcr_content/renditions/original./PIAAC2011_MS_BQ_ConceptualFramework.pdf",
        ),
        (
            "import_csv_in_stata.zip",
            f"{DAM}/data-materials/import%20PUFs%20in%20stata.zip/_jcr_content/renditions/original./import%20PUFs%20in%20stata.zip",
        ),
        (
            "compendium_background_round_1.xlsx",
            f"{DAM}/data-materials/Compendium-Background-variables-Cycle1-Round1-2013-annotated.xlsx",
        ),
        (
            "compendium_background_round_2.xlsx",
            f"{DAM}/data-materials/Compendium-Background-variables-Cycle1-Round2-2016.xlsx",
        ),
        (
            "compendium_background_round_3.xlsx",
            f"{DAM}/data-materials/Compendium-Background_variables-Cycle1-Round3-2019.xlsx",
        ),
        (
            "compendium_cognitive_round_1.xlsx",
            f"{DAM}/data-materials/Compendium-Cognitive-variables-Cycle1-Round1-2013.xlsx",
        ),
        (
            "compendium_cognitive_round_2.xlsx",
            f"{DAM}/data-materials/Compendium-Cognitive-variables-Cycle1-Round2-2016.xlsx",
        ),
        (
            "compendium_cognitive_round_3.xlsx",
            f"{DAM}/data-materials/Compendium-Cognitive-variables-Cycle1-Round3-2019.xlsx",
        ),
    ],
    "2": [
        (
            "international_codebook.xlsx",
            f"{DAM}/data-materials/cycle-2/piaac-cy2-international-codebook.xlsx",
        ),
        (
            "derived_variables_codebook_and_sql.pdf",
            f"{DAM}/data-materials/cycle-2/piaac-cy2-codebook-and-sql-codes-for-derived-variables.pdf",
        ),
        (
            "missing_variables_by_country.xlsx",
            f"{DAM}/data-materials/cycle-2/piaac-cy2-missing_variables-puf.xlsx",
        ),
        (
            "master_background_questionnaire.zip",
            f"{DAM}/background-questionnaire/cycle-2/master-bq-cy2.zip",
        ),
        (
            "background_questionnaire_flowcharts.zip",
            f"{DAM}/background-questionnaire/cycle-2/piaac-cy2-round-1-flowcharts-of-background-questionnaire.zip",
        ),
        (
            "background_questionnaire_framework.pdf",
            f"{DAM}/background-questionnaire/cycle-2/PIAAC_CY2(2018_11)BQ_Draft_Conceptual_Framework.pdf",
        ),
        (
            "import_csv_in_stata.zip",
            f"{DAM}/data-materials/cycle-2/import-csv-in-stata.zip",
        ),
        (
            "import_csv_in_r.zip",
            f"{DAM}/data-materials/cycle-2/import-csv-in-r.zip",
        ),
        (
            "compendium_background.xlsx",
            f"{DAM}/data-materials/cycle-2/piaac-cy2-compendium-background-variables.xlsx",
        ),
        (
            "compendium_cognitive.xlsx",
            f"{DAM}/data-materials/cycle-2/piaac-cy2-compendium-cognitive-variables.xlsx",
        ),
    ],
}

# Linked, not rehosted. Title -> URL, written into each auxiliary bundle's README.
REFERENCE_DOCUMENTS: list[tuple[str, str]] = [
    (
        "Technical Report of the Survey of Adult Skills (PIAAC), 2023",
        "https://www.oecd.org/en/publications/technical-report-of-the-survey-of-adult-skills-piaac-2023_35b81c8c-en.html",
    ),
    (
        "Technical Report of the Survey of Adult Skills (PIAAC), 2019",
        f"{DAM}/data-materials/PIAAC_Technical_Report_2019.pdf",
    ),
    (
        "Survey of Adult Skills 2023 Data Analysis Manual",
        "https://www.oecd.org/en/publications/the-survey-of-adult-skills-2023-data-analysis-manual_c1c1ecad-en.html",
    ),
    (
        "Technical Standards and Guidelines for PIAAC Cycle 1",
        "https://www.oecd.org/en/about/programmes/piaac/piaac-data.html",
    ),
    (
        "Technical Standards and Guidelines for PIAAC Cycle 2",
        "https://www.oecd.org/en/about/programmes/piaac/piaac-data.html",
    ),
    (
        "PIAAC 1st Cycle Database and materials",
        "https://www.oecd.org/en/data/datasets/piaac-1st-cycle-database.html",
    ),
    (
        "PIAAC 2nd Cycle Database and materials",
        "https://www.oecd.org/en/data/datasets/piaac-2nd-cycle-database.html",
    ),
]

CITATION = (
    "OECD (2024), Survey of Adult Skills (PIAAC) Public Use Files, "
    "https://www.oecd.org/en/about/programmes/piaac.html"
)
