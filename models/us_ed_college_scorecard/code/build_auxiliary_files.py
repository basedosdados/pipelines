#!/usr/bin/env python3
"""
Build the per-table auxiliary-file bundles for us_ed_college_scorecard.

One ZIP per table, under $AUX_DIR, each carrying a README plus exactly the
documentation a user of that table needs. Uploaded to

    gs://basedosdados/auxiliary_files/us_ed_college_scorecard/<table>/auxiliary_files.zip

and recorded on the table's auxiliary_files_url. Note that the bucket is
requester-pays, so the published URL currently returns HTTP 400 to an
anonymous fetch; that is a bucket setting, not something this script can fix.

Usage:
    /tmp/cs_venv/bin/python models/us_ed_college_scorecard/code/build_auxiliary_files.py
"""

import os
import pathlib
import sys
import zipfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
# pyrefly: ignore [missing-import]
import spec

DATA_DIR = pathlib.Path(
    os.environ.get(
        "SCORECARD_DATA_DIR",
        pathlib.Path.home() / "Downloads/us_ed_college_scorecard_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
AUX_DIR = pathlib.Path(os.environ.get("AUX_DIR", DATA_DIR / "auxiliary_files"))
DOWNLOADED = "2026-09-02"
RELEASE = "June 10, 2026"

DOWNLOAD_BASE = "https://ed-public-download.scorecard.network/downloads"
ASSET_BASE = "https://collegescorecard.ed.gov/assets"

# (bundled name, source path, origin URL, what it is)
DICTIONARY = (
    "data_dictionary.xlsx",
    INPUT_DIR / "CollegeScorecardDataDictionary.xlsx",
    f"{ASSET_BASE}/CollegeScorecardDataDictionary.xlsx",
    "Official data dictionary workbook: variable definitions, value labels and "
    "cohort maps for both the institution-level and field-of-study files.",
)
MACHINE_DICTIONARY = (
    "data_dictionary_machine_readable.yaml",
    INPUT_DIR / "raw" / "data.yaml",
    f"{DOWNLOAD_BASE}/College_Scorecard_Raw_Data_06102026.zip",
    "Machine-readable dictionary shipped inside the data archive. Regenerated "
    "with each release, so it is more current than the workbook, and it is what "
    "drove this dataset's architecture and variable table.",
)
INSTITUTION_DOC = (
    "institution_data_documentation.pdf",
    INPUT_DIR / "InstitutionDataDocumentation.pdf",
    f"{ASSET_BASE}/InstitutionDataDocumentation.pdf",
    "Technical documentation for the institution-level data: cohort "
    "construction, suppression rules and measure definitions.",
)
FIELD_OF_STUDY_DOC = (
    "field_of_study_data_documentation.pdf",
    INPUT_DIR / "FieldOfStudyDataDocumentation.pdf",
    f"{ASSET_BASE}/FieldOfStudyDataDocumentation.pdf",
    "Technical documentation for the field-of-study data: pooling of award "
    "years, credential levels and the debt and earnings measures.",
)

INSTITUTION_TABLES = ["institution", *sorted(set(spec.LONG_TABLES.values()))]

CONTENTS = {
    **{
        t: [DICTIONARY, MACHINE_DICTIONARY, INSTITUTION_DOC]
        for t in INSTITUTION_TABLES
    },
    "field_of_study": [DICTIONARY, MACHINE_DICTIONARY, FIELD_OF_STUDY_DOC],
    "variable": [DICTIONARY, MACHINE_DICTIONARY],
    "dicionario": [DICTIONARY, MACHINE_DICTIONARY],
}

TABLE_NOTE = {
    "institution": (
        "This table holds only the institution's identity, location, "
        "characteristics and admissions block. Every other institution-level "
        "measure is in a long table of this dataset; `variable` says which."
    ),
    "field_of_study": (
        "Borrower-based repayment columns (BBRR*) are STRING, not numeric: the "
        "source publishes them as rounding intervals ('0.30-0.39', '<=0.10') as "
        "often as numbers. 64% of published BBRR values in the 2015-2022 files "
        "are intervals."
    ),
    "variable": "Resolves the variable_name column of this dataset's long tables.",
    "dicionario": "Value labels for the coded columns of institution and field_of_study.",
}
for _t in sorted(set(spec.LONG_TABLES.values())):
    TABLE_NOTE[_t] = (
        "Long table: one row per institution, year and source variable. `value` "
        "holds the number; `value_raw` holds whatever the source published "
        "instead of a number, including 'PrivacySuppressed' for a withheld "
        "cell. Variable definitions are in the `variable` table and in the "
        "bundled dictionaries."
    )


def readme(table, files):
    lines = [
        f"# Auxiliary files — us_ed_college_scorecard.{table}",
        "",
        "## Citation",
        "",
        "U.S. Department of Education, College Scorecard. "
        f"Data release of {RELEASE}. https://collegescorecard.ed.gov/data/",
        "",
        "## License",
        "",
        "Work of the U.S. federal government, published as open data and listed "
        "in the data.gov catalogue under Creative Commons CC0 (public domain "
        "dedication). No restriction on use or redistribution.",
        "",
        "## About this table",
        "",
        TABLE_NOTE[table],
        "",
        "## Bundled files",
        "",
    ]
    for name, _path, url, what in files:
        lines += [
            f"### `{name}`",
            "",
            what,
            "",
            f"- Source: {url}",
            f"- Downloaded: {DOWNLOADED}",
            "",
        ]
    lines += [
        "## Not bundled — available from the source",
        "",
        "- **Crosswalk workbooks** (`Crosswalks/CW2000.xlsx` … `CW2024_prelim.xlsx`, "
        "~50 MB) map institutions across years and program codes. They ship inside "
        f"the data archive: {DOWNLOAD_BASE}/College_Scorecard_Raw_Data_06102026.zip",
        "- **Change log**: https://collegescorecard.ed.gov/data/changelog/",
        "- **Glossary**: https://collegescorecard.ed.gov/data/glossary/",
        "",
        "## How this dataset was built",
        "",
        "Cohort files `MERGED1996_97_PP.csv` … `MERGED2025_26_PP.csv` and "
        "`FieldOfStudyData1415_1516_PP.csv` … `FieldOfStudyData2122_2223_PP.csv` "
        "were loaded in full. The two `Most-Recent-Cohorts-*` files were **not** "
        "loaded: each of their columns carries the latest non-missing value from a "
        "different year, so they are not a cohort year and would corrupt the panel. "
        "Every value in them is already present in the cohort file it came from.",
        "",
        "'PrivacySuppressed' marks a cell the source withholds because the "
        "underlying cohort is too small. In the long tables it is kept as a row "
        "with `value` null and `value_raw` 'PrivacySuppressed'; in the two wide "
        "tables it becomes null.",
        "",
    ]
    return "\n".join(lines)


def main():
    AUX_DIR.mkdir(parents=True, exist_ok=True)
    for table in spec.TABLE_SLUGS:
        files = CONTENTS[table]
        out = AUX_DIR / table / "auxiliary_files.zip"
        out.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.md", readme(table, files))
            for name, path, _url, _what in files:
                if not path.exists():
                    raise FileNotFoundError(path)
                zf.write(path, name)
        print(
            f"{table:16s} {out.stat().st_size / 1e6:6.2f} MB  {len(files) + 1} files"
        )


if __name__ == "__main__":
    main()
