"""Build the per-table auxiliary-file bundles for us_epa_tri.

One ZIP per data table under ``$TRI_DATA_DIR/auxiliary_files/<table>/``,
containing EPA's field documentation for the Basic Data Files (the source of
every column here), the dataset's own dictionary of coded values, and a README
recording provenance, download dates and the transformations applied on load.

The Form R reporting instructions (GuideME) and the Envirofacts TRI data model
are link-only: stable at the publisher and describing the reporting form, not
these files.

Usage:
    uv run python models/us_epa_tri/code/build_auxiliary_files.py [--upload]
"""

import os
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = Path(
    os.environ.get("TRI_DATA_DIR", Path.home() / "Downloads/us_epa_tri_data")
)
AUX = DATA / "auxiliary_files"
DOC_PDF = DATA / "docs" / "basic_data_files_documentation_august_2024.pdf"
DOC_URL = (
    "https://www.epa.gov/system/files/documents/2025-09/"
    "basic_data_files_documentation_august_2024.pdf"
)
PAGE_URL = (
    "https://www.epa.gov/toxics-release-inventory-tri-program/"
    "tri-basic-data-files-calendar-years-1987-present"
)
DOWNLOAD_DATE = "2026-09-03"
DATASET = "us_epa_tri"
BUCKET = os.environ.get("TRI_AUX_BUCKET", "basedosdados-dev")

LINK_ONLY = [
    (
        "TRI Reporting Forms and Instructions (GuideME)",
        "https://guideme.epa.gov/",
    ),
    ("Envirofacts TRI data model", "https://www.epa.gov/enviro/tri-model"),
    (
        "TRI Basic Plus Data Files guides (the 10-file full extract)",
        "https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides",
    ),
    (
        "Envirofacts TRI_FACILITY table (county FIPS per facility)",
        "https://data.epa.gov/efservice/tri_facility/rows/0:9/JSON",
    ),
]

TABLE_NOTES = {
    "facility": (
        "One row per facility (TRIFID) and reporting year, taken from the first "
        "form of the year by document control number; every kept attribute is "
        "constant across a facility's forms within a year. `county_id` is the "
        "county FIPS code from the Envirofacts TRI_FACILITY table, joined by "
        "TRIFID; the Basic file itself carries only the county name."
    ),
    "chemical": (
        "One row per TRI chemical identifier, with the attributes published in the "
        "most recent reporting year in which the chemical appears (most frequent "
        "variant within that year). The PFAS flag exists since RY 2020."
    ),
    "form": (
        "One row per form (DOC_CTRL_NUM). SIC/NAICS codes are kept here because "
        "EPA assigned NAICS per submission through 2005 and they vary across a "
        "facility's forms. `naics_version` is derived from the year. Every "
        "quantity is in pounds: quantities the source reports in grams (dioxin "
        "and dioxin-like compounds, UNIT OF MEASURE = Grams) were divided by "
        "453.59237. EPA's totals are copied as published."
    ),
    "release": (
        "Long format: one row per form and release/transfer category, unpivoted "
        "from the 55 leaf columns of Form R sections 5, 6.1 and 6.2 (the "
        "categories are listed in dicionario.csv). Rows whose quantity is zero "
        "are not stored — the source fills zeros where the facility reported NA, "
        "left the field blank, or filed a Form A. `quantity_pounds` is pounds for "
        "every row (grams / 453.59237 for dioxins); `quantity_grams` keeps the "
        "dioxin rows as reported and is null otherwise."
    ),
}


def readme(table: str) -> str:
    lines = [
        f"# {DATASET}.{table} — auxiliary files",
        "",
        "Source: U.S. Environmental Protection Agency, Toxics Release Inventory (TRI)",
        f"Program, TRI Basic Data Files, {PAGE_URL}",
        "Suggested citation: U.S. EPA, Toxics Release Inventory Basic Data Files,",
        "reporting years 1987-2024, files processed as of November 5, 2025, accessed",
        f"{DOWNLOAD_DATE}. U.S. Government work, public domain.",
        "",
        "## Files in this bundle",
        "",
        '- `tri_basic_data_files_documentation_2024.pdf` — EPA, "Toxics Release',
        '  Inventory Basic Data Files Documentation", August 2024: the field-by-field',
        f"  description of the 122 columns of the source file. From {DOC_URL},",
        f"  downloaded {DOWNLOAD_DATE}.",
        "- `dicionario.csv` — the dataset's dictionary of coded values (release and",
        "  management categories, form type, chemical classification) with labels in",
        "  Portuguese, English and Spanish.",
        "",
        "## Reading this table",
        "",
        TABLE_NOTES[table],
        "",
        "## Link-only documentation",
        "",
    ]
    for title, url in LINK_ONLY:
        lines.append(f"- {title}: {url}")
    return "\n".join(lines) + "\n"


def build(table: str) -> Path:
    out_dir = AUX / table
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "auxiliary_files.zip"
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.md", readme(table))
        z.write(DOC_PDF, "tri_basic_data_files_documentation_2024.pdf")
        z.write(ROOT / "dicionario.csv", "dicionario.csv")
    return dest


def upload(table: str, path: Path) -> str:
    from google.cloud import storage

    client = storage.Client(project=BUCKET)
    blob = client.bucket(BUCKET, user_project=BUCKET).blob(
        f"auxiliary_files/{DATASET}/{table}/auxiliary_files.zip"
    )
    blob.upload_from_filename(str(path))
    return f"https://storage.googleapis.com/{BUCKET}/{blob.name}"


def main():
    for table in TABLE_NOTES:
        dest = build(table)
        print(f"{table}: {dest} ({dest.stat().st_size:,} bytes)")
        if "--upload" in sys.argv:
            print("  ->", upload(table, dest))


if __name__ == "__main__":
    main()
