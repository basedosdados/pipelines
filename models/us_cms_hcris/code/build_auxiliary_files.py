"""Build the per-table auxiliary-file bundles for us_cms_hcris.

    python build_auxiliary_files.py            # build only
    python build_auxiliary_files.py --upload   # build and upload to GCS

HCRIS is unusable without its documentation: the data is a long triple of
(worksheet, line, column, value), and nothing in it says what any cell means.
The worksheet code list, the cost centre code list, the 2552-96 to 2552-10
crosswalk and the data dictionary are what turn it into something readable, so
they are bundled per table rather than merely linked.

Per ``.claude/rules/auxiliary-files.md``:

* Bundles are per table and hold only that table's documents.
* The two CMS instruction manuals (Pub. 15-2 chapters 36 and 40, 4.6 MB and
  3.0 MB of PDF) are **link-only**: large, stable at the publisher, and read
  once. They are indexed in each README instead of rehosted.
* The bundle goes to the prod bucket under
  ``auxiliary_files/<gcp_dataset_id>/<table_slug>/auxiliary_files.zip``.

Both Data Basis buckets are requester-pays, so the published URL returns HTTP
400 (``UserProjectMissing``) to an anonymous request — as it does for all 84
production tables that use the field today. The bundle still goes to the
documented location; ``--upload`` fetches each URL anonymously afterwards and
prints the status it actually gets rather than assuming it works.
"""

import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

from common import DATA_DIR, DATASET_ID

DOCS = DATA_DIR / "docs"
BUNDLES = DATA_DIR / "auxiliary_files"
PROD_BUCKET = "basedosdados"
TODAY = date.today().isoformat()

# Documents CMS publishes, and where each came from.
SOURCES = {
    "hcris_data_dictionary.csv": (
        "d2010/HCRIS_DataDictionary.csv",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "Field-by-field meaning of every column of the RPT, NMRC and ALPHA "
        "files, with the documented value lists for report status, utilization "
        "and automated desk review vendor.",
    ),
    "hcris_production_notes.txt": (
        "d2010/HOSP2010_README.txt",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "CMS's production notes for the 2552-10 extracts: how to address a "
        "cell, how cost centre coding works, and why the published rollups "
        "should not be trusted.",
    ),
    "worksheet_codes.pdf": (
        "d2010/HOSP2010_Worksheet Codes.pdf",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "Every worksheet code of CMS Form 2552-10 and the worksheet it names.",
    ),
    "cost_center_codes.pdf": (
        "d2010/HOSP2010_CSTCODES.pdf",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "Cost centre codes and the lines they are reported on.",
    ),
    "form_crosswalk_1996_to_2010.xlsx": (
        "d2010/HOSP2010_CROSSWALK.xlsx",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "CMS's own crosswalk from each CMS Form 2552-96 cell to its location "
        "on 2552-10. The reference for extending the mapping behind "
        "hospital_financial across the form change.",
    ),
    "state_codes.csv": (
        "d2010/HCRIS_STATE_CODES.csv",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "SSA state code to state name. The first two characters of the CCN, "
        "and how state_id is resolved.",
    ),
    "facility_numbering.csv": (
        "d2010/HCRIS_FACILITY_NUMBERING.csv",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "Facility type by the last four characters of the CCN — short-term "
        "general, critical access, psychiatric, rehabilitation and the rest. "
        "Not published as a column of this dataset; derive it from "
        "provider_ccn with this table.",
    ),
    "data_model.pdf": (
        "d2010/HCRIS_Data_model.pdf",
        "https://www.cms.gov/files/zip/hospital2010-documentation.zip",
        "Diagram of the RPT, NMRC and ALPHA tables and their columns, in file "
        "order.",
    ),
}

LINK_ONLY = [
    (
        "CMS Publication 15-2, chapter 40 — Form CMS-2552-10 instructions "
        "(4.6 MB) and the blank forms (3.0 MB)",
        "https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/Downloads/P152_40.zip",
    ),
    (
        "CMS Publication 15-2, chapter 36 — Form CMS-2552-96 instructions",
        "https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/Downloads/P152_36.zip",
    ),
    (
        "Cost reports by fiscal year — the landing page every extract is "
        "published from",
        "https://www.cms.gov/data-research/statistics-trends-and-reports/"
        "cost-reports/cost-reports-fiscal-year",
    ),
]

# Which documents each table's users need. report_value needs the cell
# vocabulary; report needs the record layout and the code lists;
# hospital_financial needs the worksheets its measures are read from.
PER_TABLE = {
    "report": [
        "hcris_data_dictionary.csv",
        "hcris_production_notes.txt",
        "state_codes.csv",
        "facility_numbering.csv",
        "data_model.pdf",
    ],
    "report_value": [
        "hcris_data_dictionary.csv",
        "hcris_production_notes.txt",
        "worksheet_codes.pdf",
        "cost_center_codes.pdf",
        "form_crosswalk_1996_to_2010.xlsx",
        "data_model.pdf",
    ],
    "hospital_financial": [
        "hcris_data_dictionary.csv",
        "hcris_production_notes.txt",
        "worksheet_codes.pdf",
        "form_crosswalk_1996_to_2010.xlsx",
        "state_codes.csv",
    ],
}

CITATION = (
    "Centers for Medicare & Medicaid Services. Healthcare Cost Report "
    "Information System (HCRIS), Hospital Form CMS-2552-96 and CMS-2552-10 "
    "cost report public use files. Baltimore, MD: CMS."
)


def readme(table: str, files: list[str]) -> str:
    """Render the bundle README for one table.

    Args:
        table: Table slug.
        files: Bundled file names.

    Returns:
        Markdown text.
    """
    lines = [
        f"# Auxiliary files — `{DATASET_ID}.{table}`",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "US federal government work, in the public domain.",
        "",
        "## Contents",
        "",
    ]
    for name in files:
        _, url, what = SOURCES[name]
        lines += [
            f"### `{name}`",
            "",
            what,
            "",
            f"Downloaded {TODAY} from {url}",
            "",
        ]
    lines += ["## Referenced, not bundled", ""]
    lines += [f"- {what} — {url}" for what, url in LINK_ONLY]
    lines += [
        "",
        "These are large, stable at the publisher, and read once rather than "
        "consulted per query, so they are linked rather than rehosted.",
        "",
        "## What Data Basis changed",
        "",
        "- Dates were rewritten from the source's `MM/DD/YYYY` to ISO "
        "`YYYY-MM-DD`; every other value is as filed.",
        "- Empty strings were read as NULL.",
        "- `year` is derived from the fiscal year **end** date, not from the "
        "federal fiscal year of the CMS extract the report came in.",
        "- `state_id` and `state_abbreviation` are resolved from the SSA state "
        "code in the first two characters of the CCN, using `state_codes.csv` "
        "above.",
        "- The `ROLLUP` file shipped with the CMS Form 2552-96 extracts is not "
        "ingested. CMS's own production notes say of it: \"The methods of "
        "combining reported data as published in the 2552-96 forms have proven "
        "to be unreliable. Users are encouraged to develop their own methods "
        'of summarizing HCRIS data."',
        "",
    ]
    return "\n".join(lines)


def build(table: str) -> Path:
    """Build one table's bundle.

    Args:
        table: Table slug.

    Returns:
        Path of the written ZIP.

    Raises:
        FileNotFoundError: If a source document has not been fetched.
    """
    files = PER_TABLE[table]
    out = BUNDLES / table / "auxiliary_files.zip"
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.md", readme(table, files))
        for name in files:
            src = DOCS / SOURCES[name][0]
            if not src.exists():
                raise FileNotFoundError(
                    f"{src} — run the documentation download first"
                )
            zf.write(src, name)
    return out


def gcs_url(table: str) -> str:
    """Public URL of a table's bundle."""
    return (
        f"https://storage.googleapis.com/{PROD_BUCKET}/auxiliary_files/"
        f"{DATASET_ID}/{table}/auxiliary_files.zip"
    )


def main() -> None:
    """Build every bundle, and upload when asked."""
    for table in PER_TABLE:
        path = build(table)
        print(f"{table:<20} {path.stat().st_size:>9,} bytes  {path}")
    if "--upload" not in sys.argv:
        print("\nnot uploaded (pass --upload)")
        return
    for table in PER_TABLE:
        dest = (
            f"gs://{PROD_BUCKET}/auxiliary_files/{DATASET_ID}/{table}/"
            "auxiliary_files.zip"
        )
        subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                str(BUNDLES / table / "auxiliary_files.zip"),
                dest,
            ],
            check=True,
        )
        status = subprocess.run(
            [
                "curl",
                "-sI",
                "-o",
                "/dev/null",
                "-w",
                "%{http_code}",
                gcs_url(table),
            ],
            capture_output=True,
            text=True,
        ).stdout.strip()
        print(f"{table:<20} uploaded; anonymous GET returns HTTP {status}")


if __name__ == "__main__":
    main()
