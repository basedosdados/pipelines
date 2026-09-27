"""Build and upload the per-table auxiliary file bundles for us_nsf_ncses.

NCSES publishes documentation that this dataset is hard to use without: the HERD
public use data guide defines every code and states that expenditures are
reported in thousands of dollars, and the questionnaires name the items the
``question_code`` column refers to.

Two bundles, one per survey, attached to the tables each survey feeds:

    auxiliary_files/us_nsf_ncses/<table_slug>/auxiliary_files.zip

Two caveats worth stating rather than discovering:

* Both Data Basis buckets are requester-pays, so the published URL returns
  HTTP 400 to an anonymous fetch. Every production table using the field has
  the same problem; the fix is one bucket setting, not a bespoke host here.
* The dev service account cannot write to ``gs://basedosdados``, so the bundles
  go to ``gs://basedosdados-dev`` — where most existing rows already point.
  Re-run with prod credentials to move them.

Run
---
    python models/us_nsf_ncses/code/auxiliary_files.py            # build only
    python models/us_nsf_ncses/code/auxiliary_files.py --upload   # build + upload
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import zipfile

DATA_DIR = pathlib.Path(
    os.environ.get(
        "NCSES_DATA_DIR",
        pathlib.Path.home() / "Downloads" / "us_nsf_ncses_data",
    )
)
INPUT_DIR = DATA_DIR / "input"
BUNDLE_DIR = DATA_DIR / "auxiliary_files"

BUCKET = "basedosdados-dev"
BILLING_PROJECT = "basedosdados-dev"
GCP_DATASET = "us_nsf_ncses"

DOWNLOADED = "12 September 2026"

HERD_TABLES = [
    "herd_institution",
    "herd_expenditure",
    "herd_personnel",
    "herd_survey_item",
]
SED_TABLES = ["sed_estimate", "sed_data_table"]

HERD_FILES = {
    "herd_public_use_data_guide_fy2024.pdf": (
        "herd_guide.pdf",
        "Guide for Public Use Data Files, FY2024. Defines every code in the "
        "file, states that all expenditure items are reported in thousands of "
        "dollars, and documents both survey eras",
        "https://ncses.nsf.gov/821/assets/0/files/"
        "fy-2024-herd-dug-text-file-format.pdf",
    ),
    "herd_questionnaire_fy2024_standard.pdf": (
        "herd_questionnaire_2024.pdf",
        "FY2024 standard form questionnaire. Names the items behind each "
        "question_code",
        "https://ncses.nsf.gov/1639/assets/0/files/srvyherd-2024.pdf",
    ),
    "herd_questionnaire_fy2024_short.pdf": (
        "herd_questionnaire_2024_short.pdf",
        "FY2024 short form questionnaire, used by institutions reporting under "
        "$1 million in total R&D",
        "https://ncses.nsf.gov/1639/assets/0/files/srvyherd-2024-short.pdf",
    ),
}

SED_FILES = {
    "sed_survey_description_2024.pdf": (
        "sed_survey_description.pdf",
        "Survey description and technical notes for the 2024 cycle: target "
        "population, response rates, imputation and comparability",
        "https://ncses.nsf.gov/1499/assets/0/file/ncses_sed.pdf",
    ),
    "sed_questionnaire_2024.pdf": (
        "sed_questionnaire_2024.pdf",
        "2024 survey questionnaire",
        "https://ncses.nsf.gov/1499/assets/0/files/earned-doctorates-2024.pdf",
    ),
}

HERD_README = """# HERD auxiliary files — us_nsf_ncses

Documentation published by the National Center for Science and Engineering
Statistics (NCSES) alongside the Higher Education Research and Development
(HERD) Survey public use data files, which are the source of the `herd_*`
tables in this dataset.

## Suggested citation

National Center for Science and Engineering Statistics (NCSES). 2025. *Higher
Education Research and Development Survey, Fiscal Year 2024.* NSF 26-304.
Alexandria, VA: U.S. National Science Foundation.
https://ncses.nsf.gov/surveys/higher-education-research-development

## Files in this bundle

{files}

## Read first

* **All expenditure items are reported in thousands of dollars** in the source.
  The `expenditure` and `amount` columns here are multiplied by a thousand and
  carry dollars, so the source precision is the thousand. Values are current
  dollars and are not deflated.
* The survey has two eras and they are **not** interchangeable. FY1972-FY2009 is
  the Survey of R&D Expenditures at Universities and Colleges; FY2010 onwards is
  HERD. Several codes change meaning at that boundary — see the `dicionario`
  table, where each entry carries its own temporal coverage.
* The IPEDS UNITID appears in the source only from FY2010. For earlier years it
  is carried back from the same NCSES institution id observed in FY2010 or
  later; an institution that left the survey before FY2010 has none.

## Linked elsewhere

* HERD survey landing page, all cycles:
  https://ncses.nsf.gov/surveys/higher-education-research-development
* Public use data files, all years:
  https://ncses.nsf.gov/explore-data/microdata/higher-education-research-development
* FY2024 published data tables (NSF 26-304), 8 MB:
  https://ncses.nsf.gov/pubs/nsf26304/assets/data-tables/nsf26304-data-tables-tables.zip
"""

SED_README = """# SED auxiliary files — us_nsf_ncses

Documentation published by the National Center for Science and Engineering
Statistics (NCSES) alongside the Survey of Earned Doctorates (SED) data tables,
which are the source of the `sed_*` tables in this dataset.

## Suggested citation

National Center for Science and Engineering Statistics (NCSES). 2025. *Doctorate
Recipients from U.S. Universities: 2024.* NSF 25-349. Alexandria, VA: U.S.
National Science Foundation. https://ncses.nsf.gov/surveys/earned-doctorates

## Files in this bundle

{files}

## Read first

* Only the **published aggregate tables** are in this dataset. SED individual
  records are restricted-use microdata, obtainable from NCSES under a licence,
  and are deliberately not onboarded.
* Each survey cycle republishes its own history, so a cycle is a closed vintage.
  Filter to the largest `reference_year` rather than summing across cycles.
* `year` is the academic year the value describes: 2024 runs from 1 July 2023 to
  30 June 2024.

## Linked elsewhere

* SED survey landing page, all cycles:
  https://ncses.nsf.gov/surveys/earned-doctorates
* 2024 published data tables as PDF (NSF 25-349), 5.7 MB:
  https://ncses.nsf.gov/pubs/nsf25349/assets/data-tables/nsf25349-data-tables-tables-pdfs.zip
* Restricted use data licensing:
  https://ncses.nsf.gov/explore-data/microdata
"""


def build(name: str, files: dict, readme: str) -> pathlib.Path:
    """Write one bundle, renaming each file to something self-describing."""
    listed = "\n".join(
        f"* `{published}` — {note}. Downloaded {DOWNLOADED} from {url}"
        for published, (_source, note, url) in files.items()
    )
    BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
    out = BUNDLE_DIR / f"{name}_auxiliary_files.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.md", readme.format(files=listed))
        for published, (source, _note, _url) in files.items():
            path = INPUT_DIR / source
            if not path.exists():
                raise FileNotFoundError(
                    f"{path} is missing; download it first"
                )
            zf.write(path, published)
    print(
        f"{out.name}: {out.stat().st_size / 1024:.0f} KB, {len(files) + 1} files"
    )
    return out


def upload(bundle: pathlib.Path, tables: list[str]) -> list[str]:
    """Copy one bundle to every table's auxiliary file prefix."""
    urls = []
    for table in tables:
        target = (
            f"gs://{BUCKET}/auxiliary_files/{GCP_DATASET}/{table}/"
            "auxiliary_files.zip"
        )
        subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                f"--billing-project={BILLING_PROJECT}",
                str(bundle),
                target,
            ],
            check=True,
            env={
                k: v
                for k, v in os.environ.items()
                if k != "GOOGLE_APPLICATION_CREDENTIALS"
            },
        )
        urls.append(
            target.replace(
                f"gs://{BUCKET}/", f"https://storage.googleapis.com/{BUCKET}/"
            )
        )
        print(f"  {table} -> {urls[-1]}")
    return urls


def main() -> int:
    """Build both per-survey bundles, and upload them when asked.

    Returns:
        0 on success, non-zero if any upload failed.
    """
    herd = build("herd", HERD_FILES, HERD_README)
    sed = build("sed", SED_FILES, SED_README)
    if "--upload" not in sys.argv:
        print("\nbuilt only; pass --upload to copy to the bucket")
        return 0
    urls = upload(herd, HERD_TABLES) + upload(sed, SED_TABLES)
    print("\nanonymous HTTP status of each published URL:")
    for url in urls:
        status = subprocess.run(
            ["curl", "-sI", "-o", "/dev/null", "-w", "%{http_code}", url],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        print(f"  {status}  {url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
