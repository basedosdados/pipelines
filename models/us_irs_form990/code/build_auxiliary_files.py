"""Build the per-table auxiliary-file bundles and upload them to GCS.

Per ``auxiliary-files``: one ZIP per table, holding only what a user of that
table needs, with a README carrying provenance. Long-form IRS instructions are
link-only.

    organization      eo-info.pdf (EO BMF information sheet), ntee_codes.csv
    return_financial  concordance.csv (the XPath map actually used), README
    compensation      concordance.csv, README
    revocation        README (field layout + IRS data dictionary link)

Usage (from the repo root, ``PYTHONPATH=.``)::

    python models/us_irs_form990/code/build_auxiliary_files.py [--upload]

Uploads go to ``gs://basedosdados-dev/auxiliary_files/us_irs_form990/<table>/``
(the prod bucket is not writable with local credentials). The public URL is
recorded on each table's ``auxiliary_files_url``; note the requester-pays
caveat in ``auxiliary-files``.
"""

import argparse
import datetime as dt
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import requests

from pipelines.datasets.us_irs_form990.constants import constants

HERE = Path(__file__).resolve().parent
OUT = (
    Path(
        os.environ.get(
            "FORM990_DATA_DIR", Path.home() / "Downloads/us_irs_form990_data"
        )
    )
    / "auxiliary_files"
)
BUCKET = "basedosdados-dev"
DATASET = "us_irs_form990"
TODAY = dt.date.today().isoformat()

CONCORDANCE = constants.CONCORDANCE_PATH.value
NTEE = HERE / "ntee_codes.csv"
EO_INFO_URL = constants.BMF_INFO_URL.value

LINKS = {
    "Form 990 series downloads (e-file XML ZIPs and index files)": constants.EFILE_LISTING_URL.value,
    "EO Business Master File extract": "https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf",
    "Tax Exempt Organization Search bulk downloads (revocation list)": "https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-bulk-data-downloads",
    "Instructions for Form 990 (PDF)": "https://www.irs.gov/pub/irs-pdf/i990.pdf",
    "Instructions for Form 990-EZ (PDF)": "https://www.irs.gov/pub/irs-pdf/i990ez.pdf",
    "IRS Modernized e-File schemas for exempt organizations": "https://www.irs.gov/e-file-providers/current-valid-xml-schemas-and-business-rules-for-exempt-organizations-modernized-e-file",
    "Nonprofit Open Data Collective master concordance (ef2, MIT)": "https://github.com/Nonprofit-Open-Data-Collective/ef2",
    "NCCS NTEE code documentation": "https://nccs.urban.org/nccs/resources/ntee/",
}

CITATION = (
    "Internal Revenue Service, Tax Exempt and Government Entities Division. "
    "Form 990 series e-file returns; Exempt Organizations Business Master File "
    "extract; Automatic Revocation of Exemption list. U.S. Government work, "
    "public domain (17 U.S.C. 105)."
)


def readme(table: str, files: list[tuple[str, str, str]], notes: str) -> str:
    lines = [
        f"# us_irs_form990 / {table} — auxiliary files",
        "",
        f"Bundle built {TODAY}.",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "## Files in this bundle",
        "",
    ]
    for name, what, url in files:
        lines.append(
            f"- `{name}` — {what}. Source: {url} (downloaded {TODAY})."
        )
    lines += [
        "",
        "## Notes for users",
        "",
        notes,
        "",
        "## Link-only documents",
        "",
    ]
    for title, url in LINKS.items():
        lines.append(f"- {title}: {url}")
    return "\n".join(lines) + "\n"


EFILE_NOTES = """\
Values are read from the IRS XML through the Nonprofit Open Data Collective
master concordance (`concordance.csv`, trimmed to the variables this dataset
uses). Column `original_name` in the architecture sheet gives the concordance
`variable_name` behind each column; the CSV lists every XPath that variable has
taken across schema versions 2009v1.0 .. 2024v5.x.

Coverage caveats: only electronically filed Form 990 and 990-EZ returns are
present (paper returns never enter the IRS XML feed; e-filing became mandatory
for all filers for tax years beginning after 1 July 2019). Form 990-PF and
990-T are not covered by the concordance and are not loaded. One return is kept
per (ein, year, form_type): the most recently filed, so amended returns replace
their originals. Checkboxes absent from the XML are stored as false; on 990-EZ
the Part VII position checkboxes do not exist and are null. Money is in USD as
filed (nominal).
"""

BMF_NOTES = """\
`eo-info.pdf` is the IRS information sheet that defines every code column
(subsection, classification, affiliation, foundation, status, filing
requirement, asset/income classes). `ntee_codes.csv` carries the NTEE-CC labels
(NCCS taxonomy, via the Nonprofit Open Data Collective mission-taxonomies
repository); the BMF sometimes appends a fourth character to the code.
`ruling_date` is stored as the first day of the ruling month. The registry
excludes churches and other organizations not required to apply for
recognition, and organizations whose exemption was revoked (see the
`revocation` table). Each monthly extract is a full snapshot stacked on
`extraction_date`.
"""

REVOCATION_NOTES = """\
The source is a header-less pipe-delimited file with twelve fields: EIN, legal
name, doing-business-as name, address, city, state, ZIP, country, exemption
type (IRC subsection code, same list as the BMF `subsection_code`), revocation
date, revocation posting date, exemption reinstatement date. Dates are
converted from DD-MON-YYYY to ISO. The list is cumulative and replaced
wholesale at each monthly update.
"""


def build() -> dict[str, Path]:
    OUT.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="form990_aux_"))
    eo_info = work / "eo-info.pdf"
    r = requests.get(EO_INFO_URL, headers=constants.HEADERS.value, timeout=120)
    r.raise_for_status()
    eo_info.write_bytes(r.content)

    bundles = {
        "organization": (
            [
                (
                    "eo-info.pdf",
                    "EO BMF information sheet with every code table",
                    EO_INFO_URL,
                ),
                (
                    "ntee_codes.csv",
                    "NTEE-CC code labels",
                    "https://github.com/Nonprofit-Open-Data-Collective/mission-taxonomies/blob/main/NTEE/ntee.csv",
                ),
            ],
            [(eo_info, "eo-info.pdf"), (NTEE, "ntee_codes.csv")],
            BMF_NOTES,
        ),
        "return_financial": (
            [
                (
                    "concordance.csv",
                    "NODC master concordance, trimmed to the variables used",
                    "https://github.com/Nonprofit-Open-Data-Collective/ef2/blob/main/inst/extdata/concordance.csv",
                )
            ],
            [(CONCORDANCE, "concordance.csv")],
            EFILE_NOTES,
        ),
        "compensation": (
            [
                (
                    "concordance.csv",
                    "NODC master concordance, trimmed to the variables used",
                    "https://github.com/Nonprofit-Open-Data-Collective/ef2/blob/main/inst/extdata/concordance.csv",
                )
            ],
            [(CONCORDANCE, "concordance.csv")],
            EFILE_NOTES,
        ),
        "revocation": ([], [], REVOCATION_NOTES),
    }
    zips: dict[str, Path] = {}
    for table, (files, paths, notes) in bundles.items():
        zpath = OUT / f"{table}.zip"
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.md", readme(table, files, notes))
            for src, name in paths:
                zf.write(src, name)
        zips[table] = zpath
        print(f"{zpath.name}: {zpath.stat().st_size:,} bytes")
    shutil.rmtree(work, ignore_errors=True)
    return zips


def upload(zips: dict[str, Path]) -> None:
    import google.cloud.storage as gcs

    client = gcs.Client(project=BUCKET)
    bucket = client.bucket(BUCKET, user_project=BUCKET)
    for table, path in zips.items():
        blob = bucket.blob(
            f"auxiliary_files/{DATASET}/{table}/auxiliary_files.zip"
        )
        blob.upload_from_filename(str(path))
        url = f"https://storage.googleapis.com/{BUCKET}/auxiliary_files/{DATASET}/{table}/auxiliary_files.zip"
        code = requests.head(url, timeout=60).status_code
        print(f"{table}: {url} -> anonymous HTTP {code}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()
    zips = build()
    if args.upload:
        upload(zips)


if __name__ == "__main__":
    main()
