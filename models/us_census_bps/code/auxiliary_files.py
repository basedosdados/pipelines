"""Build and upload the per-table auxiliary-file bundles for us_census_bps.

Each bundle holds the Census record-layout document for that geography level
plus a README naming the citation, provenance and download date.

The bundles go to ``basedosdados-dev`` because that is the only bucket the
onboarding service account can write. Both data buckets are requester-pays, so
the published URLs return HTTP 400 to an anonymous reader; the fix is the
pending move to ``gs://basedosdados-public``, not a per-dataset workaround.
This script fetches every URL it registers with no credentials and reports the
status it actually gets.
"""

from __future__ import annotations

import datetime
import os
import sys
import tomllib
import urllib.request
import zipfile
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_bps.constants import constants

DATASET_ID = constants.DATASET_ID.value
BUCKET = "basedosdados-dev"
DOC_BASE = constants.BASE_URL.value + "Documentation/"
DATA_DIR = Path(
    os.environ.get(
        "BPS_DATA_DIR", Path.home() / "Downloads/us_census_bps_data"
    )
)
WORK = DATA_DIR / "auxiliary"

CITATION = (
    "U.S. Census Bureau, Building Permits Survey. "
    "https://www.census.gov/construction/bps/"
)

# table group -> (layout document, human name, what the document covers)
DOCS = {
    ("permit_place_monthly", "permit_place_annual"): (
        "placeasc.pdf",
        "place_record_layout.pdf",
        "Record layout of the place-level ASCII files, including the field "
        "order for each of the layouts used since 1988, the source codes and "
        "the footnotes appended to permit office names.",
    ),
    ("permit_county_monthly", "permit_county_annual"): (
        "cntyasc.pdf",
        "county_record_layout.pdf",
        "Record layout of the county-level ASCII files, including the Census "
        "region and division codes.",
    ),
    ("permit_state_monthly", "permit_state_annual"): (
        "stateasc.pdf",
        "state_record_layout.pdf",
        "Record layout of the state-level ASCII files. Note that the source "
        "publishes valuation in thousands of dollars at this level; the Data "
        "Basis tables carry dollars.",
    ),
    ("permit_cbsa_monthly", "permit_cbsa_annual"): (
        "cbsaasc.pdf",
        "cbsa_record_layout.pdf",
        "Record layout of the Core Based Statistical Area files, including "
        "the header code that distinguishes metropolitan from micropolitan "
        "areas from January 2024. Valuation is published in thousands of "
        "dollars; the Data Basis tables carry dollars.",
    ),
    ("permit_msa_monthly", "permit_msa_annual"): (
        "msaasc.pdf",
        "msa_record_layout.pdf",
        "Record layout of the pre-2004 Metropolitan Statistical Area files, "
        "including the header coverage code. Valuation is published in "
        "thousands of dollars; the Data Basis tables carry dollars.",
    ),
}

LINK_ONLY = [
    (
        "Importing Permits Files to Microsoft Excel",
        DOC_BASE + "Importing%20Permits%20Files%20to%20Microsoft%20Excel.docx",
        "Publisher guide to opening the raw ASCII files in Excel. Not needed "
        "to use the Data Basis tables, which are already parsed.",
    ),
    (
        "Building Permits Survey landing page",
        "https://www.census.gov/construction/bps/",
        "Survey methodology, release schedule and the current release.",
    ),
    (
        "Compiled data file (all geography levels in one CSV)",
        constants.BASE_URL.value + "Master%20Data%20Set/",
        "The Census Bureau's own combined extract. Data Basis builds from the "
        "per-level ASCII files instead, which go back further.",
    ),
]


def readme(
    tables: tuple[str, ...], doc_name: str, covers: str, remote: str
) -> str:
    """Render the bundle README."""
    today = datetime.date.today().isoformat()
    lines = [
        f"# Auxiliary files — us_census_bps.{tables[0].rsplit('_', 1)[0]}",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "The Building Permits Survey is a work of the United States",
        "Government and is in the public domain (17 U.S.C. s.105).",
        "",
        "## Tables these documents describe",
        "",
        *[f"- `basedosdados.{DATASET_ID}.{t}`" for t in tables],
        "",
        "## Bundled files",
        "",
        f"### `{doc_name}`",
        "",
        covers,
        "",
        f"- Source: {DOC_BASE}{remote}",
        f"- Downloaded: {today}",
        "",
        "## What Data Basis changed from the raw files",
        "",
        "- The raw files carry one row per geography and period with four",
        "  blocks of columns, one per structure type. The Data Basis tables",
        "  are long: one row per geography, period and structure type, with",
        "  the structure type in `structure_type` and its label in the",
        "  `dicionario` table.",
        "- Valuation is normalised to US dollars everywhere. The state and",
        "  metropolitan files publish it in thousands, so those figures are",
        "  multiplied by 1,000 and are therefore precise to $1,000.",
        "- `buildings`, `units` and `valuation` are the estimates including",
        "  imputation for non-responding permit offices, which is the series",
        "  to use by default. The `_reported` columns count only offices that",
        "  reported.",
        "- Codes the source uses to mean 'not applicable' are stored as NULL:",
        "  999 for CSA, 99999 for CBSA, 9999 for PMSA and MSA, and 00000,",
        "  000 and 99990 for place, county and minor civil division codes.",
        "- Until 2021 the survey identified the territories by their old",
        "  Census codes rather than by FIPS. `state_id` carries the FIPS code",
        "  throughout; the state tables' `geography_id` keeps the code as",
        "  published.",
        "",
        "## Further reading, not bundled",
        "",
    ]
    for title, url, note in LINK_ONLY:
        lines += [f"- **{title}** — {url}", f"  {note}", ""]
    return "\n".join(lines)


def main() -> int:
    WORK.mkdir(parents=True, exist_ok=True)
    config = tomllib.loads(
        (Path.home() / ".basedosdados/config.toml").read_text()
    )
    creds = service_account.Credentials.from_service_account_file(
        config["gcloud-projects"]["staging"]["credentials_path"]
    )
    gcs = storage.Client(credentials=creds, project=BUCKET)
    bucket = gcs.bucket(BUCKET, user_project=BUCKET)

    registered: dict[str, str] = {}
    for tables, (remote, local_name, covers) in DOCS.items():
        pdf = WORK / local_name
        if not pdf.exists():
            request = urllib.request.Request(
                DOC_BASE + remote, headers=constants.HEADERS.value
            )
            with urllib.request.urlopen(request, timeout=180) as response:
                pdf.write_bytes(response.read())
        archive = WORK / f"{tables[0].rsplit('_', 1)[0]}_auxiliary_files.zip"
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "README.md", readme(tables, local_name, covers, remote)
            )
            zf.write(pdf, local_name)
        for table in tables:
            key = f"auxiliary_files/{DATASET_ID}/{table}/auxiliary_files.zip"
            bucket.blob(key).upload_from_filename(str(archive))
            url = f"https://storage.googleapis.com/{BUCKET}/{key}"
            registered[table] = url
            print(f"{table}: {archive.stat().st_size:,} bytes -> {url}")

    print("\n=== anonymous fetch of every registered URL ===")
    for table, url in registered.items():
        request = urllib.request.Request(url, method="HEAD")
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                status = str(response.status)
        except urllib.error.HTTPError as exc:
            status = f"{exc.code} {exc.reason}"
        except Exception as exc:
            status = f"{type(exc).__name__}"
        print(f"  {table:24s} HTTP {status}")

    (WORK / "urls.txt").write_text(
        "\n".join(f"{t}\t{u}" for t, u in registered.items()), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
