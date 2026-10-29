"""Build and upload the us_census_bds auxiliary-file bundles, one per table.

Each bundle holds the BDS codebook and a README with the citation, file
provenance and what a user must know to read the table. Bundles go to
``gs://basedosdados-public``, which is not requester-pays, so the links work
for anonymous visitors.

Usage:
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.auxiliary_files [--upload]
"""

from __future__ import annotations

import argparse
import datetime as dt
import zipfile
from pathlib import Path

import requests

from models.us_census_bds.code.build_dbt import DESCRIPTIONS
from models.us_census_bds.code.upload import clients
from pipelines.datasets.us_census_bds.constants import constants

DATASET_ID = constants.DATASET_ID.value
WORK = constants.DATA_DIR.value / "auxiliary_files"
BUCKET = "basedosdados-public"
CODEBOOK_URL = (
    "https://www.census.gov/content/dam/Census/programs-surveys/"
    "business-dynamics-statistics/codebook-glossary.pdf"
)
LINKS = [
    (
        "BDS methodology",
        "https://www.census.gov/programs-surveys/bds/documentation/methodology.html",
    ),
    ("BDS datasets", constants.BASE_URL.value),
    (
        "Massell and Funk (2007), noise infusion for magnitude data",
        "https://nces.ed.gov/FCSM/pdf/2007FCSM_Massell-IX-B.pdf",
    ),
]
TABLES = ["country", "state", "county", "metropolitan_area"]

README = """# Business Dynamics Statistics (BDS): `{table}`

{description}

## Citation

U.S. Census Bureau, Business Dynamics Statistics, 2023 release (1978-2023).
{base}

## Files

| File | What it is | Source | Downloaded |
|---|---|---|---|
| `codebook_glossary.pdf` | BDS codebook and glossary, 2023 release: every variable, class and flag | {codebook} | {today} |

## Further documentation (not bundled)

{links}

## Reading this table

- Each row is one cell of one tabulation. The `breakdown` column names the
  BDS variables the tabulation crosses (for example `st,sector,fagecoarse`),
  and every breakdown covers the same universe. Filter on one breakdown
  before summing, or the same firms are counted several times.
- Fine and coarse classes of a variable share one column (`fage` and
  `fagecoarse` both land in `firm_age`); the breakdown says which applies.
- The NAICS sectors 31-33, 44-45 and 48-49 are stored as 31, 44 and 48, the
  keys of the NAICS 2017 directory.
- Suppressed cells are NULL. `suppression_flags` lists them as
  `measure=flag` pairs: D disclosure, S data quality, X structurally missing
  or zero, N rate not available.
- Employment is for the pay period including March 12, so every flow and
  rate is a March-to-March change. Figures carry noise infusion for
  disclosure avoidance.
- State tables count a multi-state firm once in each state. Puerto Rico
  (state 72) comes from the separate Puerto Rico files, covers 1998-2023, and
  sizes Puerto Rico firms by their stateside and Puerto Rico employment
  combined.
- County code 999 (statewide) is stored with a NULL `county_id`.
"""


def build(table: str, codebook: bytes) -> Path:
    WORK.mkdir(parents=True, exist_ok=True)
    today = dt.date.today().isoformat()
    readme = README.format(
        table=table,
        description=DESCRIPTIONS[table][0],
        base=constants.BASE_URL.value,
        codebook=CODEBOOK_URL,
        today=today,
        links="\n".join(f"- {name}: {url}" for name, url in LINKS),
    )
    path = WORK / f"{table}.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.md", readme)
        z.writestr("codebook_glossary.pdf", codebook)
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()
    codebook = requests.get(
        CODEBOOK_URL, headers=constants.HEADERS.value, timeout=120
    ).content
    if not codebook.startswith(b"%PDF"):
        raise ValueError("codebook download is not a PDF")
    paths = {t: build(t, codebook) for t in TABLES}
    print("built:", ", ".join(str(p) for p in paths.values()))
    if args.upload:
        gcs, _ = clients()
        bucket = gcs.bucket(BUCKET)
        for table, path in paths.items():
            name = f"auxiliary_files/{DATASET_ID}/{table}/auxiliary_files.zip"
            bucket.blob(name).upload_from_filename(str(path))
            url = f"https://storage.googleapis.com/{BUCKET}/{name}"
            status = requests.head(url, timeout=60).status_code
            print(f"{table}: {url} -> HTTP {status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
