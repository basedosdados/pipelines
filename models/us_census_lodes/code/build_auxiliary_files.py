"""Build the per-table auxiliary-file bundles for us_census_lodes.

    uv run python models/us_census_lodes/code/build_auxiliary_files.py

Writes one ZIP per table under ``<LODES_DATA_ROOT>/aux/bundles/``, each with a
README recording the citation, per-file provenance and download date, plus the
transformations this onboarding applied.

Upload is a separate step (the bucket write needs prod credentials):

    gcloud storage cp <bundle> \\
      gs://basedosdados-public/auxiliary_files/us_census_lodes/<table>/auxiliary_files.zip

Then verify the published URL **anonymously** and report what it actually
returns. As of 2026-09-06 the auxiliary-file links are broken platform-wide:
`basedosdados-dev` is requester-pays (HTTP 400 `UserProjectMissing`) and the
migration to `basedosdados-public` moved the metadata without moving the
objects (HTTP 404). That is not specific to this dataset.
"""

from __future__ import annotations

import shutil
import sys
import zipfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_lodes.constants import (
    BASE_URL,
    DATA_ROOT,
)

AUX = DATA_ROOT / "aux"
BUNDLES = AUX / "bundles"
DOWNLOADED = date.today().isoformat()

# Bundled per table. The tech document is the data dictionary and is useless to
# omit; the geography note explains the 2020-block restatement that makes this
# release incomparable with LODES 7.
DOCS = {
    "LODESTechDoc8.4.pdf": (
        "LODES Technical Document, format version 8.4 -- the authoritative data "
        "dictionary for every column, job type and workforce segment",
        f"{BASE_URL}/LODESTechDoc8.4.pdf",
    ),
    "OnTheMap2020Geography.pdf": (
        "How the Census Bureau translated the historical series onto 2020 census "
        "blocks. Read this before comparing LODES 8 with LODES 6 or 7",
        "https://lehd.ces.census.gov/doc/help/onthemap/OnTheMap2020Geography.pdf",
    ),
    "FederalEmploymentInOnTheMap.pdf": (
        "Scope and method of the federal employment data behind job types JT04 "
        "and JT05",
        "https://lehd.ces.census.gov/doc/help/onthemap/FederalEmploymentInOnTheMap.pdf",
    ),
    "LODESDataNote-FedEmp2015.pdf": (
        "Data note on the 2015 revision to federal employment coverage",
        "https://lehd.ces.census.gov/doc/help/onthemap/LODESDataNote-FedEmp2015.pdf",
    ),
}

# Which documents each table's users actually need.
PER_TABLE = {
    "residence_jobs": list(DOCS),
    "workplace_jobs": list(DOCS),
    "geography_crosswalk": [
        "LODESTechDoc8.4.pdf",
        "OnTheMap2020Geography.pdf",
    ],
}

CITATION = (
    "U.S. Census Bureau. Longitudinal Employer-Household Dynamics Program. "
    "LEHD Origin-Destination Employment Statistics (LODES), version 8.4, "
    "data vintage 20251202. Washington, DC: U.S. Census Bureau. "
    "https://lehd.ces.census.gov/data/lodes/"
)

TRANSFORMS = {
    "residence_jobs": """
- The published wide layout is preserved; the `C*` columns are renamed to
  readable English (`CNS04` -> `jobs_naics_23`, `CD04` ->
  `jobs_education_bachelors_or_higher`). `original_name` in the Data Basis
  column metadata records the source name for every column.
- Only the `S000` workforce segment is included. The other nine segments
  (`SA01`-`SA03`, `SE01`-`SE03`, `SI01`-`SI03`) repeat the same column grid
  restricted to a worker subset, i.e. they carry two-way interactions.
- Race, ethnicity, educational attainment and sex are published only for data
  year 2009 onward. LODES fills the earlier years with literal zeroes; those
  cells are stored as NULL here, so a sum over 2002-2008 reads as "not
  collected" rather than "none". Educational attainment covers only workers
  aged 30 and over even inside its window.
- `state_id`, `county_id` and `census_tract_id` are taken from the LODES
  geography crosswalk of this same release, NOT by slicing `block_id`. The two
  disagree: Connecticut replaced counties with planning regions in 2022, so its
  2020 block codes carry the legacy county while the crosswalk carries the
  planning region.
""",
    "workplace_jobs": """
- As `residence_jobs` above, plus:
- Firm age and firm size are published only for data year 2011 onward and only
  for job type JT02 (All Private Jobs). Outside that window the cells are NULL,
  not zero.
""",
    "geography_crosswalk": """
- Columns are renamed to readable English (`tabblk2020` -> `block_id`,
  `stcd119` -> `congressional_district_id`); `original_name` in the Data Basis
  column metadata records the source name for every column.
- An inapplicable geography is padded in the source with an all-nines code of
  the column's own width (`99999`, `9999999`, ...) and an empty name. Both are
  stored as NULL here so a join never matches a placeholder.
- This file reflects the delineation current at the LODES 8.4 release and is
  replaced wholesale at each new version.
""",
}


def readme(table: str, files: list[str]) -> str:
    lines = [
        f"# us_census_lodes / {table} -- auxiliary files",
        "",
        "## Citation",
        "",
        CITATION,
        "",
        "## Licence",
        "",
        "U.S. Government work, public domain (17 U.S.C. 105).",
        "",
        "## Contents",
        "",
    ]
    for name in files:
        desc, url = DOCS[name]
        lines += [
            f"### `{name}`",
            "",
            f"{desc}.",
            "",
            f"- Source: {url}",
            f"- Downloaded: {DOWNLOADED}",
            "",
        ]
    lines += [
        "## What this onboarding changed",
        "",
        "Read this before comparing the Data Basis table with the raw LODES files.",
        TRANSFORMS[table].strip(),
        "",
        "## Coverage gaps",
        "",
        "LODES publishes nothing for a state-year-jobtype with no data. Measured",
        "gaps: Alaska has no workplace file from 2017, Michigan from 2022, and",
        "Puerto Rico has residence files only for 2002-2008 and no workplace file",
        "in any year. Arizona, DC, Mississippi and New Hampshire are missing early",
        "workplace years. The full table is in the dataset's `coverage.md`.",
        "",
        "## Not bundled, read at the publisher",
        "",
        "- OnTheMap application and its help pages: "
        "https://onthemap.ces.census.gov/",
        "- LED Partnership and state partner list: "
        "https://lehd.ces.census.gov/state_partners/",
        "- LEHD research papers on firm age and firm size: "
        "https://lehd.ces.census.gov/research/",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    missing = [n for n in DOCS if not (AUX / n).exists()]
    if missing:
        raise SystemExit(
            f"missing source documents in {AUX}: {missing}. Download them first "
            "(see the URLs in DOCS)."
        )
    BUNDLES.mkdir(parents=True, exist_ok=True)
    for table, files in PER_TABLE.items():
        dest = BUNDLES / f"{table}.zip"
        with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("README.md", readme(table, files))
            for name in files:
                z.write(AUX / name, name)
        size = dest.stat().st_size
        print(
            f"{table}: {len(files) + 1} entries, {size / 1024:.0f} KB -> {dest}"
        )
        print(
            "  gcloud storage cp "
            f"'{dest}' gs://basedosdados-public/auxiliary_files/"
            f"us_census_lodes/{table}/auxiliary_files.zip"
        )
    shutil.rmtree(BUNDLES / "__pycache__", ignore_errors=True)


if __name__ == "__main__":
    main()
