#!/usr/bin/env python3
"""Build one auxiliary-file bundle per table from the BLS documentation.

Each BLS program ships a `<prog>.txt` technical document (file layouts, the
series-id decomposition, the meaning of every lookup file) and a set of
dimension lookup files. Two things in those files did not survive into the
tables and are only available here: the `display_level` and `sort_sequence`
columns that give each classification its hierarchy, and the full text of the
program documentation.

Writes <data>/auxiliary/<table>/auxiliary_files.zip.

Usage:
    uv run python models/us_bls_employment/code/build_auxiliary_files.py
"""

import os
import zipfile
from datetime import date
from pathlib import Path

from pipelines.datasets.us_bls_employment.constants import constants

DATA_ROOT = Path(
    os.environ.get(
        "US_BLS_EMPLOYMENT_DATA",
        os.path.expanduser("~/Downloads/us_bls_employment_data"),
    )
)
BASE = constants.BASE_URL.value
PROGRAMS = constants.PROGRAMS.value

# Self-describing names for files BLS names by program prefix.
RENAME = {
    "txt": "program_documentation.txt",
    "series": "series_catalogue.tsv",
    "datatype": "lookup_data_type.tsv",
    "data_type": "lookup_data_type.tsv",
    "supersector": "lookup_supersector.tsv",
    "industry": "lookup_industry.tsv",
    "area": "lookup_area.tsv",
    "area_type": "lookup_area_type.tsv",
    "measure": "lookup_measure.tsv",
    "state": "lookup_state.tsv",
    "state_region_division": "lookup_state_region_division.tsv",
    "sizeclass": "lookup_size_class.tsv",
    "dataelement": "lookup_data_element.tsv",
    "ratelevel": "lookup_rate_or_level.tsv",
    "seasonal": "lookup_seasonal_adjustment.tsv",
    "period": "lookup_period.tsv",
    "footnote": "lookup_footnote.tsv",
}

README = """# {table} — auxiliary files

Documentation published by the U.S. Bureau of Labor Statistics alongside the
`{prog}` program, bundled for the `us_bls_employment.{table}` table.

## Citation

U.S. Bureau of Labor Statistics, {name}. Retrieved from
{url}

## Contents

{contents}

Every file was downloaded from `{url}` on {today}.

## What is in here that is not in the table

- **`program_documentation.txt`** — the authoritative description of the file
  layouts and of how the series id decomposes into its dimensions. Section 2
  lists what each observation file contains, which is what determines the
  minimal set that makes a complete history.
- **`lookup_*.tsv`** — the code-to-label maps. The labels themselves are in the
  `dicionario` table, but these files also carry `display_level` and
  `sort_sequence`, which give each classification its hierarchy and are not
  otherwise available.
- **`series_catalogue.tsv`** — one row per series, with the begin and end period
  of each, so a series can be checked for coverage without scanning the table.

## Notes on the data

- BLS prints `-` for an observation it did not publish. Those are NULL in the
  table, not zero.
- Seasonally adjusted and unadjusted values are different series with different
  ids. The table keeps both and marks them in `seasonal_adjustment`.
- `download.bls.gov` returns HTTP 403 to a request without a browser
  User-Agent header.
"""


def build(table: str, prog: str) -> Path:
    """Bundle one program's documentation for one table."""
    src = DATA_ROOT / "input" / prog
    out = DATA_ROOT / "auxiliary" / table
    out.mkdir(parents=True, exist_ok=True)
    files = []
    for suffix in ["txt", *constants.DIM_FILES.value[prog]]:
        path = src / f"{prog}.{suffix}"
        if path.exists():
            files.append((path, RENAME.get(suffix, f"{suffix}.tsv")))
    contents = "\n".join(
        f"- `{name}` — from `{p.name}`" for p, name in sorted(files, key=lambda x: x[1])
    )
    readme = README.format(
        table=table, prog=prog, name=NAMES[table],
        url=f"{BASE}/{prog}/", contents=contents,
        today=date.today().isoformat(),
    )
    zpath = out / "auxiliary_files.zip"
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.md", readme)
        for path, name in files:
            z.write(path, name)
    print(f"{table}: {len(files) + 1} files, {zpath.stat().st_size / 1e6:.1f} MB")
    return zpath


NAMES = {
    "ces_national": "Current Employment Statistics (National)",
    "ces_state_metro": "State and Metro Area Employment, Hours, and Earnings",
    "laus": "Local Area Unemployment Statistics",
    "jolts": "Job Openings and Labor Turnover Survey",
}


def main() -> None:
    """Build every table's bundle."""
    for table, prog in PROGRAMS.items():
        build(table, prog)


if __name__ == "__main__":
    main()
