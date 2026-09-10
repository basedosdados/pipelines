"""Download the raw sources for au_abs_migration.

Two kinds of source:

* four time-series spreadsheets from the Overseas Migration release
  (ABS cat. 3407.0), which carry the country-of-birth and annual visa detail;
* five SDMX dataflows from the ABS Data API, which carry the age/sex and
  quarterly visa detail the spreadsheets omit.

Everything lands under ``$AU_ABS_MIGRATION_DATA/input`` (default
``~/Downloads/au_abs_migration_data``), never inside the repo or Dropbox.
"""

from __future__ import annotations

import os
from pathlib import Path

import requests

DATA_DIR = Path(
    os.environ.get(
        "AU_ABS_MIGRATION_DATA",
        Path.home() / "Downloads" / "au_abs_migration_data",
    )
)
INPUT_DIR = DATA_DIR / "input"

RELEASE = "2024-25"
SPREADSHEET_BASE = "https://www.abs.gov.au/statistics/people/population/overseas-migration/2024-25"
SPREADSHEETS = {
    "34070DO001_202425.xlsx": "net overseas migration by country of birth, by state",
    "34070DO002_202425.xlsx": "overseas migrant arrivals by country of birth, by state",
    "34070DO003_202425.xlsx": "overseas migrant departures by country of birth, by state",
    "34070DO004_202425.xlsx": "arrivals and departures by visa and citizenship group, by state",
}

SDMX_BASE = "https://data.api.abs.gov.au/rest"
DATAFLOWS = {
    "NOM_FY": "1.0.0",
    "NOM_CY": "1.0.0",
    "OMAD_VISA": "1.0.0",
    "NIM_FY": "1.0.0",
    "NIM_CY": "1.0.0",
}
# Full SACC country names, used to label the SACC codes the spreadsheets ship
# abbreviated. ERP_COB is the only ABS dataflow whose codelist covers them.
CODELIST_SOURCE = "ERP_COB"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
}


def _get(url: str, headers: dict[str, str], timeout: int = 600) -> bytes:
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.content


def download_spreadsheets(input_dir: Path = INPUT_DIR) -> list[Path]:
    input_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in SPREADSHEETS:
        path = input_dir / name
        if not path.exists():
            path.write_bytes(_get(f"{SPREADSHEET_BASE}/{name}", HEADERS))
        paths.append(path)
        print(f"{path.name}: {path.stat().st_size:,} bytes")
    return paths


def download_dataflows(input_dir: Path = INPUT_DIR) -> list[Path]:
    input_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for flow, version in DATAFLOWS.items():
        path = input_dir / f"{flow}.csv"
        if not path.exists():
            url = (
                f"{SDMX_BASE}/data/ABS,{flow},{version}/all"
                "?dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
            )
            path.write_bytes(_get(url, {**HEADERS, "Accept": "text/csv"}))
        paths.append(path)
        print(f"{path.name}: {path.stat().st_size:,} bytes")
    return paths


def download_country_codelist(input_dir: Path = INPUT_DIR) -> Path:
    input_dir.mkdir(parents=True, exist_ok=True)
    path = input_dir / "CL_ERP_COB.json"
    if not path.exists():
        url = f"{SDMX_BASE}/datastructure/ABS/{CODELIST_SOURCE}?references=children"
        headers = {
            **HEADERS,
            "Accept": "application/vnd.sdmx.structure+json;version=1.0",
        }
        path.write_bytes(_get(url, headers))
    print(f"{path.name}: {path.stat().st_size:,} bytes")
    return path


def main() -> None:
    download_spreadsheets()
    download_dataflows()
    download_country_codelist()


if __name__ == "__main__":
    main()
