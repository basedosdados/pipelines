"""Download raw data for world_un_wpp (UN World Population Prospects, 2024 revision).

Source: https://population.un.org/wpp/downloads (JavaScript SPA — do not scrape).
Files are fetched via direct asset URLs. Files are kept gzipped; never decompress
or modify files in input/ after saving.

Note: a mid-year (1 July) population-by-5-year-age-group file does not exist
on the portal (checked 2026-07-09); the 1 January file is used instead. The
single-age files are 1 July estimates.
"""

import time
from pathlib import Path

import requests

BASE_URL = (
    "https://population.un.org/wpp/assets/Excel%20Files/"
    "1_Indicator%20(Standard)/CSV_FILES/"
)

FILES = [
    "WPP2024_Demographic_Indicators_Medium.csv.gz",
    "WPP2024_Population1JanuaryByAge5GroupSex_Medium.csv.gz",
    "WPP2024_PopulationBySingleAgeSex_Medium_1950-2023.csv.gz",
    "WPP2024_PopulationBySingleAgeSex_Medium_2024-2100.csv.gz",
]

INPUT_DIR = Path(__file__).resolve().parents[1] / "input"
INPUT_DIR.mkdir(parents=True, exist_ok=True)


def download_file(url: str, dest: Path, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=120, stream=True)
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(
                f"Downloaded: {dest} ({dest.stat().st_size / 1e6:.1f} MB) <- {url}"
            )
            return
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(f"Failed to download {url}: {e}") from e


def main() -> None:
    for filename in FILES:
        dest = INPUT_DIR / filename
        if dest.exists() and dest.stat().st_size > 0:
            print(f"Skip (exists): {dest}")
            continue
        download_file(BASE_URL + filename, dest)


if __name__ == "__main__":
    main()
