"""Download raw data for world_dasanaike_sage (SAGE — Small-Area Global Elections).

Source: Noah Dasanaike, "The Small-Area Global Elections (SAGE) Dataset",
Harvard Dataverse doi:10.7910/DVN/YGJR1L (v2.0), License CC0 1.0.

Files:
    - sage.parquet (~863 MB): the consolidated, harmonized returns for all 110
      countries (284,271,492 rows, 56 columns). Fetched from the public GCS
      archive gs://sage-archive/ (anonymous read). This single file — not the
      reduced hive-partitioned copy under gs://sage-archive/parquet/ — is the
      full 56-column schema and is the source for the main table.
    - spatial_admin_crosswalk.parquet (~60 MB): coordinate -> GADM/NUTS/FIPS
      crosswalk, from gs://sage-archive/crosswalks/.
    - netherlands_preference_votes_by_stembureau.parquet (~6.6 MB): Dutch
      preference votes (2017, 2021, 2023), Harvard Dataverse datafile 14036184.
    - germany_erststimme_candidates_by_wahlkreis.parquet (~0.09 MB): German
      constituency first votes, Harvard Dataverse datafile 14036183.

Note: the Dataverse *_2021.parquet (datafile 14036185) is an exact duplicate of
the 2021 subset already inside the main Netherlands file and is not downloaded.
"""

import time
from pathlib import Path

import requests

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

FILES = {
    "sage.parquet": "https://storage.googleapis.com/sage-archive/sage.parquet",
    "spatial_admin_crosswalk.parquet": (
        "https://storage.googleapis.com/sage-archive/crosswalks/"
        "spatial_admin_crosswalk.parquet"
    ),
    "netherlands_preference_votes_by_stembureau.parquet": (
        "https://dataverse.harvard.edu/api/access/datafile/14036184"
        "?format=original"
    ),
    "germany_erststimme_candidates_by_wahlkreis.parquet": (
        "https://dataverse.harvard.edu/api/access/datafile/14036183"
        "?format=original"
    ),
}

INPUT_DIR = Path(__file__).resolve().parents[1] / "input"
INPUT_DIR.mkdir(parents=True, exist_ok=True)


def download_file(url: str, dest: Path, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            r = requests.get(
                url, timeout=600, stream=True, headers={"User-Agent": UA}
            )
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
            print(f"Downloaded: {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
            return
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError(f"Failed to download {url}: {e}") from e


def main() -> None:
    for filename, url in FILES.items():
        dest = INPUT_DIR / filename
        if dest.exists() and dest.stat().st_size > 0:
            print(f"Skip (exists): {dest}")
            continue
        download_file(url, dest)


if __name__ == "__main__":
    main()
