"""Download one year of HMDA LAR to the scratch input dir.

  python download.py modern 2024     # -> input/modern_2024.csv  (nationwide CSV, ~4.6 GB)
  python download.py legacy 2017     # -> input/legacy_2017.csv  (unzipped from _codes.zip)

Modern: the data-browser nationwide endpoint 301-redirects to a pre-generated CSV on
files.ffiec.cfpb.gov (follow with -L). Legacy: the historic `_codes.zip` (raw numeric
codes) unzipped to a single CSV; the zip is removed after extraction.

Idempotent: skips the download if the target CSV already exists and is non-empty.
"""

import subprocess
import sys
import zipfile
from pathlib import Path

from common import INPUT, LEGACY_YEARS, MODERN_YEARS

MODERN_URL = "https://ffiec.cfpb.gov/v2/data-browser-api/view/nationwide/csv?years={year}"
LEGACY_URL = (
    "https://files.consumerfinance.gov/hmda-historic-loan-data/"
    "hmda_{year}_nationwide_all-records_codes.zip"
)


def _curl(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    cmd = [
        "curl",
        "-fL",
        "--retry",
        "4",
        "--retry-delay",
        "5",
        "--connect-timeout",
        "30",
        "-o",
        str(tmp),
        url,
    ]
    print("  $", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)
    tmp.rename(dest)


def download(era: str, year: int) -> Path:
    if era == "modern":
        dest = INPUT / f"modern_{year}.csv"
        if dest.exists() and dest.stat().st_size > 0:
            print(f"  exists, skip: {dest} ({dest.stat().st_size:,} B)")
            return dest
        _curl(MODERN_URL.format(year=year), dest)
    elif era == "legacy":
        dest = INPUT / f"legacy_{year}.csv"
        if dest.exists() and dest.stat().st_size > 0:
            print(f"  exists, skip: {dest} ({dest.stat().st_size:,} B)")
            return dest
        zpath = INPUT / f"legacy_{year}.zip"
        _curl(LEGACY_URL.format(year=year), zpath)
        with zipfile.ZipFile(zpath) as zf:
            inner = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if len(inner) != 1:
                raise RuntimeError(f"expected 1 CSV in {zpath}, found {inner}")
            with zf.open(inner[0]) as src, open(dest, "wb") as out:
                while chunk := src.read(1 << 20):
                    out.write(chunk)
        zpath.unlink()
    else:
        raise SystemExit(f"unknown era {era!r} (use modern|legacy)")
    print(f"  downloaded {dest} ({dest.stat().st_size:,} B)")
    return dest


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    era, year = sys.argv[1], int(sys.argv[2])
    valid = MODERN_YEARS if era == "modern" else LEGACY_YEARS
    if year not in valid:
        raise SystemExit(f"{era} year must be in {valid[0]}..{valid[-1]}")
    download(era, year)
