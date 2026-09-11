"""Download the ABS Household Income and Wealth (6523.0) source files.

Fetches two things into ``<input_dir>``:

  ``2019-20/*.xlsx``  the 16 data cubes of the 2019-20 release, the latest
                      published under the former catalogue number 6523.0.
  ``wp1351.pdf/.txt`` ABS working paper 1351.0 (2002), whose appendix 14.2
                      holds the only ABS estimates of household wealth by age
                      before the Survey of Income and Housing measured wealth.

abs.gov.au returns 403 to the default urllib agent, so a browser User-Agent is
required. The working paper sits on the retired Lotus Notes site and is only
reachable through its ``free.nsf/log?openagent`` download agent, with the whole
query string intact.

The PDF is converted with ``pdftotext -layout`` (poppler). Without ``-layout``
the appendix tables lose their column alignment and cannot be read back.

Usage:
    python download.py [input_dir]
"""

from __future__ import annotations

import io
import os
import shutil
import subprocess
import sys
import urllib.request
import zipfile

RELEASE = "2019-20"
CUBES_URL = (
    "https://www.abs.gov.au/statistics/economy/finance/"
    "household-income-and-wealth-australia/2019-20/Download-all.zip"
)
PAPER_URL = (
    "https://www.abs.gov.au/AUSTATS/free.nsf/log?openagent"
    "&1351_1994%20to%202000.pdf&1351.0&Publication"
    "&736801C70B5A72D9CA256C44000D995C&&1994%20to%202000&30.09.2002&Latest"
)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
DEFAULT_INPUT = os.path.join(
    os.path.expanduser("~"),
    "Downloads",
    "au_abs_household_income_wealth_data",
    "input",
)


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request) as response:
        return response.read()


def download_cubes(input_dir: str) -> None:
    target = os.path.join(input_dir, RELEASE)
    os.makedirs(target, exist_ok=True)
    print(f"downloading {CUBES_URL}")
    with zipfile.ZipFile(io.BytesIO(fetch(CUBES_URL))) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".xlsx")]
        archive.extractall(target, members=names)
    print(f"  extracted {len(names)} cubes to {target}")


def download_paper(input_dir: str) -> None:
    os.makedirs(input_dir, exist_ok=True)
    pdf_path = os.path.join(input_dir, "wp1351.pdf")
    text_path = os.path.join(input_dir, "wp1351.txt")
    print(f"downloading {PAPER_URL[:80]}...")
    blob = fetch(PAPER_URL)
    if not blob.startswith(b"%PDF"):
        raise SystemExit(
            "the working paper download did not return a PDF; the ABS legacy"
            " download agent needs the full query string, unescaped"
        )
    with open(pdf_path, "wb") as handle:
        handle.write(blob)
    print(f"  wrote {pdf_path} ({len(blob):,} bytes)")

    if shutil.which("pdftotext") is None:
        raise SystemExit(
            "pdftotext is not installed (brew install poppler); without it the"
            " appendix tables cannot be read out of the PDF"
        )
    subprocess.run(["pdftotext", "-layout", pdf_path, text_path], check=True)
    print(f"  wrote {text_path}")


def main() -> None:
    input_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    download_cubes(input_dir)
    download_paper(input_dir)


if __name__ == "__main__":
    main()
