"""Download the ABS Australian System of National Accounts (5204.0) source files.

Fetches the "All time series spreadsheets" zip for the latest release and extracts
the 72 .xlsx workbooks into an input directory. A browser User-Agent is required;
abs.gov.au returns 403 to the default requests/urllib agent.

Usage:
    python download.py <output_input_dir>   # default: models/au_abs_national_accounts/input
"""

import io
import os
import sys
import urllib.request
import zipfile

RELEASE = "2024-25"
ZIP_URL = (
    "https://www.abs.gov.au/statistics/economy/national-accounts/"
    f"australian-system-national-accounts/{RELEASE}/All-time-series-spreadsheets.zip"
)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def main(input_dir: str):
    os.makedirs(input_dir, exist_ok=True)
    req = urllib.request.Request(ZIP_URL, headers={"User-Agent": UA})
    print(f"Downloading {ZIP_URL}")
    with urllib.request.urlopen(req) as resp:
        blob = resp.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".xlsx")]
        zf.extractall(input_dir, members=names)
    print(f"Extracted {len(names)} xlsx files to {input_dir}")


if __name__ == "__main__":
    out = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "input",
        )
    )
    main(out)
