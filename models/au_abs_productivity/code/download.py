"""Download the ABS Estimates of Industry Multifactor Productivity (5260.0.55.002).

Fetches the "Time series spreadsheets all" zip for the latest release and extracts
the three .xlsx workbooks into an input directory. A browser User-Agent is
required; abs.gov.au returns 403 to the default requests/urllib agent.

Scratch data must not live in the repo or under Dropbox. The default directory is
``~/Library/Caches/au_abs_productivity_data/input``, overridable with
``AU_ABS_PRODUCTIVITY_DATA_DIR``.

Usage:
    uv run models/au_abs_productivity/code/download.py [input_dir]
"""

import io
import os
import sys
import urllib.request
import zipfile

RELEASE = "2024-25"
ZIP_URL = (
    "https://www.abs.gov.au/statistics/industry/industry-overview/"
    f"estimates-industry-multifactor-productivity/{RELEASE}/"
    "Time-series-spreadsheets-all.zip"
)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

DEFAULT_DATA_DIR = os.path.expanduser(
    os.environ.get(
        "AU_ABS_PRODUCTIVITY_DATA_DIR",
        "~/Library/Caches/au_abs_productivity_data",
    )
)


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
    for n in sorted(names):
        print(f"  {n}")


if __name__ == "__main__":
    out = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.path.join(DEFAULT_DATA_DIR, "input")
    )
    main(out)
