"""Download and unzip the CFPB Consumer Complaint Database bulk export.

The downloader lives in ``pipelines/datasets/us_cfpb_complaints/utils.py`` and is
shared with the recurring pipeline; this is only the CLI around it.

The bulk export is the only usable route. The documented REST/CSV API at
www.consumerfinance.gov is behind Akamai, which returns "Access Denied" to scripted
clients regardless of user agent, and since release 23 (July 2026) its filtered CSV
export is capped at 100,000 complaints and its JSON export has been discontinued.

The export is a full snapshot of the whole database refreshed daily, roughly 1.4 GB
zipped and 9.3 GB as a single CSV.

    python download.py
"""

import argparse
from pathlib import Path

from common import INPUT, download_snapshot

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, default=INPUT)
    csv_path = download_snapshot(ap.parse_args().input)
    print(f"ready: {csv_path}")
