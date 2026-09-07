"""Download and unzip the CFPB Consumer Complaint Database bulk export.

    python download.py            # download + unzip into <DATA_DIR>/input
    python download.py --keep-zip # leave the .zip in place afterwards

The bulk export at files.consumerfinance.gov is the only usable route. The
documented REST/CSV API at www.consumerfinance.gov is behind Akamai, which returns
"Access Denied" to scripted clients regardless of user agent, and since release 23
(July 2026) its filtered CSV export is capped at 100,000 complaints and its JSON
export has been discontinued.

The export is a full snapshot of the whole database refreshed daily, roughly 1.4 GB
zipped and 9.3 GB as a single CSV.
"""

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests
from common import BULK_URL, CSV_NAME, INPUT, ZIP_NAME

CHUNK = 8 * 1024 * 1024


def head() -> dict:
    """Return the export's Content-Length and Last-Modified without downloading."""
    r = requests.head(BULK_URL, timeout=60, allow_redirects=True)
    r.raise_for_status()
    return {
        "content_length": int(r.headers.get("content-length", 0)),
        "last_modified": r.headers.get("last-modified", ""),
    }


def download(input_dir: Path) -> Path:
    input_dir.mkdir(parents=True, exist_ok=True)
    meta = head()
    size = meta["content_length"]
    print(f"source: {BULK_URL}")
    print(f"  last-modified: {meta['last_modified']}")
    print(f"  size:          {size / 1e9:.2f} GB")

    zip_path = input_dir / ZIP_NAME
    if zip_path.exists() and zip_path.stat().st_size == size:
        print(f"  already downloaded: {zip_path}")
        return zip_path

    t0 = time.time()
    got = 0
    with requests.get(BULK_URL, stream=True, timeout=(30, 300)) as r:
        r.raise_for_status()
        # decode_content=True so a Content-Encoding'd body is decompressed rather
        # than written as raw transport bytes.
        r.raw.decode_content = True
        with open(zip_path, "wb") as fh:
            while True:
                block = r.raw.read(CHUNK)
                if not block:
                    break
                fh.write(block)
                got += len(block)
                if got % (256 * 1024 * 1024) < CHUNK:
                    print(
                        f"  ...{got / 1e9:.2f}/{size / 1e9:.2f} GB "
                        f"({time.time() - t0:.0f}s)",
                        flush=True,
                    )
    if size and zip_path.stat().st_size != size:
        raise SystemExit(
            f"short download: got {zip_path.stat().st_size} bytes, expected {size}"
        )
    print(f"  downloaded in {time.time() - t0:.0f}s -> {zip_path}")
    return zip_path


def unzip(zip_path: Path, input_dir: Path) -> Path:
    csv_path = input_dir / CSV_NAME
    print(f"unzipping -> {csv_path}")
    subprocess.run(
        ["unzip", "-o", str(zip_path), "-d", str(input_dir)],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    if not csv_path.exists():
        raise SystemExit(f"expected {csv_path} after unzip")
    print(f"  {csv_path.stat().st_size / 1e9:.2f} GB")
    return csv_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path, default=INPUT)
    ap.add_argument("--keep-zip", action="store_true")
    a = ap.parse_args()

    if shutil.which("unzip") is None:
        sys.exit("`unzip` not found on PATH")
    zip_path = download(a.input)
    unzip(zip_path, a.input)
    if not a.keep_zip:
        zip_path.unlink()
        print(f"removed {zip_path}")


if __name__ == "__main__":
    main()
