"""Record the CSV header of every source file, once, into headers.json.

Headers are read straight from the network: the current program years by HTTP
range request, the archived years by decompressing only the first block of the
relevant ZIP member. Nothing is downloaded in full.
"""

import csv
import io
import json
import subprocess
from pathlib import Path

import constants as c
import remote_zip


def current_header(url: str) -> list[str]:
    raw = subprocess.run(
        ["curl", "-s", "-m", "120", "-r", "0-200000", url], capture_output=True
    ).stdout
    line = raw.split(b"\n")[0].decode("utf-8-sig", errors="replace")
    return next(csv.reader(io.StringIO(line)))


def archived_headers(year: int) -> dict[str, list[str]]:
    url = c.archive_url(year)
    entries = remote_zip.central_dir(url)
    out = {}
    for kind, stem in c.DETAIL_KINDS.items():
        match = next(e for e in entries if f"OP_DTL_{stem}_" in e["name"])
        blob = remote_zip.head_of(url, match)
        line = blob.split(b"\n")[0].decode("utf-8-sig", errors="replace")
        out[kind] = next(csv.reader(io.StringIO(line)))
    return out


if __name__ == "__main__":
    headers: dict[str, dict[str, list[str]]] = {
        "detail": {},
        "profile": {},
        "summary": {},
    }

    for year in c.ARCHIVE_ZIPS:
        for kind, cols in archived_headers(year).items():
            headers["detail"][f"{kind}_{year}"] = cols
            print(f"detail {kind} {year}: {len(cols)} cols")
    for year in c.CURRENT_YEARS:
        for kind in c.DETAIL_KINDS:
            cols = current_header(c.detail_url(year, kind))
            headers["detail"][f"{kind}_{year}"] = cols
            print(f"detail {kind} {year}: {len(cols)} cols")
    for name, url in c.PROFILE_FILES.items():
        headers["profile"][name] = current_header(url)
        print(f"profile {name}: {len(headers['profile'][name])} cols")
    for name, stem in c.SUMMARY_PER_YEAR.items():
        url = c.summary_url(stem.format(year=c.SUMMARY_YEARS[-1]))
        headers["summary"][name] = current_header(url)
        print(f"summary {name}: {len(headers['summary'][name])} cols")
    for name, (stem, joined) in c.SUMMARY_ALL_YEARS.items():
        headers["summary"][name] = current_header(c.summary_url(stem, joined))
        print(f"summary {name}: {len(headers['summary'][name])} cols")

    # tables.py always reads this from the module directory, so writing it
    # relative to the cwd would leave the consumer on a stale snapshot.
    with open(Path(__file__).resolve().parent / "headers.json", "w") as fh:
        json.dump(headers, fh, indent=1)
