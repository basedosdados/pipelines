"""Download, clean and upload every contribution cycle, one at a time.

The 23 cycle files total ~75 GiB compressed and their Parquet comes to roughly
82 GB, well beyond the free space on the machine doing the work. So nothing is
staged in bulk: each cycle is downloaded, converted straight into GCS-bound part
files that are uploaded and deleted as they are written, and its source file is
removed before the next cycle starts. Peak local usage is one source file plus a
couple of parts.

Progress is recorded in ``backfill_state.json`` so an interrupted run resumes at
the next unfinished cycle instead of re-uploading what already landed.

    export GOOGLE_APPLICATION_CREDENTIALS=<dev service account key>
    uv run --with duckdb --with google-cloud-storage --with google-cloud-bigquery \\
        --with pyarrow python run_backfill.py

Add ``--load`` on the final run to build the BigQuery staging table from the
uploaded prefix, or run it separately once every cycle is in.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clean
import constants
import upload

STATE_FILE = Path(__file__).resolve().parent / "backfill_state.json"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"cycles": {}}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


def free_gb(path: Path) -> float:
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize / 1e9


def download_cycle(cycle: int) -> Path:
    """Fetch one cycle's source file if it is not already present."""
    dest = clean.INPUT / f"contribDB_{cycle}.csv.gz"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    part = dest.with_suffix(".gz.part")
    url = constants.CONTRIBUTION_URLS[cycle]
    print(f"  downloading cycle {cycle} ...", flush=True)
    subprocess.run(
        [
            "curl",
            "-sSL",
            "-A",
            USER_AGENT,
            "--retry",
            "3",
            "--max-time",
            "10800",
            url,
            "-o",
            str(part),
        ],
        check=True,
    )
    part.rename(dest)
    print(f"  downloaded {dest.stat().st_size / 1e9:.2f} GB", flush=True)
    return dest


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--cycle", type=int, action="append", help="restrict to these cycles"
    )
    p.add_argument(
        "--load",
        action="store_true",
        help="build the staging table at the end",
    )
    p.add_argument(
        "--reset",
        action="store_true",
        help="clear the staging prefix and start over",
    )
    args = p.parse_args()

    state = load_state()
    cycles = args.cycle or clean.CYCLES

    if args.reset:
        n = upload.clear_prefix("contribution")
        print(f"cleared {n} blob(s) from the staging prefix")
        state = {"cycles": {}}
        save_state(state)
    if not state["cycles"]:
        upload.write_header_blob("contribution")

    for cycle in cycles:
        key = str(cycle)
        if key in state["cycles"]:
            print(
                f"cycle {cycle}: already done ({state['cycles'][key]['rows']:,} rows)"
            )
            continue
        t0 = time.time()
        download_cycle(cycle)
        try:
            rows, parts = upload.stream_cycle(cycle)
        except upload.ConversionError:
            # Almost always a stray non-UTF-8 byte in the source. Repair the
            # file once and retry; a second failure is a real problem.
            removed = clean.sanitize_source(
                clean.INPUT / f"contribDB_{cycle}.csv.gz"
            )
            print(
                f"  cycle {cycle}: source had {removed} invalid-UTF-8 line(s), repaired",
                flush=True,
            )
            rows, parts = upload.stream_cycle(cycle)
        # The codebook figure counts physical lines, so it is an upper bound on
        # the record count: the header is one line and some fields carry newlines
        # inside quotes. Flag only a shortfall too large to be embedded newlines.
        ceiling = constants.CODEBOOK_ROWS[cycle] - 1
        gap = ceiling - rows
        if rows > ceiling:
            flag = f"  <-- ABOVE codebook ceiling {ceiling:,}"
        elif gap > max(50, ceiling * 0.001):
            flag = f"  <-- {gap:,} SHORT of codebook ceiling {ceiling:,}"
        else:
            flag = f"  (codebook {ceiling:,}; {gap} embedded newline(s))"
        state["cycles"][key] = {"rows": rows, "parts": parts}
        save_state(state)
        print(
            f"cycle {cycle}: {rows:,} rows, {parts} part(s), "
            f"{time.time() - t0:.0f}s, {free_gb(clean.SCRATCH):.1f} GB free{flag}",
            flush=True,
        )

    total = sum(v["rows"] for v in state["cycles"].values())
    print(f"\ncycles complete: {len(state['cycles'])}/{len(clean.CYCLES)}")
    print(f"rows uploaded  : {total:,}")
    if len(state["cycles"]) == len(clean.CYCLES):
        print(
            f"codebook total : {sum(constants.CODEBOOK_ROWS.values()) - len(clean.CYCLES):,}"
        )

    if args.load:
        loaded = upload.load_staging_table("contribution")
        print(
            f"BigQuery {upload.STAGING_DATASET}.contribution: {loaded:,} rows"
        )
        if loaded != total:
            raise SystemExit(
                f"MISMATCH: uploaded {total:,}, BigQuery has {loaded:,}"
            )
        print("row counts match")


if __name__ == "__main__":
    main()
