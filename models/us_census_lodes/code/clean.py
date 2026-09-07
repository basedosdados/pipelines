"""One-shot bootstrap: download and clean all of us_census_lodes.

The transform itself lives in ``pipelines/datasets/us_census_lodes/utils.py`` and
is shared with the recurring Prefect pipeline; this script only drives it and
reports.

    uv run python models/us_census_lodes/code/clean.py                # everything
    uv run python models/us_census_lodes/code/clean.py --states vt,dc # a subset
    uv run python models/us_census_lodes/code/clean.py --years 2022,2023

Work proceeds one (state, year) at a time and each gzipped CSV is deleted after
it is read, so peak disk stays near a dozen files. Re-running is safe: an
already-written parquet is overwritten, and a partially downloaded input is
re-fetched.

Scratch data goes to ``~/Downloads/us_census_lodes_data`` (override with
``LODES_DATA_ROOT``) — never the repo, never Dropbox.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_census_lodes.constants import (
    INPUT,
    OUTPUT,
    STATES,
    YEARS,
)
from pipelines.datasets.us_census_lodes.utils import (
    clean_crosswalk,
    clean_state_year,
    load_block_geography,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--states", help="comma-separated postal codes, default all"
    )
    ap.add_argument("--years", help="comma-separated years, default 2002-2023")
    ap.add_argument(
        "--skip-crosswalk",
        action="store_true",
        help="do not rebuild the crosswalk",
    )
    ap.add_argument(
        "--keep-input",
        action="store_true",
        help="do not delete raw files after use",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=5,
        help="states processed concurrently; each fetches up to 6 files at once",
    )
    args = ap.parse_args()

    states = args.states.split(",") if args.states else STATES
    years = [int(y) for y in args.years.split(",")] if args.years else YEARS

    totals: dict[str, int] = {}
    gaps: list[str] = []
    started = time.time()
    lock = threading.Lock()
    done = 0

    def run_state(state: str) -> None:
        nonlocal done
        local: dict[str, int] = {}
        local_gaps: list[str] = []
        if not args.skip_crosswalk:
            local["geography_crosswalk"] = clean_crosswalk(
                state, INPUT, OUTPUT, keep_input=args.keep_input
            )
        # County and tract come from the state's crosswalk, not from slicing
        # the block code -- the two disagree (all of CT, some VT blocks).
        geo = load_block_geography(state, INPUT, OUTPUT)
        for year in years:
            got = clean_state_year(
                state,
                year,
                INPUT,
                OUTPUT,
                geo=geo,
                keep_input=args.keep_input,
            )
            for table, rows in got.items():
                local[table] = local.get(table, 0) + rows
            for table in ("residence_jobs", "workplace_jobs"):
                if table not in got:
                    local_gaps.append(f"{state.upper()} {year} {table}")
        with lock:
            done += 1
            for table, rows in local.items():
                totals[table] = totals.get(table, 0) + rows
            gaps.extend(local_gaps)
            print(
                f"[{done}/{len(states)}] {state.upper()} done "
                f"({(time.time() - started) / 60:.1f} min elapsed) "
                + ", ".join(f"{k}={v:,}" for k, v in sorted(totals.items())),
                flush=True,
            )

    # Any state that raises must surface: a swallowed failure would look
    # identical to a genuine coverage gap and silently drop a whole state.
    with cf.ThreadPoolExecutor(args.workers) as ex:
        for future in cf.as_completed(
            [ex.submit(run_state, s) for s in states]
        ):
            future.result()

    print("\n=== totals ===")
    for table, rows in sorted(totals.items()):
        print(f"{table}: {rows:,} rows")
    print(
        f"\n=== coverage gaps: {len(gaps)} state-year-table combinations ==="
    )
    for g in gaps:
        print(f"  {g}")


if __name__ == "__main__":
    main()
