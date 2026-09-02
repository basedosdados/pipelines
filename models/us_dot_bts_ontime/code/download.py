"""One-shot bootstrap download of the full BTS on-time series.

Resumable: a month whose .zip already exists is skipped, so the script can be
re-run after an interruption. The 1990s go through the slow form route and are
run at low concurrency; everything else comes from the static PREZIP archives.

    uv run --no-project --with requests python models/us_dot_bts_ontime/code/download.py
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_dot_bts_ontime.utils import (
    _session,
    download_lookups,
    download_month,
    month_iter,
    uses_prezip,
)

DATA = Path(
    os.environ.get(
        "BTS_DATA_DIR", Path.home() / "Downloads" / "us_dot_bts_ontime_data"
    )
)
RAW = DATA / "input" / "monthly"
END = (
    int(os.environ.get("BTS_END_YEAR", 2026)),
    int(os.environ.get("BTS_END_MONTH", 6)),
)


def fetch(ym: tuple[int, int]) -> tuple[tuple[int, int], str]:
    year, month = ym
    dest = RAW / f"ontime_{year}_{month:02d}.zip"
    if dest.exists() and dest.stat().st_size > 100_000:
        return ym, "skip"
    session = _session()
    for attempt in range(4):
        try:
            download_month(year, month, dest, session=session)
            return ym, f"ok {dest.stat().st_size / 1e6:.0f}MB"
        except Exception as exc:
            if attempt == 3:
                return ym, f"FAIL {type(exc).__name__}: {exc}"
            time.sleep(10 * (attempt + 1))
    return ym, "FAIL"


def run(
    months: list[tuple[int, int]], workers: int, label: str
) -> list[tuple[int, int]]:
    failed: list[tuple[int, int]] = []
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fetch, ym): ym for ym in months}
        for fut in as_completed(futs):
            ym, status = fut.result()
            done += 1
            if status.startswith("FAIL"):
                failed.append(ym)
            if (
                status.startswith("FAIL")
                or done % 10 == 0
                or done == len(months)
            ):
                print(
                    f"[{label} {done}/{len(months)} {(time.time() - t0) / 60:.1f}m] "
                    f"{ym[0]}-{ym[1]:02d} {status}",
                    flush=True,
                )
    return failed


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    print("lookups ...", flush=True)
    download_lookups(DATA / "input" / "lookups")

    months = month_iter(end=END)
    prezip = [m for m in months if uses_prezip(m[0])]
    form = [m for m in months if not uses_prezip(m[0])]
    print(
        f"{len(months)} months: {len(prezip)} prezip, {len(form)} form",
        flush=True,
    )

    failed = run(prezip, 4, "prezip")
    # The form builds each extract server-side; keep the pressure low.
    failed += run(form, 2, "form")

    if failed:
        print(f"RETRY {len(failed)} failed serially", flush=True)
        failed = run(failed, 1, "retry")

    have = sorted(RAW.glob("ontime_*.zip"))
    print(
        f"DONE: {len(have)}/{len(months)} months, "
        f"{sum(p.stat().st_size for p in have) / 1e9:.1f} GB",
        flush=True,
    )
    if failed:
        print("STILL FAILING:", failed, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
