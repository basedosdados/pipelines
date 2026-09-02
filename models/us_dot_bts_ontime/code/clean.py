"""One-shot bootstrap clean of the full BTS on-time series.

Reads the monthly archives fetched by ``download.py`` and writes all-STRING
Snappy Parquet, hive-partitioned by year, one file per month. Resumable: a month
whose parquet already exists is skipped.

Also records, per column and per year, how many values are non-null. The delay
attribution columns do not exist before June 2003 and the diversion columns
before 2008, so a naive reading of the table would report those years as "no
delay cause" rather than "not collected". The measured coverage is written to
``coverage.json`` and used to fill ``temporal_coverage`` in the architecture.

    uv run --no-project --with pyarrow --with pandas --with requests \
        python models/us_dot_bts_ontime/code/clean.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_dot_bts_ontime.utils import (
    build_airport,
    build_dicionario,
    clean_month,
    month_iter,
    open_month_zip,
    read_arch,
    write_month_parquet,
    write_reference_parquet,
)

DATA = Path(
    os.environ.get(
        "BTS_DATA_DIR", Path.home() / "Downloads" / "us_dot_bts_ontime_data"
    )
)
RAW = DATA / "input" / "monthly"
OUT = DATA / "output"
END = (
    int(os.environ.get("BTS_END_YEAR", 2026)),
    int(os.environ.get("BTS_END_MONTH", 6)),
)
# Each worker holds one month of 114 string columns in arrow; three is the point
# where wall-clock stops improving on this box without pushing into swap.
WORKERS = int(os.environ.get("BTS_CLEAN_WORKERS", 3))


def one(ym: tuple[int, int]) -> tuple[tuple[int, int], str, int, dict]:
    year, month = ym
    src = RAW / f"ontime_{year}_{month:02d}.zip"
    dest = OUT / "flight" / f"year={year}" / f"data_{year}_{month:02d}.parquet"
    if not src.exists():
        return ym, "NOSRC", 0, {}
    if dest.exists() and dest.stat().st_size > 1000:
        return ym, "skip", 0, {}
    try:
        tbl = clean_month(open_month_zip(src))
        write_month_parquet(tbl, OUT, year, month)
        nn = {
            name: tbl.num_rows - tbl.column(name).null_count
            for name in tbl.column_names
        }
        return ym, "ok", tbl.num_rows, nn
    except Exception as exc:
        return ym, f"FAIL {type(exc).__name__}: {exc}", 0, {}


def main() -> None:
    months = month_iter(end=END)
    have = [
        m for m in months if (RAW / f"ontime_{m[0]}_{m[1]:02d}.zip").exists()
    ]
    print(
        f"{len(have)}/{len(months)} months downloaded; cleaning with {WORKERS} workers",
        flush=True,
    )

    cov_path = OUT / "coverage.json"
    coverage: dict[str, dict[str, int]] = {}
    if cov_path.exists():
        coverage = json.loads(cov_path.read_text())

    rows = 0
    failed = []
    done = 0
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(one, m): m for m in have}
        for fut in as_completed(futs):
            ym, status, n, nn = fut.result()
            done += 1
            rows += n
            if status.startswith("FAIL") or status == "NOSRC":
                failed.append((ym, status))
            if nn:
                year = str(ym[0])
                acc = coverage.setdefault(year, {})
                for k, v in nn.items():
                    acc[k] = acc.get(k, 0) + v
            if (
                status.startswith("FAIL")
                or done % 20 == 0
                or done == len(have)
            ):
                print(
                    f"[{done}/{len(have)} {(time.time() - t0) / 60:.1f}m] "
                    f"{ym[0]}-{ym[1]:02d} {status} (rows so far {rows:,})",
                    flush=True,
                )
                cov_path.parent.mkdir(parents=True, exist_ok=True)
                cov_path.write_text(json.dumps(coverage, indent=1))

    cov_path.write_text(json.dumps(coverage, indent=1))

    print("building reference tables ...", flush=True)
    lk = DATA / "input" / "lookups"
    ap = build_airport(lk)
    dic = build_dicionario(lk)
    write_reference_parquet(ap, "airport", OUT)
    write_reference_parquet(dic, "dicionario", OUT)
    print(
        f"airport: {len(ap):,} rows | dicionario: {len(dic):,} rows",
        flush=True,
    )

    files = list((OUT / "flight").rglob("*.parquet"))
    print(
        f"DONE: flight {len(files)} files, {rows:,} new rows, "
        f"{sum(f.stat().st_size for f in files) / 1e9:.1f} GB",
        flush=True,
    )
    if failed:
        print("FAILED:", failed[:20], flush=True)
        sys.exit(1)


def report_coverage() -> None:
    """Print the first and last year each column carries data, from coverage.json."""
    cov = json.loads((OUT / "coverage.json").read_text())
    years = sorted(int(y) for y in cov)
    print(f"{'column':<40} {'first':>6} {'last':>6}  {'years with data':>15}")
    for a in read_arch("flight"):
        c = a["name"]
        present = [y for y in years if cov[str(y)].get(c, 0) > 0]
        if present:
            print(
                f"{c:<40} {present[0]:>6} {present[-1]:>6}  {len(present):>15}"
            )
        else:
            print(f"{c:<40} {'-':>6} {'-':>6}  {0:>15}")


if __name__ == "__main__":
    if "--report" in sys.argv:
        report_coverage()
    else:
        main()
