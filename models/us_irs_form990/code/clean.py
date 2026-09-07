"""One-shot bootstrap: clean every IRS source file into partitioned parquet.

Thin CLI over the shared transform in
``pipelines/datasets/us_irs_form990/utils.py`` so the bootstrap and the
recurring pipeline never drift.

Scratch data lives under ``~/Downloads/us_irs_form990_data`` (never in the repo
or Dropbox); override with ``FORM990_DATA_DIR``. Run from the repo root with
``PYTHONPATH=.``::

    python models/us_irs_form990/code/clean.py efile      # all ZIPs present
    python models/us_irs_form990/code/clean.py bmf revocation dicionario

``efile`` processes ZIPs in parallel, skips batches whose result JSON already
exists, and writes ``output/efile_results/<batch>.json`` (counts, skipped form
types, concordance XPath hit tallies) for the validation step.
"""

import json
import os
import sys
import time
from email.utils import parsedate_to_datetime
from multiprocessing import Pool
from pathlib import Path

from pipelines.datasets.us_irs_form990 import utils

HERE = Path(__file__).resolve().parent
DATA = Path(
    os.environ.get(
        "FORM990_DATA_DIR", Path.home() / "Downloads/us_irs_form990_data"
    )
)
INPUT = DATA / "input"
OUTPUT = DATA / "output"
RESULTS = OUTPUT / "efile_results"


def _one_zip(zip_path: str) -> dict:
    t0 = time.time()
    try:
        res = utils.clean_efile_zip(Path(zip_path), OUTPUT)
    except Exception as exc:  # one bad ZIP must not kill the pool
        return {
            "batch": utils.batch_id(zip_path),
            "error": f"{type(exc).__name__}: {exc}",
            "return_financial": {},
            "compensation": {},
            "skipped": {},
            "unparseable": [],
            "seconds": round(time.time() - t0, 1),
        }
    res["seconds"] = round(time.time() - t0, 1)
    res["zip_bytes"] = Path(zip_path).stat().st_size
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / f"{res['batch']}.json").write_text(json.dumps(res))
    return res


def run_efile(workers: int) -> None:
    zips = sorted(
        (INPUT / "efile").glob("*.zip"), key=lambda p: p.stat().st_size
    )
    todo = [
        z for z in zips if not (RESULTS / f"{utils.batch_id(z)}.json").exists()
    ]
    print(f"{len(zips)} ZIPs present, {len(todo)} to process", flush=True)
    # Largest last so the pool tail is not one 3.7 GB straggler.
    with Pool(workers) as pool:
        for res in pool.imap_unordered(_one_zip, [str(z) for z in todo]):
            if "error" in res:
                print(
                    f"  {res['batch']:28s} FAILED {res['error']}", flush=True
                )
                continue
            n = sum(res["return_financial"].values())
            print(
                f"  {res['batch']:28s} {n:>8,} returns "
                f"{sum(res['compensation'].values()):>9,} people "
                f"skipped={res['skipped']} bad={len(res['unparseable'])} "
                f"{res['seconds']}s",
                flush=True,
            )


def run_bmf() -> None:
    stamp = (INPUT / "bmf" / "last_modified.txt").read_text()
    extraction_date = (
        parsedate_to_datetime(stamp.split(":", 1)[1].strip())
        .date()
        .isoformat()
    )
    print("BMF extraction_date:", extraction_date)
    print(utils.clean_bmf(INPUT / "bmf", OUTPUT, extraction_date))
    (OUTPUT / "bmf_extraction_date.txt").write_text(extraction_date)


def run_revocation() -> None:
    zip_path = INPUT / "revocation" / "data-download-revocation.zip"
    print(utils.clean_revocation(zip_path, OUTPUT))


def run_dicionario() -> None:
    n = utils.write_dicionario(HERE / "dicionario.csv", OUTPUT)
    print("dicionario rows:", n)


def write_headers() -> None:
    """0-row header guards for the one-shot onboarding upload (table-approve)."""
    tables = {
        "return_financial": [c for c in utils.RETURN_COLUMNS if c != "year"],
        "compensation": [c for c in utils.COMPENSATION_COLUMNS if c != "year"],
        "organization": [
            c for c in utils.ORGANIZATION_COLUMNS if c != "extraction_date"
        ],
        "revocation": utils.REVOCATION_COLUMNS,
        "dicionario": utils.DICIONARIO_COLUMNS,
    }
    for table, cols in tables.items():
        # Hive-partitioned tables: the header must sit inside a partition
        # directory (BigQuery rejects a bare file under the prefix), and in
        # the lexicographically first one so it is the first blob listed.
        parts = sorted(p for p in (OUTPUT / table).glob("*=*") if p.is_dir())
        target = parts[0] if parts else OUTPUT / table
        print(
            utils.write_header_parquet(
                target.parent if not parts else OUTPUT,
                table if not parts else f"{table}/{parts[0].name}",
                cols,
            )
        )


def main() -> None:
    steps = sys.argv[1:] or [
        "efile",
        "bmf",
        "revocation",
        "dicionario",
        "headers",
    ]
    workers = int(os.environ.get("FORM990_WORKERS", "6"))
    for step in steps:
        t0 = time.time()
        print(f"=== {step} ===", flush=True)
        {
            "efile": lambda: run_efile(workers),
            "bmf": run_bmf,
            "revocation": run_revocation,
            "dicionario": run_dicionario,
            "headers": write_headers,
        }[step]()
        print(f"=== {step} done in {time.time() - t0:,.0f}s ===", flush=True)


if __name__ == "__main__":
    main()
