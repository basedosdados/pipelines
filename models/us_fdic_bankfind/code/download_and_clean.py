"""One-shot bootstrap: download the FDIC BankFind data and write clean parquet.

Reuses the transform in `pipelines.datasets.us_fdic_bankfind.utils`, which the
recurring Prefect pipeline also calls, so the two can never drift.

Quarters are independent, so they run in a process pool and each writes its own
partition.  The run is resumable: a quarter whose parquet already exists is
skipped, which matters because the full download takes hours and the API returns
the occasional 5xx.

    uv run python models/us_fdic_bankfind/code/download_and_clean.py --workers 5
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines.datasets.us_fdic_bankfind import utils
from pipelines.datasets.us_fdic_bankfind.institution_spec import (
    SPEC,
)

HERE = Path(__file__).resolve().parent
ARCH = HERE / "architecture"
DATA = Path(
    os.environ.get(
        "FDIC_DATA_DIR", Path.home() / "Downloads/us_fdic_bankfind_data"
    )
)
DOCS = DATA / "input/docs"
OUT = DATA / "output"


def columns_of(table: str) -> list[str]:
    with (ARCH / f"{table}.csv").open() as handle:
        return [row["name"] for row in csv.DictReader(handle)]


def catalog() -> dict:
    return json.loads((HERE / "indicator_catalog.json").read_text())


def partition_paths(report_date: str) -> tuple[Path, Path]:
    stamp = pd.Timestamp(report_date)
    quarter = (stamp.month - 1) // 3 + 1
    wide = OUT / f"financials/year={stamp.year}/data_q{quarter}.parquet"
    long = (
        OUT / f"financials_indicator/year={stamp.year}/data_q{quarter}.parquet"
    )
    return wide, long


def build_quarter(report_date: str) -> dict:
    """Download and write one quarter.  Runs in a worker process."""
    import pyarrow.parquet as pq

    wide_path, long_path = partition_paths(report_date)
    if wide_path.exists() and long_path.exists():
        return {"report_date": report_date, "skipped": True}

    reference = catalog()
    names = json.loads((HERE / "wide_column_names.json").read_text())
    batches = utils.financial_field_batches(utils.load_field_catalog(DOCS))

    raw = utils.fetch_quarter(report_date, batches)
    if raw.empty:
        return {"report_date": report_date, "institutions": 0, "long_rows": 0}

    iso = pd.Timestamp(report_date).strftime("%Y-%m-%d")
    wide = utils.clean_financials(raw, iso, names, reference)
    long = utils.clean_financials_indicator(raw, iso, reference)
    del raw

    for frame, path, table in (
        (wide, wide_path, "financials"),
        # sorting by indicator groups equal values together, which lets the
        # parquet dictionary encoding do its job: ~5.5 bytes a row
        (
            long.sort_values(["indicator_id", "cert"]),
            long_path,
            "financials_indicator",
        ),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        table_arrow = utils.to_string_table(frame, columns_of(table))
        # written to a temp name and renamed, so a run killed mid-write cannot
        # leave a truncated parquet that the resume logic would then skip
        staged = path.with_suffix(".parquet.tmp")
        pq.write_table(table_arrow, staged, compression="snappy")
        staged.replace(path)

    return {
        "report_date": report_date,
        "institutions": len(wide),
        "long_rows": len(long),
    }


def build_institutions() -> int:
    import pyarrow.parquet as pq

    raw = utils.fetch_institutions()
    extraction_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    frame = utils.clean_institutions(raw, SPEC, extraction_date)
    path = OUT / "institution/data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        utils.to_string_table(frame, columns_of("institution")),
        path,
        compression="snappy",
    )
    return len(frame)


def build_indicator() -> int:
    import pyarrow.parquet as pq

    reference = {
        code: record
        for code, record in catalog().items()
        if record["source_type"] == "number"
    }
    names = json.loads((HERE / "wide_column_names.json").read_text())
    frame = pd.DataFrame(
        [
            {
                "indicator_id": code,
                "name": record["name"],
                "description": record["description"],
                "measurement_unit": (
                    "USD"
                    if record["unit_of_measure"] == "USD_thousand"
                    else record["unit_of_measure"]
                ),
                "is_ratio": record["is_ratio"],
                "is_quarterly": record["is_quarterly"],
                "is_flag": record["is_flag"],
                "financials_column": names.get(code, ""),
            }
            for code, record in sorted(reference.items())
        ]
    )
    path = OUT / "indicator/data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        utils.to_string_table(frame, columns_of("indicator")),
        path,
        compression="snappy",
    )
    return len(frame)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--quarters", type=int, default=0, help="0 means all")
    parser.add_argument("--skip-static", action="store_true")
    args = parser.parse_args()

    utils.download_docs(DOCS)
    if not args.skip_static:
        print(
            f"institution           {build_institutions():>10,} rows",
            flush=True,
        )
        print(
            f"indicator             {build_indicator():>10,} rows", flush=True
        )

    quarters = utils.list_report_dates()
    if args.quarters:
        quarters = quarters[-args.quarters :]
    print(f"quarters to build: {len(quarters)}", flush=True)

    manifest, done = [], 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(build_quarter, q): q for q in quarters}
        for future in as_completed(futures):
            quarter = futures[future]
            try:
                result = future.result()
            except Exception as error:  # keep going; rerun picks up the gap
                print(f"  FAILED {quarter}: {error}", flush=True)
                continue
            done += 1
            manifest.append(result)
            if not result.get("skipped"):
                print(
                    f"  [{done}/{len(quarters)}] {quarter} "
                    f"{result['institutions']:>6,} institutions "
                    f"{result['long_rows']:>12,} long rows",
                    flush=True,
                )

    (DATA / "manifest.json").write_text(json.dumps(manifest, indent=1))
    built = [r for r in manifest if not r.get("skipped")]
    print(f"\nbuilt {len(built)} quarters")
    print(f"institution-quarters {sum(r['institutions'] for r in built):>14,}")
    print(f"long rows            {sum(r['long_rows'] for r in built):>14,}")


if __name__ == "__main__":
    main()
