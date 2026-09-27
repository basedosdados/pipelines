"""One-shot bootstrap: download IATI Tables, extract, clean to parquet.

    python clean.py                # download if missing, extract, clean everything
    python clean.py --skip-download
    python clean.py --tables activity transaction

The transform itself lives in ``pipelines/datasets/world_iati_activities/utils.py``
and is imported, never copied, so this bootstrap and the recurring Prefect flow
run identical code.

Scratch data goes under ``~/Downloads/world_iati_activities_data/`` — never in
the repo or in Dropbox. The CSV zip is 2.97 GB and expands to about 16 GB for the
18 tables we take; ``--prune-csv`` deletes each CSV once it has been converted so
peak disk stays near the largest single table instead.
"""

import argparse
import json
import time
import zipfile

from common import CSV_DIR, CSV_ZIP, DATASETS_MINIMAL, INPUT, OUTPUT, STATS

from pipelines.datasets.world_iati_activities.constants import constants
from pipelines.datasets.world_iati_activities.utils import (
    build_registry_dataset,
    clean_table,
    download,
    source_max_date,
    write_transaction_lookup,
)


def fetch() -> None:
    for url, dest in (
        (constants.CSV_ZIP_URL.value, CSV_ZIP),
        (constants.DATASETS_MINIMAL_URL.value, DATASETS_MINIMAL),
        (constants.STATS_URL.value, STATS),
    ):
        if dest.exists():
            print(f"have {dest.name} ({dest.stat().st_size:,} bytes)")
            continue
        print(f"downloading {url}")
        download(url, dest)
        print(f"  -> {dest} ({dest.stat().st_size:,} bytes)")


def extract(tables: list[str]) -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    wanted = {
        constants.SOURCE_TABLES.value[t]
        for t in tables
        if t != "registry_dataset"
    }
    with zipfile.ZipFile(CSV_ZIP) as zf:
        for member in wanted:
            dest = CSV_DIR / f"{member}.csv"
            if dest.exists():
                continue
            with zf.open(f"iati/{member}.csv") as src, dest.open("wb") as out:
                while block := src.read(1 << 22):
                    out.write(block)
            print(f"extracted {dest.name} ({dest.stat().st_size:,} bytes)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-download", action="store_true")
    ap.add_argument("--prune-csv", action="store_true")
    ap.add_argument("--tables", nargs="*", default=constants.ALL_TABLES.value)
    args = ap.parse_args()

    INPUT.mkdir(parents=True, exist_ok=True)
    if not args.skip_download:
        fetch()
    print(f"source snapshot: {source_max_date(STATS)}")
    extract(args.tables)

    OUTPUT.mkdir(parents=True, exist_ok=True)
    registry = build_registry_dataset(DATASETS_MINIMAL, OUTPUT)
    report, lookup = [], None
    for table in args.tables:
        if table == "registry_dataset":
            continue
        if table == "transaction_breakdown":
            lookup = write_transaction_lookup(
                OUTPUT, OUTPUT / "_transaction_lookup.parquet"
            )
        started = time.time()
        row = clean_table(table, CSV_DIR, OUTPUT, registry, txn_lookup=lookup)
        row["seconds"] = round(time.time() - started)
        report.append(row)
        print(
            f"{row['table']:28s} src={row['source_rows']:>12,} "
            f"kept={row['kept_rows']:>12,} "
            f"dropped={row['source_rows'] - row['kept_rows']:>7,} "
            f"{row['seconds']:>5}s",
            flush=True,
        )
        if args.prune_csv:
            (CSV_DIR / f"{constants.SOURCE_TABLES.value[table]}.csv").unlink(
                missing_ok=True
            )

    if lookup and lookup.exists():
        lookup.unlink()
    (OUTPUT / "clean_report.json").write_text(json.dumps(report, indent=1))
    print(f"\nTOTAL kept: {sum(r['kept_rows'] for r in report):,}")


if __name__ == "__main__":
    main()
