"""Clean the downloaded BDS CSVs into all-STRING partitioned parquet.

Every run rewrites the output of the files it cleans, so a change to the
transform can never leave stale output from the old code behind.

Usage:
    ~/.venvs/bd-pipelines/bin/python -m models.us_census_bds.code.clean [file ...]
"""

from __future__ import annotations

import argparse
import json
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.us_census_bds.constants import constants
from pipelines.datasets.us_census_bds.utils import (
    build_dictionary,
    clean_file,
)

INPUT = constants.DATA_DIR.value / "input"
OUTPUT = constants.DATA_DIR.value / "output"
# One parquet per source file and year; compacted into OUTPUT/<table>/.
PARTS = OUTPUT / "parts"
TABLES = ["country", "state", "county", "metropolitan_area"]
COVERAGE = "1978(1)2023"


def remove_outputs(stem: str) -> None:
    """Delete every partition file a source file wrote on an earlier run."""
    for old in PARTS.glob(f"*/year=*/{stem}-*.parquet"):
        old.unlink()


def compact(table: str) -> int:
    """Merge a table's per-file parts into one parquet per year.

    Uploading thousands of small files is slow, and BigQuery reads a few
    larger files faster; the parts stay so later runs can re-clean one file.
    """
    target = OUTPUT / table
    shutil.rmtree(target, ignore_errors=True)
    rows = 0
    for year_dir in sorted((PARTS / table).glob("year=*")):
        parts = sorted(year_dir.glob("*.parquet"))
        if not parts:
            continue
        merged = pa.concat_tables(pq.read_table(p) for p in parts)
        (target / year_dir.name).mkdir(parents=True)
        pq.write_table(
            merged,
            target / year_dir.name / "data.parquet",
            compression="snappy",
        )
        rows += merged.num_rows
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="*", help="file names; default all")
    args = parser.parse_args()
    paths = (
        sorted(INPUT / f for f in args.files)
        if args.files
        else sorted(INPUT.glob("*.csv"))
    )

    manifest_path = OUTPUT / "manifest.json"
    manifest = (
        json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    )
    for path in paths:
        remove_outputs(path.stem)
        table, rows = clean_file(path, PARTS)
        manifest[path.name] = {"table": table, "rows": rows}
        print(f"{path.name}: {rows:,} rows -> {table}", flush=True)
        manifest_path.write_text(
            json.dumps(manifest, indent=1, sort_keys=True)
        )

    # The dictionary describes every file present, so rebuild it each run.
    dictionary = build_dictionary(sorted(INPUT.glob("*.csv")), COVERAGE)
    target = OUTPUT / "dictionary"
    shutil.rmtree(target, ignore_errors=True)
    target.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pandas(dictionary, preserve_index=False),
        target / "data.parquet",
    )
    totals: dict[str, int] = {}
    for entry in manifest.values():
        totals[entry["table"]] = totals.get(entry["table"], 0) + entry["rows"]
    for table in TABLES:
        written = compact(table)
        if written != totals.get(table, 0):
            raise ValueError(
                f"{table}: compacted {written:,} rows, manifest says "
                f"{totals.get(table, 0):,}"
            )
    totals["dictionary"] = len(dictionary)
    print(json.dumps(totals, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
