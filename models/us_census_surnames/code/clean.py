"""Download and clean the Census Bureau frequently occurring surnames lists.

Usage:
    python models/us_census_surnames/code/clean.py [--skip-download]

Writes ``$US_CENSUS_SURNAMES_DATA_DIR/{input,output}`` (default
``~/Library/Caches/us_census_surnames_data``). One table, ``surname``, stacks the
2000 and 2010 decennial lists, partitioned by census year.

Both lists hold every surname reported 100 or more times. The Census Bureau
suppresses a race/ethnicity percentage as ``(S)`` when it could identify
individuals; those cells become NULL. The 2010 list ends with an aggregate
``ALL OTHER NAMES`` row (rank 0) for surnames below the threshold, which is not a
surname and is dropped. Neither list will change, so there is no pipeline.
"""

import csv
import io
import json
import logging
import os
import shutil
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

ARCH = Path(__file__).resolve().parent / "architecture"
DATA_DIR = Path(
    os.environ.get(
        "US_CENSUS_SURNAMES_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_census_surnames_data",
    )
)

# year -> (archive URL, CSV member inside it)
SOURCES = {
    2000: (
        "https://www2.census.gov/topics/genealogy/2000surnames/names.zip",
        "app_c.csv",
    ),
    2010: (
        "https://www2.census.gov/topics/genealogy/2010surnames/names.zip",
        "Names_2010Census.csv",
    ),
}

# source column -> architecture column
RENAME = {
    "name": "surname",
    "rank": "rank",
    "count": "count",
    "prop100k": "proportion_per_100k",
    "cum_prop100k": "cumulative_proportion_per_100k",
    "pctwhite": "percent_white",
    "pctblack": "percent_black",
    "pctapi": "percent_asian_pacific_islander",
    "pctaian": "percent_american_indian_alaska_native",
    "pct2prace": "percent_two_or_more_races",
    "pcthispanic": "percent_hispanic",
}
AGGREGATE_ROW = "ALL OTHER NAMES"
SUPPRESSED = "(S)"


def download(input_dir: Path) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    for year, (url, _) in SOURCES.items():
        response = requests.get(url, timeout=1800)
        response.raise_for_status()
        if not response.content.startswith(b"PK"):
            raise ValueError(f"{url} did not return a zip")
        (input_dir / f"names_{year}.zip").write_bytes(response.content)
        log.info(f"downloaded {url} ({len(response.content):,} bytes)")


def read_year(input_dir: Path, year: int) -> pa.Table:
    _, member = SOURCES[year]
    with zipfile.ZipFile(input_dir / f"names_{year}.zip") as zf:
        raw = zf.read(member)
    header = next(csv.reader(io.StringIO(raw[:500].decode())))
    if header != list(RENAME):
        raise ValueError(f"{year}: unexpected header {header}")
    tbl = pacsv.read_csv(
        io.BytesIO(raw),
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in RENAME},
            null_values=[SUPPRESSED],
            strings_can_be_null=True,
        ),
    )
    tbl = tbl.rename_columns([RENAME[c] for c in tbl.column_names])
    is_agg = pc.equal(tbl["surname"], AGGREGATE_ROW)  # pyrefly: ignore [missing-attribute]
    n_agg = pc.sum(is_agg).as_py() or 0  # pyrefly: ignore [missing-attribute]
    if n_agg:
        log.info(f"{year}: dropping the {AGGREGATE_ROW!r} aggregate row")
        tbl = tbl.filter(pc.invert(is_agg))  # pyrefly: ignore [missing-attribute]
    return tbl.add_column(
        0, "year", pa.array([str(year)] * tbl.num_rows, pa.string())
    )


def validate(tbl: pa.Table, year: int) -> None:
    n = tbl.group_by(["surname"]).aggregate([]).num_rows
    if n != tbl.num_rows:
        raise ValueError(f"{year}: {tbl.num_rows - n} duplicate surnames")
    for col in ["surname", "rank", "count", "proportion_per_100k"]:
        if tbl[col].null_count:
            raise ValueError(f"{year}: {tbl[col].null_count} null {col}")
    counts = pc.cast(tbl["count"], pa.int64())  # pyrefly: ignore [missing-attribute]
    if pc.min(counts).as_py() < 100:  # pyrefly: ignore [missing-attribute]
        raise ValueError(f"{year}: a surname below the 100-count threshold")


def arch_order() -> list[str]:
    with (ARCH / "surname.csv").open(newline="", encoding="utf-8") as f:
        return [r["name"] for r in csv.DictReader(f)]


def main() -> None:
    input_dir, output_dir = DATA_DIR / "input", DATA_DIR / "output"
    if "--skip-download" not in sys.argv:
        download(input_dir)
    order = [c for c in arch_order() if c != "year"]
    tdir = output_dir / "surname"
    if tdir.exists():
        shutil.rmtree(tdir)
    rows = 0
    for year in SOURCES:
        tbl = read_year(input_dir, year)
        validate(tbl, year)
        missing = set(order) - set(tbl.column_names)
        if missing:
            raise ValueError(f"{year}: missing {missing}")
        pdir = tdir / f"year={year}"
        pdir.mkdir(parents=True)
        pq.write_table(
            tbl.select(order), pdir / "data.parquet", compression="snappy"
        )
        log.info(f"surname year={year}: {tbl.num_rows:,} rows")
        rows += tbl.num_rows
    (output_dir / "_manifest_surname.json").write_text(
        json.dumps({"rows": rows})
    )
    print(f"surname: {rows:,} rows")


if __name__ == "__main__":
    main()
