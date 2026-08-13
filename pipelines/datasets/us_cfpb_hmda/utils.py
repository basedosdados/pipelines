"""Pure functions for us_cfpb_hmda - no Prefect imports.

Download + clean the modern HMDA LAR (2018+) into all-STRING, year-partitioned
parquet. Schema, column order, the raw->clean name map, and the x1000 rule come
from the architecture TSV (constants.ARCHITECTURE_TSV) - the single source of
truth shared with the one-shot bootstrap in models/us_cfpb_hmda/code. The typed
transform here mirrors models/us_cfpb_hmda/code/clean.py; the only difference is
this writes all-STRING parquet (staging convention - see write note below) while
keeping the `year` column in-file.

Memory: duckdb streams the COPY with preserve_insertion_order=false, so a 4-5 GB
CSV cleans at ~0.8 GB RSS. Years are processed one at a time and the raw CSV is
deleted right after its clean, so peak disk stays near a single raw file.
"""

import csv
import subprocess
from pathlib import Path

import duckdb
import requests

from pipelines.datasets.us_cfpb_hmda.constants import constants

DATASET_ID = constants.DATASET_ID.value
TABLE_ID = constants.TABLE_ID.value
FIRST_YEAR = constants.FIRST_YEAR.value
MODERN_URL = constants.MODERN_URL.value
MULTIPLY_1000 = set(constants.MULTIPLY_1000.value)
BQ_TO_DUCK = {"INT64": "BIGINT", "FLOAT64": "DOUBLE", "STRING": "VARCHAR"}


def read_arch() -> list[dict]:
    """Read the architecture TSV: ordered [{name, bigquery_type, original_name}]."""
    with open(constants.ARCHITECTURE_TSV.value, encoding="utf-8") as fh:
        return [
            {
                "name": r["name"].strip(),
                "bigquery_type": r["bigquery_type"].strip().upper(),
                "original_name": r["original_name"].strip(),
            }
            for r in csv.DictReader(fh, delimiter="\t")
        ]


def latest_source_year(this_year: int) -> int:
    """Highest modern year CFPB has published, probed from this_year down.

    The nationwide endpoint 301-redirects to a real CSV for a published year and
    returns a small/error body otherwise. We stream a few KB and accept the year
    only if the payload starts with the expected `activity_year,lei,...` header.

    Args:
        this_year: Calendar year to start probing down from (caller passes it so
            the flow controls "now"; keeps this function free of clock calls).

    Returns:
        The latest published modern year (>= FIRST_YEAR).

    Raises:
        RuntimeError: If no published year is found down to FIRST_YEAR.
    """
    for y in range(this_year, FIRST_YEAR - 1, -1):
        try:
            with requests.get(
                MODERN_URL.format(year=y), stream=True, timeout=120
            ) as r:
                if r.status_code != 200:
                    continue
                head = next(r.iter_content(chunk_size=2048), b"").decode(
                    "utf-8", "ignore"
                )
            if head.lstrip().startswith("activity_year,lei,"):
                return y
        except requests.RequestException:
            continue
    raise RuntimeError(
        f"no published HMDA modern year found down to {FIRST_YEAR}"
    )


def download_year(year: int, input_dir: Path) -> Path:
    """Download one modern year's nationwide CSV to input_dir/modern_<year>.csv."""
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / f"modern_{year}.csv"
    tmp = dest.with_suffix(".csv.part")
    cmd = [
        "curl",
        "-fL",
        "--retry",
        "4",
        "--retry-delay",
        "5",
        "--connect-timeout",
        "30",
        "-o",
        str(tmp),
        MODERN_URL.format(year=year),
    ]
    subprocess.run(cmd, check=True)
    tmp.rename(dest)
    return dest


def _expr(col: dict, present: set[str]) -> str:
    """Typed transform for one column, then cast to VARCHAR (all-STRING staging).

    STRING -> trimmed, blank -> NULL. INT64/FLOAT64 -> TRY_CAST (source
    sentinels NA/Exempt/blank -> NULL); x1000 columns scaled to real USD. The
    outer CAST(... AS VARCHAR) keeps NULL as NULL (never the string "nan"), which
    the dbt model then safe_casts back to the architecture type.
    """
    q = '"' + col["original_name"].replace('"', '""') + '"'
    name, typ = col["name"], col["bigquery_type"]
    if col["original_name"] not in present:
        return f"CAST(NULL AS VARCHAR) AS {name}"
    if typ == "STRING":
        inner = f"nullif(trim({q}), '')"
    else:
        inner = f"TRY_CAST(nullif(trim({q}), '') AS {BQ_TO_DUCK[typ]})"
        if name in MULTIPLY_1000:
            inner = f"({inner}) * 1000"
    return f"CAST({inner} AS VARCHAR) AS {name}"


def clean_year(year: int, csv_path: Path, output_dir: Path) -> Path:
    """Clean one year's CSV into all-STRING parquet at
    output_dir/loan_application_register/year=<year>/data.parquet (year in-file)."""
    cols = read_arch()  # includes `year`
    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        present = set(next(csv.reader(fh)))
    exprs = ",\n    ".join(_expr(c, present) for c in cols)
    out_dir = output_dir / TABLE_ID / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"
    tmp = output_dir / "duck_tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute("SET memory_limit='4GB'")
    con.execute(f"SET temp_directory='{tmp}'")
    read = (
        f"read_csv('{csv_path}', header=true, all_varchar=true, sample_size=-1, "
        f"quote='\"', escape='\"', null_padding=true, ignore_errors=false)"
    )
    con.execute(
        f"COPY (SELECT\n    {exprs}\n FROM {read}) "
        f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 100000)"
    )
    con.close()
    return out


def clean_all(output_dir: Path, years: list[int], input_dir: Path) -> dict:
    """Download+clean each modern year one at a time (deleting each raw CSV after).

    Args:
        output_dir: Root for partitioned output.
        years: Modern years to process (2018..latest).
        input_dir: Where raw CSVs land (deleted per year after cleaning).

    Returns:
        {"loan_application_register": <partition dir>, "max_year": "<YYYY>"}.
    """
    for y in years:
        csv_path = download_year(y, input_dir)
        clean_year(y, csv_path, output_dir)
        csv_path.unlink(missing_ok=True)
    return {TABLE_ID: str(output_dir / TABLE_ID), "max_year": str(max(years))}
