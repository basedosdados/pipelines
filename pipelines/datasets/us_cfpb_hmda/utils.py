"""Pure functions for us_cfpb_hmda - no Prefect imports.

Download + clean the modern HMDA LAR (2018+) into all-STRING, year-partitioned
parquet. Schema, column order, the raw->clean name map, and the x1000 rule come
from the architecture TSV (constants.ARCHITECTURE_TSV) - the single source of
truth shared with the one-shot bootstrap in models/us_cfpb_hmda/code.

Cleaning uses polars' streaming engine (scan_csv -> select -> sink_parquet), which
is memory-bounded (a 4-5 GB CSV cleans well under the worker's 8 GB) and, unlike
duckdb, is already in the deployed worker image. Output is all-STRING (the staging
convention: upload_to_gcs infers a STRING header, and the dbt model safe_casts
each column back to its architecture type); numeric coercion drops the source
sentinels (NA/Exempt/blank) to NULL, and NULL stays NULL through the string cast.
The `year` column is kept in-file. Years are processed one at a time and the raw
CSV is deleted right after its clean, so peak disk stays near a single raw file.
"""

import csv
import subprocess
from pathlib import Path

import polars as pl

from pipelines.datasets.us_cfpb_hmda.constants import constants

TABLE_ID = constants.TABLE_ID.value
FIRST_YEAR = constants.FIRST_YEAR.value
MODERN_URL = constants.MODERN_URL.value
MULTIPLY_1000 = set(constants.MULTIPLY_1000.value)
PL_NUM = {"INT64": pl.Int64, "FLOAT64": pl.Float64}


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


def _header(csv_path: Path) -> set[str]:
    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        return set(next(csv.reader(fh)))


def latest_source_year(this_year: int) -> int:
    """Highest modern year CFPB has published, probed from this_year down.

    The nationwide endpoint 301-redirects to a real CSV for a published year and
    returns a small/error body otherwise; we accept a year only if the streamed
    head starts with the expected `activity_year,lei,...` header.

    Args:
        this_year: Calendar year to start probing down from (the flow passes it,
            keeping this function free of clock calls).

    Returns:
        The latest published modern year (>= FIRST_YEAR).

    Raises:
        RuntimeError: If no published year is found down to FIRST_YEAR.
    """
    import requests

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


def _expr(col: dict, present: set[str]) -> pl.Expr:
    """Polars expression for one column -> all-STRING output aliased to `name`.

    STRING -> trimmed, blank -> NULL, raw codes kept. INT64/FLOAT64 -> non-strict
    numeric cast (source sentinels NA/Exempt/blank -> NULL); x1000 columns scaled
    to real USD. The final cast to Utf8 keeps NULL as NULL (never "nan"), which
    the dbt model then safe_casts back to the architecture type. Columns absent
    from a given year's header become typed NULLs.
    """
    name, typ = col["name"], col["bigquery_type"]
    if col["original_name"] not in present:
        return pl.lit(None, dtype=pl.Utf8).alias(name)
    s = pl.col(col["original_name"]).str.strip_chars()
    if typ == "STRING":
        return (
            pl.when(s.str.len_chars() == 0)
            .then(None)
            .otherwise(s)
            .cast(pl.Utf8)
            .alias(name)
        )
    num = s.cast(PL_NUM[typ], strict=False)
    if name in MULTIPLY_1000:
        num = num * 1000
    return num.cast(pl.Utf8).alias(name)


def clean_year(year: int, csv_path: Path, output_dir: Path) -> Path:
    """Clean one year's CSV into all-STRING parquet at
    output_dir/loan_application_register/year=<year>/data.parquet (year in-file)."""
    cols = read_arch()  # includes `year`
    present = _header(csv_path)
    lf = pl.scan_csv(
        csv_path,
        separator=",",
        has_header=True,
        infer_schema_length=0,  # read every column as Utf8
        quote_char='"',
        truncate_ragged_lines=True,
    ).select([_expr(c, present) for c in cols])

    out_dir = output_dir / TABLE_ID / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"
    lf.sink_parquet(out, compression="snappy", row_group_size=100_000)
    return out


def clean_all(output_dir: Path, years: list[int], input_dir: Path) -> dict:
    """Download+clean each modern year one at a time (deleting each raw CSV after).

    Returns {"loan_application_register": <partition dir>, "max_year": "<YYYY>"}.
    """
    for y in years:
        csv_path = download_year(y, input_dir)
        clean_year(y, csv_path, output_dir)
        csv_path.unlink(missing_ok=True)
    return {TABLE_ID: str(output_dir / TABLE_ID), "max_year": str(max(years))}
