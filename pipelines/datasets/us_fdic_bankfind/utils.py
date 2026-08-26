"""Pure download and cleaning helpers for us_fdic_bankfind.

No Prefect imports live here: `tasks.py` wraps these, and the one-shot
onboarding scripts under `models/us_fdic_bankfind/code/` import them directly so
the transform exists in exactly one place.

Two API limits shape the download:

* a request asking for more than 250 fields is capped at 500 rows, while one
  asking for 250 or fewer may ask for 10,000.  Financials has 2,378 fields, so
  each quarter is fetched as ten field batches of <=248 and stitched on CERT.
  Batching this way costs ~1,700 requests instead of ~34,000.
* `offset` is only needed for the early quarters: 17,930 institutions reported
  in 1984Q1 against 4,313 in 2026Q2.
"""

from __future__ import annotations

import io
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml

BASE_URL = "https://api.fdic.gov/banks"
DOCS_URL = f"{BASE_URL}/docs"
PAGE_SIZE = 10_000
FIELDS_PER_BATCH = 248  # +CERT +REPDTE stays within the 250-field limit
KEY_FIELDS = ["CERT", "REPDTE"]
TIMEOUT = 300
RETRIES = 4

# The FDIC serves these to ordinary clients, but a bare urllib user agent is
# rejected, so a browser-shaped one is sent on every request.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/json",
}


def _get(path: str, params: dict) -> requests.Response:
    """GET with retries; the API returns sporadic 5xx under sustained load."""
    last: Exception | None = None
    for attempt in range(RETRIES):
        try:
            response = requests.get(
                f"{BASE_URL}/{path}",
                params=params,
                headers=HEADERS,
                timeout=TIMEOUT,
            )
            if response.status_code == 200:
                return response
            last = RuntimeError(
                f"HTTP {response.status_code}: {response.text[:200]}"
            )
        except requests.RequestException as error:
            last = error
        time.sleep(2**attempt)
    raise RuntimeError(f"{path} failed after {RETRIES} attempts: {last}")


def _read_csv(response: requests.Response) -> pd.DataFrame:
    # every column is read as text: the API emits blanks for missing values and
    # pandas' inference would turn an all-blank column into float NaN
    return pd.read_csv(
        io.StringIO(response.text), dtype=str, keep_default_na=False
    )


def load_field_catalog(docs_dir: Path) -> dict[str, dict]:
    doc = yaml.safe_load((docs_dir / "risview_properties.yaml").read_text())
    return doc["properties"]["data"]["properties"]


def download_docs(docs_dir: Path) -> None:
    """Fetch the two published OpenAPI property files."""
    docs_dir.mkdir(parents=True, exist_ok=True)
    for name in ("risview_properties", "institution_properties"):
        target = docs_dir / f"{name}.yaml"
        if target.exists():
            continue
        response = requests.get(
            f"{DOCS_URL}/{name}.yaml", headers=HEADERS, timeout=TIMEOUT
        )
        response.raise_for_status()
        target.write_text(response.text)


def financial_field_batches(catalog: dict[str, dict]) -> list[list[str]]:
    """Split the reportable fields into batches within the 250-field limit."""
    fields = sorted(k for k in catalog if k not in KEY_FIELDS)
    return [
        fields[i : i + FIELDS_PER_BATCH]
        for i in range(0, len(fields), FIELDS_PER_BATCH)
    ]


def list_report_dates() -> list[str]:
    """Every quarter the FDIC has financial data for, oldest first."""
    response = _get(
        "financials",
        {
            "agg_by": "REPDTE",
            "agg_limit": 1000,
            "fields": "REPDTE",
            "format": "json",
        },
    )
    buckets = response.json().get("data", [])
    return sorted(bucket["data"]["REPDTE"] for bucket in buckets)


def _fetch_page(path: str, params: dict) -> pd.DataFrame:
    frames, offset = [], 0
    while True:
        page = dict(params, limit=PAGE_SIZE, offset=offset, format="csv")
        frame = _read_csv(_get(path, page))
        if frame.empty:
            break
        frames.append(frame)
        if len(frame) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_institutions() -> pd.DataFrame:
    """The institution master: one row per institution ever FDIC-registered."""
    return _fetch_page("institutions", {})


def fetch_quarter(report_date: str, batches: list[list[str]]) -> pd.DataFrame:
    """All reportable fields for one quarter, stitched across field batches."""
    merged: pd.DataFrame | None = None
    for batch in batches:
        params = {
            "filters": f"REPDTE:{report_date}",
            "fields": ",".join(KEY_FIELDS + batch),
        }
        frame = _fetch_page("financials", params)
        if frame.empty:
            continue
        frame = frame.drop(
            columns=[c for c in ("ID",) if c in frame], errors="ignore"
        )
        frame = frame.drop_duplicates(subset=["CERT"])
        if merged is None:
            merged = frame
        else:
            merged = merged.merge(
                frame.drop(columns=["REPDTE"], errors="ignore"),
                on="CERT",
                how="outer",
            )
    return merged if merged is not None else pd.DataFrame()


def to_string_table(frame: pd.DataFrame, columns: list[str]) -> pa.Table:
    """Build an all-STRING Arrow table with a stable column order.

    Staging is all-STRING by house convention and the dbt model safe_casts each
    column, so the schema carries order rather than types.  The cast goes through
    Arrow rather than `astype(str)`, which would render NULL as the literal
    "nan" -- a value `safe_cast` will not turn back into NULL.
    """
    ordered = frame.reindex(columns=columns)
    table = pa.Table.from_pandas(ordered, preserve_index=False)
    return table.cast(pa.schema([(name, pa.string()) for name in columns]))


def write_partition(
    frame: pd.DataFrame, columns: list[str], target: Path
) -> int:
    """Write one hive partition as Snappy parquet; returns the row count."""
    target.mkdir(parents=True, exist_ok=True)
    table = to_string_table(frame, columns)
    pq.write_table(table, target / "data.parquet", compression="snappy")
    return table.num_rows


# --------------------------------------------------------------------------
# cleaning
# --------------------------------------------------------------------------

THOUSANDS = 1000  # the FDIC reports dollar amounts in thousands
MELT_CHUNK = 200  # columns per melt pass, to bound peak memory
LONG_COLUMNS = [
    "year",
    "quarter",
    "report_date",
    "cert",
    "indicator_id",
    "value",
]
# Dates the FDIC uses to mean "not applicable" rather than a real date.
DATE_SENTINELS = {"9999-12-31", "12/31/9999", "99991231", "0", ""}


def to_float(values: pd.Series) -> pd.Series:
    """Parse a text column of numbers to float64, blanks and junk to NaN."""
    return pd.to_numeric(values.replace("", pd.NA), errors="coerce")


def scale_series(values: pd.Series, unit: str) -> pd.Series:
    """Parse a text column of numbers, applying the thousands conversion.

    Values stay float64: `to_string_table` lets Arrow render them at write time,
    which is far quicker than formatting each one in Python.  Arrow emits
    scientific notation for large magnitudes ("3.788551e+12") and BigQuery's
    safe_cast parses that back to the identical float64.
    """
    numbers = to_float(values)
    return numbers * THOUSANDS if unit == "USD" else numbers


def parse_dates(values: pd.Series) -> pd.Series:
    """Parse the FDIC's several date spellings into ISO text, sentinels to NULL."""
    cleaned = values.astype(str).str.strip()
    # pyrefly: ignore [bad-argument-type]
    cleaned = cleaned.where(~cleaned.isin(DATE_SENTINELS), pd.NA)
    parsed = pd.to_datetime(cleaned, format="%m/%d/%Y", errors="coerce")
    fallback = pd.to_datetime(cleaned, format="%Y%m%d", errors="coerce")
    parsed = parsed.fillna(fallback)
    fallback = pd.to_datetime(cleaned, format="%Y-%m-%d", errors="coerce")
    parsed = parsed.fillna(fallback)
    # a date past the far future is a sentinel the FDIC spells inconsistently
    # pyrefly: ignore [bad-argument-type]
    parsed = parsed.where(parsed.dt.year < 2200, pd.NA)
    return parsed.dt.strftime("%Y-%m-%d").fillna("")


def clean_institutions(
    frame: pd.DataFrame, spec, extraction_date: str
) -> pd.DataFrame:
    """Reshape the institution master onto the architecture's columns."""
    # assembled as a dict and built once: assigning 77 columns one by one
    # re-copies the block manager each time
    blank = pd.Series("", index=frame.index)
    columns: dict[str, pd.Series] = {
        "extraction_date": pd.Series(extraction_date, index=frame.index)
    }
    for code, name, btype, _description, opts in spec:
        column = frame[code] if code in frame.columns else blank
        if btype == "DATE":
            columns[name] = parse_dates(column)
        elif btype == "FLOAT64":
            columns[name] = scale_series(
                column, "USD" if opts.get("scaled") else ""
            )
        else:
            columns[name] = column.astype(str).str.strip()
    return pd.DataFrame(columns, index=frame.index)


def _quarter_keys(frame: pd.DataFrame, report_date: str) -> pd.DataFrame:
    stamp = pd.Timestamp(report_date)
    return pd.DataFrame(
        {
            "year": str(stamp.year),
            "quarter": str((stamp.month - 1) // 3 + 1),
            "report_date": stamp.strftime("%Y-%m-%d"),
            "cert": frame["CERT"].astype(str).str.strip(),
        },
        index=frame.index,
    )


def clean_financials(
    frame: pd.DataFrame,
    report_date: str,
    column_names: dict[str, str],
    catalog: dict,
) -> pd.DataFrame:
    """The wide headline table: one row per institution-quarter."""
    keys = _quarter_keys(frame, report_date)
    rssd = (
        frame["RSSDID"]
        if "RSSDID" in frame.columns
        else pd.Series("", index=frame.index)
    )
    # built in one pass; 290 individual assignments fragment the frame badly
    columns: dict[str, pd.Series] = {name: keys[name] for name in keys.columns}
    columns["rssd_id"] = rssd.astype(str).str.strip()
    for code, name in column_names.items():
        if code not in frame.columns:
            columns[name] = pd.Series(
                pd.NA, index=frame.index, dtype="Float64"
            )
            continue
        unit = (
            "USD" if catalog[code]["unit_of_measure"] == "USD_thousand" else ""
        )
        columns[name] = scale_series(frame[code], unit)
    return pd.DataFrame(columns, index=frame.index)


def clean_financials_indicator(
    frame: pd.DataFrame, report_date: str, catalog: dict
) -> pd.DataFrame:
    """The long archive: one row per institution-quarter-indicator reported.

    Rows where the institution did not report the line item are dropped, which
    is what keeps the table to the reported cells rather than the full
    cross-product.
    """
    keys = _quarter_keys(frame, report_date)
    # Only numeric line items belong here.  The financials response also repeats
    # 52 institution descriptors (ADDRESS, CITY, ...), which live in the
    # institution table and would otherwise break the float cast below.
    codes = [
        c
        for c in frame.columns
        if c in catalog
        and c not in KEY_FIELDS
        and catalog[c]["source_type"] == "number"
    ]

    # A pandas melt over a 2,378-column frame spends most of its time building
    # intermediates.  Casting to a numpy block and reshaping is far quicker.
    # The cast goes through a fixed-width unicode array, which costs 128 bytes a
    # cell, so it runs in column chunks: done in one pass an early quarter
    # (17,930 institutions x 2,326 fields) would need about 5 GB.
    certs = keys["cert"].to_numpy()
    pieces = []
    for start in range(0, len(codes), MELT_CHUNK):
        chunk = codes[start : start + MELT_CHUNK]
        text = frame[chunk].to_numpy(dtype="U32")
        text[text == ""] = "nan"
        numbers = text.astype(np.float64)
        del text

        numbers *= np.array(
            [
                THOUSANDS
                if catalog[code]["unit_of_measure"] == "USD_thousand"
                else 1
                for code in chunk
            ],
            dtype=np.float64,
        )
        row_index, column_index = np.nonzero(~np.isnan(numbers))
        if len(row_index) == 0:
            continue
        pieces.append(
            pd.DataFrame(
                {
                    "cert": certs[row_index],
                    "indicator_id": np.asarray(chunk, dtype=object)[
                        column_index
                    ],
                    "value": numbers[row_index, column_index],
                }
            )
        )
        del numbers

    if not pieces:
        return pd.DataFrame(columns=LONG_COLUMNS)
    long = pd.concat(pieces, ignore_index=True)
    long.insert(0, "year", keys["year"].iloc[0])
    long.insert(1, "quarter", keys["quarter"].iloc[0])
    long.insert(2, "report_date", keys["report_date"].iloc[0])
    return long
