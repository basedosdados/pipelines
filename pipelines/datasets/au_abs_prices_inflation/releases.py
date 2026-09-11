"""Cleaning transforms for the non-CPI ABS price releases.

One fact table per ABS release, each long on the ABS Series ID, with the
semicolon-delimited Data Item Description decomposed into named dimension
columns. Decomposing is safe because each release's description layout is
fixed -- but only the Wage Price Index needs shape-based classification, and
getting that wrong mislabels rows without raising.

No Prefect imports here. The one-shot onboarding bootstrap
(``models/au_abs_prices_inflation/code/clean_data.py``) and the recurring
Prefect pipeline both import these functions, so the transform lives in
exactly one place.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.au_abs_prices_inflation.constants import constants
from pipelines.datasets.au_abs_prices_inflation.timeseries import (
    financial_year_of,
    parse_ts_workbook,
    quarter_of,
    split_description,
)

COLUMNS = constants.RELEASE_COLUMNS.value
STATISTIC = constants.STATISTIC.value
ITPI_CLASSIFICATION = constants.ITPI_CLASSIFICATION.value

_INT_COLS = {"year", "quarter"}
_FLOAT_COLS = {"value"}

_CODE_RE = re.compile(r"^(\d+)\s+(.+)$")
_PAY_STRIP_RE = re.compile(r"\b(time|hourly)\b")

_WPI_REGIONS = {r.lower(): r for r in constants.WPI_REGIONS.value}
_WPI_SECTORS = {s.lower(): s for s in constants.WPI_SECTORS.value}
_WPI_PAY_MEASURE = constants.WPI_PAY_MEASURE.value


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def _norm(text: str) -> str:
    # pyrefly: ignore [unnecessary-type-conversion]
    return re.sub(r"\s+", " ", str(text)).strip().lower()


def statistic_of(label: str) -> str:
    """Canonical statistic label for an ABS description's first part.

    ABS spells the same statistic differently between a release's quarterly and
    financial-year workbooks and varies the case, so matching the raw label
    would split one statistic into several. Unknown labels raise rather than
    fall through to a default: a silently mislabelled statistic is invisible in
    the output, where a failed run is not.
    """
    n = _norm(label)
    if "points contribution" in n:
        return STATISTIC[
            "points_change" if n.startswith("change") else "points"
        ]
    if n.startswith("percentage change"):
        year = (
            "corresponding" in n
            or "previous year" in n
            or n.endswith("annual")
        )
        return STATISTIC["year" if year else "period"]
    if "index" in n:
        return STATISTIC["index"]
    raise ValueError(f"unrecognised ABS statistic label: {label!r}")


def split_item_code(item: str) -> tuple[str | None, str]:
    """Split ``"2419 Other professional ... manufacturing"`` into code and name.

    Classification codes (ANZSIC, SITC, HTISC, AHECC) prefix the label in most
    ABS price tables but not all, so a label with no leading code keeps a null
    code rather than being forced into one.
    """
    m = _CODE_RE.match(item)
    return (m.group(1), m.group(2)) if m else (None, item)


def _wpi_dimensions(parts: list[str]) -> dict:
    """Classify Wage Price Index description parts by shape, never by position.

    ABS reorders these parts between workbooks of the same release: position 1
    carries the pay measure in the quarterly tables but a region in the
    state tables, and position 3 carries a sector or a pay measure. Reading
    them positionally mislabels every row of the reordered files and does not
    raise, so each part is matched against its own vocabulary instead.
    """
    dims = {
        "pay_measure": None,
        "region": None,
        "sector": None,
        "industry": None,
    }
    for part in parts:
        norm = _norm(part)
        if "rates of pay" in norm:
            key = re.sub(r"\s+", " ", _PAY_STRIP_RE.sub("", norm)).strip()
            if key not in _WPI_PAY_MEASURE:
                raise ValueError(f"unrecognised WPI pay measure: {part!r}")
            dims["pay_measure"] = _WPI_PAY_MEASURE[key]
        elif norm in _WPI_REGIONS:
            dims["region"] = _WPI_REGIONS[norm]
        elif norm in _WPI_SECTORS:
            dims["sector"] = _WPI_SECTORS[norm]
        else:
            dims["industry"] = part
    return dims


def _index_type_of(table: str | None, patterns: dict[str, str]) -> str | None:
    """Read a dimension out of the ABS table title.

    The Producer and International Trade releases put the direction of the
    index -- Output vs Input, Import vs Export -- only in the table title. The
    description omits it, so "All groups" collides 21 ways in Producer and 6
    ways in International Trade without this.
    """
    for needle, value in patterns.items():
        if table and needle.lower() in table.lower():
            return value
    return None


# --------------------------------------------------------------------------- #
# Per-release row builders
# --------------------------------------------------------------------------- #
def _row_wage_price_index(meta: dict, parts: list[str]) -> dict:
    """Wage Price Index is the one release whose Series Type varies.

    Its Table 1 publishes nine descriptions three times over -- Original,
    Seasonally Adjusted and Trend -- with the type recorded only in the
    workbook's metadata block, never in the description. Without the column
    those 27 series stack indistinguishably, and the ABS headline ("the WPI
    rose 0.8% in the June quarter 2026") is a seasonally adjusted figure.
    """
    return {
        "statistic": statistic_of(parts[0]),
        "series_type": meta["series_type"],
        **_wpi_dimensions(parts[1:]),
    }


def _row_producer_price_index(meta: dict, parts: list[str]) -> dict:
    code, name = split_item_code(parts[1]) if len(parts) > 1 else (None, None)
    return {
        "statistic": statistic_of(parts[0]),
        "index_type": _index_type_of(
            meta["source_table"],
            {
                "Final demand": "Final demand",
                "Input": "Input",
                "Output": "Output",
            },
        ),
        "item_code": code,
        "item_name": name,
        "region": parts[2] if len(parts) > 2 else None,
        "source_table": meta["source_table"],
    }


def _row_international_trade_price_index(meta: dict, parts: list[str]) -> dict:
    code, name = split_item_code(parts[1]) if len(parts) > 1 else (None, None)
    return {
        "statistic": statistic_of(parts[0]),
        "index_type": _index_type_of(
            meta["source_table"],
            {"Import Price Index": "Import", "Export Price Index": "Export"},
        ),
        "classification": _index_type_of(
            meta["source_table"], ITPI_CLASSIFICATION
        ),
        "item_code": code,
        "item_name": name,
        "source_table": meta["source_table"],
    }


def _row_living_cost_index(meta: dict, parts: list[str]) -> dict:
    return {
        "statistic": statistic_of(parts[0]),
        "household_type": parts[1],
        "commodity_group": parts[2],
    }


def _row_dwelling_value(meta: dict, parts: list[str]) -> dict:
    """Total Value of Dwellings keeps the raw measure -- it is not a statistic.

    Its measures are quantities in their own right (a count, a median price, a
    stock value) carried in four different units, so they stay as a `measure`
    column rather than being folded into the shared statistic vocabulary. The
    stock measures append the owning sector after a bare semicolon, which is
    split out here.
    """
    measure, _, owner = parts[0].partition("; Owned by ")
    return {
        "measure": measure.strip(),
        "owner_sector": owner.strip() or None,
        "region": parts[1],
    }


_ROW_BUILDER = {
    "wage_price_index": _row_wage_price_index,
    "producer_price_index": _row_producer_price_index,
    "international_trade_price_index": _row_international_trade_price_index,
    "living_cost_index": _row_living_cost_index,
    "dwelling_value": _row_dwelling_value,
}


# --------------------------------------------------------------------------- #
# Build one release
# --------------------------------------------------------------------------- #
def build_release(release: str, input_dir: str) -> pd.DataFrame:
    """Build one release's fact table from its downloaded workbooks."""
    paths = sorted(Path(input_dir, release).glob("*.xlsx"))
    if not paths:
        raise FileNotFoundError(
            f"no workbooks for {release} under {input_dir}"
        )

    # A Wage Price Index or Living Cost series appears in several workbooks
    # (Table 1 repeats series from Tables 2b-9b). Descriptions were verified
    # identical across those copies, so the first wins.
    series: dict[str, dict] = {}
    observations: list[dict] = []
    for path in paths:
        series_rows, obs_rows = parse_ts_workbook(str(path))
        for row in series_rows:
            series.setdefault(row["series_id"], row)
        observations.extend(obs_rows)

    # Every release except the Wage Price Index publishes Original series
    # only, so they carry no `series_type` column. If ABS ever adds a
    # seasonally adjusted or trend series to one of them, it would otherwise
    # stack silently against its Original twin -- so fail loudly instead.
    if "series_type" not in COLUMNS[release]:
        extra = {
            row["series_type"]
            for row in series.values()
            if row["series_type"] not in (None, "Original")
        }
        if extra:
            raise ValueError(
                f"{release} gained non-Original series types {sorted(extra)}; "
                "add a series_type column to its architecture and dbt model"
            )

    builder = _ROW_BUILDER[release]
    dims = {}
    for series_id, meta in series.items():
        parts = split_description(meta["description"])
        dims[series_id] = {
            **builder(meta, parts),
            "frequency": meta["frequency"],
            "unit": meta["unit"],
        }

    seen: set[tuple[str, object]] = set()
    records = []
    for obs in observations:
        key = (obs["series_id"], obs["date"])
        if key in seen:
            continue
        seen.add(key)
        date = obs["date"]
        annual = dims[obs["series_id"]]["frequency"] == "Annual"
        records.append(
            {
                "year": date.year,
                # An annual series is dated at the June ending its financial
                # year, so deriving a quarter from that month would label it
                # the June quarter -- a period it does not describe.
                "quarter": None if annual else quarter_of(date),
                "financial_year": financial_year_of(date) if annual else None,
                "series_id": obs["series_id"],
                "value": obs["value"],
                **dims[obs["series_id"]],
            }
        )

    df = pd.DataFrame.from_records(records)
    cols = COLUMNS[release]
    for col in cols:
        if col not in df.columns:
            df[col] = None
    return (
        df[cols]
        .sort_values(["series_id", "year", "quarter"], na_position="first")
        .reset_index(drop=True)
    )


def clean_all(
    input_dir: str, output_dir: str, releases=None
) -> dict[str, str]:
    """Build every release into partitioned parquet under ``output_dir``.

    Returns a mapping of output table slug to its partition root, plus
    ``"<release>__max_year_month"`` for each release -- the latest period it
    contains, as ``"YYYY-MM"``, which drives that release's source-update poll.

    That string is a year-*month*, not a year-quarter, because the poll
    compares it against the registered coverage, and the coverage for a
    ``YearQuarter`` column is stored as ``MAX(DATE(year, quarter * 3, 1))``
    formatted ``"%Y-%m"``. Polling at a coarser granularity than the coverage
    is stored at compares mismatched clocks, and the guard then either never
    fires or fires on every run.
    """
    result: dict[str, str] = {}
    for release in releases or COLUMNS:
        df = build_release(release, input_dir)
        write_partitioned(df, release, output_dir)
        result[release] = str(Path(output_dir) / release)
        # Ignore rows with no quarter (the Wage Price Index's financial-year
        # series): BigQuery computes the coverage as
        # MAX(DATE(year, quarter * 3, 1)), where a NULL quarter yields NULL,
        # so those rows do not move the coverage either.
        periods = df.loc[df["quarter"].notna(), ["year", "quarter"]]
        year, quarter = periods.sort_values(["year", "quarter"]).iloc[-1]
        result[f"{release}__max_year_month"] = (
            f"{int(year):04d}-{int(quarter) * 3:02d}"
        )
    return result


# --------------------------------------------------------------------------- #
# Write partitioned parquet (all-STRING, hive-partitioned by year)
# --------------------------------------------------------------------------- #
def _arrow_type(column: str) -> pa.DataType:
    if column in _INT_COLS:
        return pa.int64()
    if column in _FLOAT_COLS:
        return pa.float64()
    return pa.string()


def write_partitioned(df: pd.DataFrame, table: str, out_dir: str) -> int:
    """Write a table as all-STRING Snappy parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention -- the dbt model
    ``safe_cast``s every column, and the pipeline's ``upload_to_gcs`` infers
    the staging schema from a stringified header, which rejects typed parquet.
    Values therefore pass through their real arrow types first (so ``year``
    serialises as ``"1966"``, not ``"1966.0"``, and NULL is preserved) and are
    then cast to string via arrow -- never ``astype(str)``, which would render
    NULL as the literal ``"nan"`` and defeat ``safe_cast``.
    """
    cols = COLUMNS[table]
    typed_schema = pa.schema([(c, _arrow_type(c)) for c in cols])
    string_schema = pa.schema([(c, pa.string()) for c in cols])
    written = 0
    for year, part in df.groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        dest = Path(out_dir) / table / f"year={int(year)}"
        dest.mkdir(parents=True, exist_ok=True)
        arrow = pa.Table.from_pandas(
            part[cols], schema=typed_schema, preserve_index=False
        )
        pq.write_table(
            arrow.cast(string_schema),
            dest / "data.parquet",
            compression="snappy",
        )
        written += len(part)
    return written
