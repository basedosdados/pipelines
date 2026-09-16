"""Cleaning transform for us_eia_consumption (Form EIA-861 and EIA-861M).

Pure functions, no Prefect imports — shared by the one-shot bootstrap
(``models/us_eia_consumption/code/``) and the recurring pipeline. See
``models/us_eia_consumption/CLAUDE.md`` for the design.

EIA-861's annual files come in three layout eras and the workbooks are renamed on
every release, so the reader resolves a year's ZIP by trying a few URL templates,
finds the right workbook inside by pattern (never a literal name), and detects
the header row rather than assuming a fixed position. The wide sector blocks are
melted to long, and the TOTAL sector is dropped because it is the sum of the four
end-use sectors.
"""

import csv
import io
import re
import shutil
import zipfile
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import openpyxl  # pyrefly: ignore [untyped-import]
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import xlrd  # pyrefly: ignore [untyped-import]

from pipelines.datasets.us_eia_consumption.constants import constants

_WS = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^0-9a-z]+")
_NA_RE = re.compile(r"^(\.|-+|n/?a|null|nan)$", re.IGNORECASE)


def _norm(value: object) -> str:
    """Normalise a header cell: lowercase, non-alphanumerics to single underscores."""
    text = _NON_ALNUM.sub("_", str(value).lower()).strip("_")
    return text


# --------------------------------------------------------------------------
# Architecture
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Col:
    name: str
    bq_type: str
    covered_by_dictionary: bool = False
    directory_column: str = ""
    measurement_unit: str = ""
    description_pt: str = ""
    description_en: str = ""
    description_es: str = ""
    observations_pt: str = ""
    observations_en: str = ""
    observations_es: str = ""
    has_sensitive_data: bool = False


def load_cols(table: str) -> list[Col]:
    path = Path(constants.ARCHITECTURE_DIR.value) / f"sheet_{table}.csv"
    cols = []
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            cols.append(
                Col(
                    name=r["name"].strip(),
                    bq_type=r["bigquery_type"].strip().upper(),
                    covered_by_dictionary=r["covered_by_dictionary"].strip()
                    == "yes",
                    directory_column=r["directory_column"].strip(),
                    measurement_unit=r["measurement_unit"].strip(),
                    description_pt=r["description_pt"].strip(),
                    description_en=r["description_en"].strip(),
                    description_es=r["description_es"].strip(),
                    observations_pt=r["observations_pt"].strip(),
                    observations_en=r["observations_en"].strip(),
                    observations_es=r["observations_es"].strip(),
                    has_sensitive_data=r["has_sensitive_data"].strip()
                    == "yes",
                )
            )
    return cols


# --------------------------------------------------------------------------
# Cleaning helpers (shared shape with us_eia_seds / us_eia_electricity)
# --------------------------------------------------------------------------


def _get(frame: pd.DataFrame, name: str) -> pd.Series:
    """Column ``name`` if present, else an all-null Series aligned to ``frame``.

    The three EIA-861 eras carry different id columns — 2008 sales files omit the
    data_type column, pre-2012 files omit ba_code — so a builder must tolerate a
    canonical column simply not being there rather than crashing on ``None``.
    """
    if name in frame.columns:
        return frame[name]
    return pd.Series([None] * len(frame), index=frame.index, dtype="object")


def clean_text(series: pd.Series) -> pd.Series:
    def _text(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        return str(value)

    out = series.astype("object").map(_text)
    out = out.map(lambda v: None if v is None else _WS.sub(" ", v).strip())
    return out.map(lambda v: None if v is None or _NA_RE.match(v) else v)


def as_id(series: pd.Series) -> pd.Series:
    """Render a numeric identifier as a plain integer string ('3', not '3.0')."""
    text = clean_text(series)

    def one(value):
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return value
        if pd.isna(number):
            return None
        return str(int(number)) if number == int(number) else value

    return text.map(one)


def _stringify(series: pd.Series, bq_type: str) -> list[str | None]:
    if bq_type == "INT64":
        values = pd.to_numeric(series, errors="coerce").astype("Int64")
        return [None if pd.isna(v) else str(v) for v in values]
    if bq_type == "FLOAT64":
        values = pd.to_numeric(series, errors="coerce")
        return [None if pd.isna(v) else repr(float(v)) for v in values]
    return [
        None
        if v is None
        or (isinstance(v, float) and pd.isna(v))
        or str(v).strip() == ""
        else str(v).strip()
        for v in series.astype("object")
    ]


def to_string_table(frame: pd.DataFrame, cols: list[Col]) -> pa.Table:
    arrays = []
    for spec in cols:
        if spec.name in frame.columns:
            arrays.append(
                pa.array(
                    _stringify(frame[spec.name], spec.bq_type),
                    type=pa.string(),
                )
            )
        else:
            arrays.append(pa.nulls(len(frame), type=pa.string()))
    return pa.Table.from_arrays(arrays, names=[c.name for c in cols])


def write_partition(
    frame: pd.DataFrame, cols: list[Col], out_dir: Path, year: int
) -> int:
    target = Path(out_dir) / f"year={year}"
    if target.exists():
        shutil.rmtree(target)
    if frame is None or frame.empty:
        return 0
    target.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        to_string_table(frame, cols),
        target / "data.parquet",
        compression="snappy",
    )
    return len(frame)


def assert_all_string(path: Path) -> None:
    for file in sorted(Path(path).rglob("*.parquet")):
        schema = pq.read_schema(file)
        bad = [
            name
            for name, kind in zip(schema.names, schema.types, strict=True)
            if not pa.types.is_string(kind)
        ]
        if bad:
            raise AssertionError(f"{file}: non-string columns {bad}")
        if pq.ParquetFile(file).metadata.num_rows == 0:
            raise AssertionError(f"{file}: zero rows — poisons staging schema")


# --------------------------------------------------------------------------
# Directory resolution (state and county), reused export of br_bd_diretorios_us
# --------------------------------------------------------------------------


def _norm_county(name: str) -> str:
    import unicodedata

    text = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    )
    text = _NON_ALNUM.sub(" ", text.lower()).strip()
    text = re.sub(
        r"\b(county|parish|borough|census area|municipality|city)\b", "", text
    )
    return _WS.sub(" ", text).strip()


@cache
def _directory() -> tuple[dict[str, str], dict[tuple[str, str], str]]:
    states: dict[str, str] = {}
    counties: dict[tuple[str, str], str] = {}
    with open(constants.COUNTY_DIRECTORY.value, encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            abbrev = row["abbreviation_state"].strip().upper()
            states[abbrev] = row["id_state"].strip()
            counties[(abbrev, _norm_county(row["name"]))] = row[
                "id_county"
            ].strip()
    return states, counties


def state_id_for(series: pd.Series) -> pd.Series:
    states, _ = _directory()
    return clean_text(series).map(
        lambda v: states.get(v.upper()) if isinstance(v, str) else None
    )


def county_id_for(state: pd.Series, county: pd.Series) -> pd.Series:
    _, counties = _directory()
    st = clean_text(state)
    cy = clean_text(county)
    return pd.Series(
        [
            counties.get((a.upper(), _norm_county(b)))
            if isinstance(a, str) and isinstance(b, str)
            else None
            for a, b in zip(st, cy, strict=True)
        ],
        index=state.index,
        dtype="object",
    )


# --------------------------------------------------------------------------
# Reading workbooks (xls via xlrd, xlsx via openpyxl) to plain row lists
# --------------------------------------------------------------------------


def _rows_from_xlsx(payload: bytes, sheet: str | None) -> list[list]:
    wb = openpyxl.load_workbook(
        io.BytesIO(payload), read_only=True, data_only=True
    )
    ws = _pick_sheet_xlsx(wb, sheet)
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    wb.close()
    return rows


def _pick_sheet_xlsx(wb, sheet: str | None):
    if sheet is None:
        return wb[wb.sheetnames[0]]
    for name in wb.sheetnames:
        if _norm(name) == _norm(sheet) or _norm(sheet) in _norm(name):
            return wb[name]
    return wb[wb.sheetnames[0]]


def _rows_from_xls(payload: bytes, sheet: str | None) -> list[list]:
    wb = xlrd.open_workbook(file_contents=payload)
    ws = None
    if sheet is not None:
        for s in wb.sheets():
            if _norm(s.name) == _norm(sheet) or _norm(sheet) in _norm(s.name):
                ws = s
                break
    if ws is None:
        ws = wb.sheet_by_index(0)
    return [
        [ws.cell_value(i, j) for j in range(ws.ncols)] for i in range(ws.nrows)
    ]


def _read_member_rows(
    payload: bytes, member: str, sheet: str | None
) -> list[list]:
    lower = member.lower()
    if lower.endswith(".xlsx"):
        return _rows_from_xlsx(payload, sheet)
    return _rows_from_xls(payload, sheet)


# --------------------------------------------------------------------------
# Source: ZIP resolution and member lookup
# --------------------------------------------------------------------------


def _session(session: requests.Session | None = None) -> requests.Session:
    if session is not None:
        return session
    s = requests.Session()
    s.headers.update({"User-Agent": constants.USER_AGENT.value})
    return s


def download_year(
    year: int, input_dir: Path, session: requests.Session | None = None
) -> Path | None:
    """Download a report year's EIA-861 ZIP, trying the URL templates in order."""
    s = _session(session)
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / f"f861_{year}.zip"
    if target.exists() and target.stat().st_size > 0:
        return target
    for template in constants.ZIP_URL_TEMPLATES.value:
        url = template.format(yyyy=year, yy=f"{year % 100:02d}")
        try:
            r = s.get(url, timeout=300)
        except requests.RequestException:
            continue
        if r.status_code == 200 and r.content[:2] == b"PK":
            target.write_bytes(r.content)
            return target
    return None


def download_eia861m(
    input_dir: Path, session: requests.Session | None = None
) -> Path:
    """Download the single harmonised EIA-861M sales_revenue.xlsx."""
    s = _session(session)
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / "sales_revenue.xlsx"
    with s.get(constants.EIA861M_XLS_URL.value, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(target, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    # The 1990-2009 history is a separate archived workbook.
    hist = input_dir / "HS861M.xlsx"
    try:
        with s.get(
            constants.EIA861M_HISTORICAL_URL.value, stream=True, timeout=300
        ) as r:
            if r.status_code == 200:
                with open(hist, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
    except requests.RequestException:
        pass
    return target


def year_zip(input_dir: Path, year: int) -> Path:
    return Path(input_dir) / f"f861_{year}.zip"


def _resolve_member(names: list[str], table: str, year: int) -> str | None:
    """Find the one ZIP member for a table in a year, by pattern. Raise on >1."""
    for pattern in constants.MEMBER_PATTERNS.value[table]:
        rx = re.compile(pattern.format(year=year), re.IGNORECASE)
        hits = [n for n in names if rx.search(n)]
        # a directory entry ("2005/") is not a member
        hits = [n for n in hits if not n.endswith("/")]
        if len(hits) == 1:
            return hits[0]
        if len(hits) > 1:
            raise ValueError(
                f"{table} {year}: pattern {pattern!r} matched {len(hits)} members: {hits}"
            )
    return None


# --------------------------------------------------------------------------
# Wide -> long parsing
# --------------------------------------------------------------------------

# Canonical id-column synonyms (normalised header token -> canonical name).
_ID_SYNONYMS = {
    "year": "year",
    "data_year": "year",
    "utility_id": "utility_id",
    "utility_number": "utility_id",
    "utility_name": "utility_name",
    "part": "part",
    "schedule_4_part": "part",
    "sched4part": "part",
    "service_type": "service_type",
    "data_type": "data_type",
    "data_type_o_observed": "data_type",
    "state": "state",
    "mail_state": "state",
    "ownership": "ownership_type",
    "owner_type": "ownership_type",
    "ownership_type": "ownership_type",
    "nerc_location": "nerc_region",
    "nerc_region": "nerc_region",
    "ba_code": "ba_code",
    "month": "month",
    "data_status": "data_status",
    "short_form": "short_form",
    "county": "county",
    "state_code": "state",
}

_MEASURE_SYNONYMS = {
    "revenue": "revenue",
    "revenues": "revenue",
    "sales": "sales",
    "customers": "customers",
    "consumers": "consumers",
    "price": "price",
}


def _find_header_row(rows: list[list], must_have: set[str]) -> int:
    """First row index whose normalised cells cover every token in must_have."""
    for i, row in enumerate(rows[:15]):
        norm = {_norm(c) for c in row if c is not None and str(c).strip()}
        if must_have <= norm:
            return i
    raise ValueError(f"header row not found (need {must_have})")


def _find_banner_row(rows: list[list]) -> int:
    for i, row in enumerate(rows[:12]):
        for c in row:
            if c is not None and constants.SECTOR_BANNER.value.get(_norm(c)):
                return i
    raise ValueError("sector banner row not found")


def _parse_banner(rows: list[list]) -> pd.DataFrame:
    """Melt a 3-row banner sheet (sector / measure / id-labels) to long rows.

    Works for both the EIA-861 Sales_Ult_Cust / file2 banner (measures Revenues,
    Sales, Customers) and the EIA-861M sales_revenue banner (which adds Price).
    Returns one row per (id columns) x sector, with raw measure columns
    revenue / sales / customers / price where present. TOTAL is kept here and
    dropped by the caller.
    """
    b = _find_banner_row(rows)
    banner, measure, idrow = rows[b], rows[b + 1], rows[b + 2]
    ncol = max(len(banner), len(measure), len(idrow))

    def cell(row, j):
        return row[j] if j < len(row) else None

    # Forward-fill the sector banner across its merged group.
    sectors: list[str | None] = []
    current = None
    for j in range(ncol):
        slug = (
            constants.SECTOR_BANNER.value.get(_norm(cell(banner, j)))
            if cell(banner, j)
            else None
        )
        if slug:
            current = slug
        sectors.append(current)

    id_cols: dict[int, str] = {}
    value_cols: dict[int, tuple[str, str]] = {}  # col -> (sector, measure)
    for j in range(ncol):
        m = (
            _MEASURE_SYNONYMS.get(_norm(cell(measure, j)))
            if cell(measure, j)
            else None
        )
        sector = sectors[j]
        if m and sector:
            value_cols[j] = (
                sector,
                "customers" if m == "consumers" else m,
            )
        else:
            label = (
                _ID_SYNONYMS.get(_norm(cell(idrow, j)))
                if cell(idrow, j)
                else None
            )
            if label:
                id_cols[j] = label

    records = []
    for row in rows[b + 3 :]:
        if not any(c is not None and str(c).strip() for c in row):
            continue
        base = {name: cell(row, j) for j, name in id_cols.items()}
        # skip footnote / total rows that carry no utility or state id
        if not (base.get("utility_id") or base.get("state")):
            continue
        by_sector: dict[str, dict[str, Any]] = {}
        for j, (sector, meas) in value_cols.items():
            by_sector.setdefault(sector, {})[meas] = cell(row, j)
        for sector, meas in by_sector.items():
            rec = dict(base)
            rec["customer_sector"] = sector
            rec.update(meas)
            records.append(rec)
    return pd.DataFrame(records)


def _parse_retail_flat(rows: list[list]) -> pd.DataFrame:
    """Melt the 2001-2007 flat file2 (single-row header) to long rows."""
    hdr = _find_header_row(rows, {"utility_id"})
    header = [_norm(c) for c in rows[hdr]]
    id_cols: dict[int, str] = {}
    value_cols: dict[int, tuple[str, str]] = {}
    # The flat era has two naming variants: 2001-2006 abbreviate the sector
    # ("Res Revenue (000)" -> res_revenue_000) and 2007 spells it out
    # ("RESIDENTIAL_REVENUES" -> residential_revenues). Both map here.
    prefix = {
        "res": "residential",
        "residential": "residential",
        "com": "commercial",
        "commercial": "commercial",
        "ind": "industrial",
        "industrial": "industrial",
        "trans": "transportation",
        "transportation": "transportation",
        "total": "total",
    }
    for j, h in enumerate(header):
        if h in _ID_SYNONYMS:
            id_cols[j] = _ID_SYNONYMS[h]
            continue
        parts = h.split("_")
        sector = prefix.get(parts[0]) if parts else None
        if not sector:
            continue
        if "revenue" in h:
            value_cols[j] = (sector, "revenue")
        elif "sales" in h:
            value_cols[j] = (sector, "sales")
        elif "consumer" in h or "customer" in h:
            value_cols[j] = (sector, "customers")

    records = []
    for row in rows[hdr + 1 :]:
        if not any(c is not None and str(c).strip() for c in row):
            continue
        base = {
            name: (row[j] if j < len(row) else None)
            for j, name in id_cols.items()
        }
        if not base.get("utility_id"):
            continue
        by_sector: dict[str, dict[str, Any]] = {}
        for j, (sector, meas) in value_cols.items():
            by_sector.setdefault(sector, {})[meas] = (
                row[j] if j < len(row) else None
            )
        for sector, meas in by_sector.items():
            rec = dict(base)
            rec["customer_sector"] = sector
            rec.update(meas)
            records.append(rec)
    return pd.DataFrame(records)


def _avg_price_cents_kwh(
    revenue_thousand: pd.Series, sales_mwh: pd.Series
) -> pd.Series:
    """Average revenue per kWh in cents: revenue_thousand*100 / sales_mwh."""
    rev = pd.to_numeric(revenue_thousand, errors="coerce")
    sales = pd.to_numeric(sales_mwh, errors="coerce")
    price = (rev * 100.0) / sales
    return price.where(sales.notna() & (sales != 0))


def _finish_sectored(
    frame: pd.DataFrame, year: int, has_ba: bool
) -> pd.DataFrame:
    """Common tail for retail_sales: drop TOTAL, derive columns, resolve state."""
    if frame.empty:
        return frame
    frame = frame[frame["customer_sector"] != "total"].copy()
    revenue_thousand = pd.to_numeric(_get(frame, "revenue"), errors="coerce")
    out = pd.DataFrame(
        {
            "year": year,
            "utility_id": as_id(_get(frame, "utility_id")),
            "utility_name": clean_text(_get(frame, "utility_name")),
            "state_id": state_id_for(_get(frame, "state")),
            "ownership_type": clean_text(_get(frame, "ownership_type")),
            "ba_code": clean_text(frame["ba_code"])
            if has_ba and "ba_code" in frame
            else None,
            "part": clean_text(_get(frame, "part")),
            "service_type": clean_text(_get(frame, "service_type")),
            "data_type": clean_text(_get(frame, "data_type")),
            "customer_sector": frame["customer_sector"],
            "sales_mwh": pd.to_numeric(_get(frame, "sales"), errors="coerce"),
            "revenue_usd": revenue_thousand * 1000.0,
            "customer_count": pd.to_numeric(
                _get(frame, "customers"), errors="coerce"
            ),
            "average_price_cents_kwh": _avg_price_cents_kwh(
                revenue_thousand, _get(frame, "sales")
            ),
        }
    )
    # drop rows with no measures at all (footnotes that slipped through)
    measures = ["sales_mwh", "revenue_usd", "customer_count"]
    out = out[out[measures].notna().any(axis=1)]
    return out.reset_index(drop=True)


# --------------------------------------------------------------------------
# Table builders
# --------------------------------------------------------------------------


def build_retail_sales(payload: bytes, member: str, year: int) -> pd.DataFrame:
    is_banner = year >= 2008
    sheet = (
        "States" if member.lower().endswith(".xlsx") or year >= 2008 else None
    )
    rows = _read_member_rows(payload, member, sheet if is_banner else None)
    frame = _parse_banner(rows) if is_banner else _parse_retail_flat(rows)
    has_ba = year >= 2012
    return _finish_sectored(frame, year, has_ba)


def build_utility(payload: bytes, member: str, year: int) -> pd.DataFrame:
    sheet = "States" if member.lower().endswith(".xlsx") else None
    rows = _read_member_rows(payload, member, sheet)
    hdr = (
        _find_header_row(rows, {"utility_id"})
        if year < 2012
        else _find_header_row(rows, {"utility_number"})
    )
    header = [_norm(c) for c in rows[hdr]]
    id_cols = {
        j: _ID_SYNONYMS[h] for j, h in enumerate(header) if h in _ID_SYNONYMS
    }
    records = []
    for row in rows[hdr + 1 :]:
        if not any(c is not None and str(c).strip() for c in row):
            continue
        rec = {
            name: (row[j] if j < len(row) else None)
            for j, name in id_cols.items()
        }
        if rec.get("utility_id"):
            records.append(rec)
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    out = pd.DataFrame(
        {
            "year": year,
            "utility_id": as_id(_get(frame, "utility_id")),
            "utility_name": clean_text(_get(frame, "utility_name")),
            "state_id": state_id_for(_get(frame, "state")),
            "ownership_type": clean_text(_get(frame, "ownership_type")),
            "nerc_region": clean_text(_get(frame, "nerc_region")),
        }
    )
    # one row per (utility, year): keep the first filing if a utility repeats
    out = out.drop_duplicates(subset=["year", "utility_id"]).reset_index(
        drop=True
    )
    return out


def build_service_territory(
    payload: bytes, member: str, year: int
) -> pd.DataFrame:
    rows = _read_member_rows(payload, member, "Counties_States")
    hdr = _find_header_row(rows, {"utility_number", "county"})
    header = [_norm(c) for c in rows[hdr]]
    id_cols = {
        j: _ID_SYNONYMS[h] for j, h in enumerate(header) if h in _ID_SYNONYMS
    }
    records = []
    for row in rows[hdr + 1 :]:
        if not any(c is not None and str(c).strip() for c in row):
            continue
        rec = {
            name: (row[j] if j < len(row) else None)
            for j, name in id_cols.items()
        }
        if rec.get("utility_id") and rec.get("county"):
            records.append(rec)
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    state = clean_text(_get(frame, "state"))
    county = clean_text(_get(frame, "county"))
    out = pd.DataFrame(
        {
            "year": year,
            "utility_id": as_id(_get(frame, "utility_id")),
            "utility_name": clean_text(_get(frame, "utility_name")),
            "state_id": state_id_for(state),
            "county_id": county_id_for(state, county),
            "county_name": county,
            "short_form": clean_text(_get(frame, "short_form")),
        }
    )
    return out.drop_duplicates(
        subset=["year", "utility_id", "state_id", "county_name"]
    ).reset_index(drop=True)


def build_eia861m(input_dir: Path) -> pd.DataFrame:
    """Melt the monthly sales_revenue.xlsx (state x sector) to long, all years."""
    frames = []
    sources = [
        (
            Path(input_dir) / "sales_revenue.xlsx",
            ("Monthly-States", "Monthly-Ter"),
        ),
        (Path(input_dir) / "HS861M.xlsx", ("Monthly",)),
    ]
    for path, sheets in sources:
        if not path.exists():
            continue
        payload = path.read_bytes()
        for sheet in sheets:
            try:
                rows = _rows_from_xlsx(payload, sheet)
            except Exception:
                continue
            frame = _parse_banner(rows)
            if not frame.empty:
                frames.append(frame)
    if not frames:
        return pd.DataFrame()
    frame = pd.concat(frames, ignore_index=True)
    frame = frame[frame["customer_sector"] != "total"].copy()
    revenue_thousand = pd.to_numeric(_get(frame, "revenue"), errors="coerce")
    out = pd.DataFrame(
        {
            "year": pd.to_numeric(
                clean_text(_get(frame, "year")), errors="coerce"
            ).astype("Int64"),
            "month": pd.to_numeric(
                clean_text(_get(frame, "month")), errors="coerce"
            ).astype("Int64"),
            "state_id": state_id_for(_get(frame, "state")),
            "state_code": clean_text(_get(frame, "state")),
            "customer_sector": frame["customer_sector"],
            "data_status": clean_text(_get(frame, "data_status")),
            "sales_mwh": pd.to_numeric(_get(frame, "sales"), errors="coerce"),
            "revenue_usd": revenue_thousand * 1000.0,
            "customer_count": pd.to_numeric(
                _get(frame, "customers"), errors="coerce"
            ),
            "average_price_cents_kwh": pd.to_numeric(
                _get(frame, "price"), errors="coerce"
            ),
        }
    )
    out = out[
        out["year"].notna() & out["month"].notna() & out["state_code"].notna()
    ]
    # If the current and historical files ever overlap on a period, keep the
    # first (current file, listed first) occurrence.
    out = out.drop_duplicates(
        subset=["year", "month", "state_code", "customer_sector"]
    )
    return out.reset_index(drop=True)


_ANNUAL_BUILDERS = {
    "utility": build_utility,
    "retail_sales": build_retail_sales,
    "service_territory": build_service_territory,
}


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def clean_annual_year(
    year: int,
    input_dir: Path,
    output_dir: Path,
    tables: list[str] | None = None,
) -> dict[str, int]:
    """Clean the annual EIA-861 tables for one report year from its ZIP."""
    tables = [
        t
        for t in (tables or constants.ANNUAL_TABLES.value)
        if t in constants.ANNUAL_TABLES.value
    ]
    if not tables:
        return {}
    path = year_zip(input_dir, year)
    if not path.exists():
        return {}
    counts: dict[str, int] = {}
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        for table in tables:
            if (
                table == "service_territory"
                and year < constants.SERVICE_TERRITORY_FIRST_YEAR.value
            ):
                continue
            member = _resolve_member(names, table, year)
            if member is None:
                continue
            payload = zf.read(member)
            frame = _ANNUAL_BUILDERS[table](payload, member, year)
            counts[table] = write_partition(
                frame, load_cols(table), Path(output_dir) / table, year
            )
    return counts


def clean_eia861m(input_dir: Path, output_dir: Path) -> dict[str, int]:
    """Clean the monthly table, one parquet partition per calendar year."""
    cols = load_cols("eia861m")
    built = build_eia861m(input_dir)
    total = 0
    out_table = Path(output_dir) / "eia861m"
    if out_table.exists():
        shutil.rmtree(out_table)
    for year, group in built.groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        total += write_partition(group, cols, out_table, int(year))
    return {"eia861m": total}


def clean_all(
    input_dir: Path,
    output_dir: Path,
    years: list[int] | None = None,
    tables: list[str] | None = None,
    log=print,
) -> dict[str, int]:
    """Clean every requested annual year plus the monthly table."""
    want = set(tables) if tables else set(constants.DATA_TABLES.value)
    totals: dict[str, int] = {}
    annual_want = [t for t in constants.ANNUAL_TABLES.value if t in want]
    if annual_want:
        annual_years = years or list(
            range(constants.FIRST_ANNUAL_YEAR.value, 2026)
        )
        for year in annual_years:
            counts = clean_annual_year(
                year, input_dir, output_dir, annual_want
            )
            for table, n in counts.items():
                totals[table] = totals.get(table, 0) + n
            if counts:
                log(
                    f"{year}: "
                    + ", ".join(
                        f"{t}={n:,}" for t, n in sorted(counts.items())
                    )
                )
    if "eia861m" in want:
        counts = clean_eia861m(input_dir, output_dir)
        totals.update(counts)
        log(f"eia861m: {counts.get('eia861m', 0):,}")
    return totals


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------

LOCAL_LABELS: dict[str, dict[str, str]] = {
    "part": {
        "A": "Schedule 4A — utilities that own or operate bundled (energy plus delivery) retail service",
        "B": "Schedule 4B — energy-only service providers",
        "C": "Schedule 4C — delivery-only service providers",
        "D": "Schedule 4D — behind-the-meter or other retail arrangements",
    },
    "data_type": {
        "O": "Observed value reported by the respondent",
        "I": "Imputed value estimated by EIA",
    },
    "short_form": {
        "Y": "Utility filed the short form (EIA-861S)",
        "N": "Utility filed the full form",
    },
}


def build_dicionario(output_dir: Path, tables: list[str] | None = None) -> int:
    """Build dicionario from the cleaned parquet, over dict-covered columns."""
    import pyarrow.dataset as pads

    tables = tables or list(constants.DATA_TABLES.value)
    rows = []
    for table in tables:
        part = Path(output_dir) / table
        if not part.exists() or not any(part.rglob("*.parquet")):
            continue
        columns = [c.name for c in load_cols(table) if c.covered_by_dictionary]
        if not columns:
            continue
        data = pads.dataset(part, format="parquet")
        seen: dict[tuple[str, str], list[int]] = {}
        for batch in data.to_batches(columns=[*columns, "year"]):
            years = batch.column(len(columns)).to_pylist()
            for index, column in enumerate(columns):
                for value, year in zip(
                    batch.column(index).to_pylist(), years, strict=True
                ):
                    if value is None or value == "" or year is None:
                        continue
                    seen.setdefault((column, value), []).append(int(year))
        for (column, value), yrs in sorted(seen.items()):
            lo, hi = min(yrs), max(yrs)
            label = LOCAL_LABELS.get(column, {}).get(value, value)
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": value,
                    "cobertura_temporal": f"{lo}(1){hi}"
                    if lo != hi
                    else f"{lo}",
                    "valor": label,
                }
            )
    out = Path(output_dir) / "dicionario"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    arrays = [
        pa.array(_stringify(frame[c], "STRING"), type=pa.string())
        for c in frame.columns
    ]
    pq.write_table(
        pa.Table.from_arrays(arrays, names=list(frame.columns)),
        out / "data.parquet",
        compression="snappy",
    )
    return len(frame)


# --------------------------------------------------------------------------
# Source coverage, for the poll
# --------------------------------------------------------------------------


def source_max_annual_year(input_dir: Path) -> int | None:
    """Latest report year for which an annual ZIP is present locally."""
    years = [
        int(p.stem.rsplit("_", 1)[1])
        for p in Path(input_dir).glob("f861_*.zip")
        if p.stem.rsplit("_", 1)[1].isdigit()
    ]
    return max(years) if years else None
