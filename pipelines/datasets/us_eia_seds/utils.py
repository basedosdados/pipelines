"""Cleaning transform for us_eia_seds (SEDS).

Pure functions, no Prefect imports — shared by the one-shot bootstrap
(``models/us_eia_seds/code/``) and the recurring pipeline (``tasks.py`` /
``flows.py``), so the two can never drift.

The source is one long file, ``Complete_SEDS.csv`` (``Data_Status, MSN,
StateCode, Year, Data``), decoded against a vendored copy of EIA's codes
workbook. SEDS restates its whole 1960-latest series on every release, so
``clean_all`` rebuilds every year partition from the one file it is given.

See ``models/us_eia_seds/CLAUDE.md`` for the design.
"""

import csv
import re
import shutil
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_eia_seds.constants import constants

_WS = re.compile(r"\s+")
# EIA's missing-value stand-ins: a bare dot, runs of hyphens, blanks, NA words.
_NA_RE = re.compile(r"^(\.|-+|n/?a|null|nan)$", re.IGNORECASE)


# --------------------------------------------------------------------------
# Architecture
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Col:
    """One column of the architecture table.

    The transform needs ``name`` / ``bq_type``; the rest is carried so the
    metadata scripts read the same CSV through the same parser.
    """

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
    """Load ordered column specs from the architecture CSV for a table."""
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
# Cleaning helpers
# --------------------------------------------------------------------------


def clean_text(series: pd.Series) -> pd.Series:
    """Strip, collapse internal whitespace, and turn NA stand-ins into None."""

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


def _stringify(series: pd.Series, bq_type: str) -> list[str | None]:
    """Render one typed column as the strings the all-STRING staging holds.

    Types are passed through before stringifying, never ``astype(str)``: that
    renders a missing value as the literal ``"nan"`` (which ``safe_cast`` will
    not turn back into NULL) and an integer year as ``"1960.0"`` (which
    ``safe_cast(... as int64)`` turns into NULL).
    """
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
    """Project a built frame onto the architecture and return an all-STRING table."""
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
    """Write one year as ``<out_dir>/year=<year>/data.parquet``; empty writes nothing."""
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
    """Fail loudly if any parquet under ``path`` is non-string or zero-row."""
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
# Directory and code tables
# --------------------------------------------------------------------------


@cache
def _state_directory() -> dict[str, str]:
    """``{state postal abbreviation: state FIPS}`` from the committed directory export."""
    states: dict[str, str] = {}
    with open(constants.COUNTY_DIRECTORY.value, encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            states[row["abbreviation_state"].strip().upper()] = row[
                "id_state"
            ].strip()
    return states


def state_id_for(series: pd.Series) -> pd.Series:
    """Two-letter postal code -> state FIPS, or null (US, X3, X5 have none)."""
    states = _state_directory()
    return clean_text(series).map(
        lambda v: states.get(v.upper()) if isinstance(v, str) else None
    )


@cache
def load_msn_codes() -> dict[str, dict[str, str]]:
    """``{MSN: {"description":..., "unit":...}}`` from the vendored codes table."""
    out: dict[str, dict[str, str]] = {}
    with open(constants.MSN_CODES.value, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            out[r["msn"].strip().upper()] = {
                "description": r["description"].strip(),
                "unit": r["unit"].strip(),
            }
    return out


@cache
def load_state_codes() -> dict[str, str]:
    """``{state code: description}`` from the vendored codes table."""
    with open(constants.STATE_CODES.value, encoding="utf-8") as fh:
        return {
            r["code"].strip(): r["description"].strip()
            for r in csv.DictReader(fh)
        }


def measure_type_for(msn: str) -> str:
    """MSN character 5 -> measure_type slug; unknown falls back to ``other``.

    A recurring pipeline must not die on a measure code EIA adds later, so an
    unrecognised fifth character maps to ``other`` rather than raising — the MSN
    itself is preserved and still carries its full description in dicionario.
    """
    table = constants.MEASURE_TYPE.value
    if not isinstance(msn, str) or len(msn) < 5:
        return "other"
    return table.get(msn[4].upper(), "other")


MEASURE_TYPE_LABELS = {
    "consumption_btu": "Consumption in energy units (British thermal units)",
    "consumption_physical": "Consumption in physical units (barrels, short tons, cubic feet, kilowatthours)",
    "price": "Price per unit of energy",
    "expenditure": "Expenditure in dollars",
    "co2_emissions": "Carbon dioxide emissions",
    "electricity": "Electricity in kilowatthours",
    "capacity": "Generating or other capacity",
    "conversion_factor": "Heat content or price conversion factor",
    "number": "Count of units, vehicles or degree days",
    "other": "Other indicator (chained-dollar GDP, emission factors, and similar)",
}


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------


def _session(session: requests.Session | None = None) -> requests.Session:
    if session is not None:
        return session
    s = requests.Session()
    s.headers.update({"User-Agent": constants.USER_AGENT.value})
    return s


def download_complete(
    input_dir: Path, session: requests.Session | None = None
) -> Path:
    """Download ``Complete_SEDS.csv`` into ``input_dir`` and return its path."""
    s = _session(session)
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / "Complete_SEDS.csv"
    with s.get(
        constants.COMPLETE_CSV_URL.value, stream=True, timeout=600
    ) as r:
        r.raise_for_status()
        with open(target, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return target


def download_codes(
    input_dir: Path, session: requests.Session | None = None
) -> Path:
    """Download the codes workbook into ``input_dir`` and return its path."""
    s = _session(session)
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / "Codes_and_Descriptions.xlsx"
    with s.get(constants.CODES_URL.value, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(target, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return target


# --------------------------------------------------------------------------
# Read and build
# --------------------------------------------------------------------------


def _complete_path(input_dir: Path) -> Path:
    return Path(input_dir) / "Complete_SEDS.csv"


def read_complete(input_dir: Path) -> pd.DataFrame:
    """Read the whole ``Complete_SEDS.csv`` as strings."""
    return pd.read_csv(
        _complete_path(input_dir),
        dtype=str,
        keep_default_na=False,
        na_values=[],
    )


def build_seds_consumption(frame: pd.DataFrame) -> pd.DataFrame:
    """Build the clean ``seds_consumption`` frame from raw SEDS rows.

    Input columns: ``Data_Status, MSN, StateCode, Year, Data``. The output is
    the long table the architecture declares, decoded against the vendored
    codes: state FIPS resolved from the postal code, the MSN's published unit
    attached per row, and measure_type derived from the MSN's fifth character.
    """
    msn_codes = load_msn_codes()
    year = pd.to_numeric(clean_text(frame["Year"]), errors="coerce").astype(
        "Int64"
    )
    msn = clean_text(frame["MSN"]).map(
        lambda v: v.upper() if isinstance(v, str) else v
    )
    state_code = clean_text(frame["StateCode"])
    out = pd.DataFrame(
        {
            "year": year,
            "state_id": state_id_for(state_code),
            "state_code": state_code,
            "msn": msn,
            "measure_type": msn.map(
                lambda v: measure_type_for(v) if isinstance(v, str) else None
            ),
            "value": pd.to_numeric(clean_text(frame["Data"]), errors="coerce"),
            "measurement_unit": msn.map(
                lambda v: (
                    (msn_codes.get(v, {}) or {}).get("unit") or None
                    if isinstance(v, str)
                    else None
                )
            ),
            "data_status": clean_text(frame["Data_Status"]),
        }
    )
    # A row with no MSN, state or year is not interpretable; a null value is
    # kept (SEDS files a value for every series-state-year, including 0).
    out = out[
        out["msn"].notna() & out["state_code"].notna() & out["year"].notna()
    ]
    return out.reset_index(drop=True)


def clean_all(
    input_dir: Path,
    output_dir: Path,
    years: list[int] | None = None,
    tables: list[str] | None = None,
    log=print,
) -> dict[str, int]:
    """Clean the whole SEDS file into one parquet partition per year.

    The source is a single long file, so it is read once and grouped by year;
    ``years`` optionally restricts which partitions are written.
    """
    tables = tables or list(constants.DATA_TABLES.value)
    if "seds_consumption" not in tables:
        return {}
    cols = load_cols("seds_consumption")
    built = build_seds_consumption(read_complete(input_dir))
    wanted = set(years) if years else None
    out_table = Path(output_dir) / "seds_consumption"
    total = 0
    for year, group in built.groupby("year", sort=True):
        y = int(year)  # pyrefly: ignore [bad-argument-type]
        if wanted and y not in wanted:
            continue
        n = write_partition(group, cols, out_table, y)
        total += n
        log(f"{y}: seds_consumption={n:,}")
    return {"seds_consumption": total}


def clean_year(
    year: int,
    input_dir: Path,
    output_dir: Path,
    tables: list[str] | None = None,
) -> dict[str, int]:
    """Clean a single year (reads the whole file, keeps one partition)."""
    return clean_all(input_dir, output_dir, years=[year], tables=tables)


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------


def _data_status_label(value: str) -> str:
    """``2024F`` -> a human description of the vintage/status stamp."""
    v = value.strip()
    suffix = {"F": "final estimates", "P": "preliminary estimates"}
    if len(v) == 5 and v[:4].isdigit() and v[4].upper() in suffix:
        return f"{v[:4]} {suffix[v[4].upper()]}"
    return v


def build_dicionario(output_dir: Path, tables: list[str] | None = None) -> int:
    """Write ``dicionario`` from the cleaned parquet, with per-value coverage.

    Enumerates every value of each dict-covered column of ``seds_consumption``
    (``state_code``, ``msn``, ``measure_type``, ``data_status``), attaches its
    human label, and records the years the value appears in as
    ``start(1)end``.
    """
    import pyarrow.dataset as pads

    out_table = Path(output_dir) / "seds_consumption"
    if not out_table.exists() or not any(out_table.rglob("*.parquet")):
        return 0
    dict_cols = ["state_code", "msn", "measure_type", "data_status"]

    msn_codes = load_msn_codes()
    state_codes = load_state_codes()

    def label(column: str, value: str) -> str:
        if column == "msn":
            return (msn_codes.get(value, {}) or {}).get("description") or value
        if column == "state_code":
            return state_codes.get(value, value)
        if column == "measure_type":
            return MEASURE_TYPE_LABELS.get(value, value)
        if column == "data_status":
            return _data_status_label(value)
        return value

    # No hive partitioning: year is already a column inside every file, and
    # letting pyarrow also derive it from the year=<year> directory yields an
    # int partition field that will not merge with the string column.
    data = pads.dataset(out_table, format="parquet")
    seen: dict[tuple[str, str], list[int]] = {}
    for batch in data.to_batches(columns=[*dict_cols, "year"]):
        years = batch.column(len(dict_cols)).to_pylist()
        for index, column in enumerate(dict_cols):
            for value, year in zip(
                batch.column(index).to_pylist(), years, strict=True
            ):
                if value is None or value == "" or year is None:
                    continue
                seen.setdefault((column, value), []).append(int(year))

    rows = []
    for (column, value), years in sorted(seen.items()):
        lo, hi = min(years), max(years)
        rows.append(
            {
                "id_tabela": "seds_consumption",
                "nome_coluna": column,
                "chave": value,
                "cobertura_temporal": f"{lo}(1){hi}" if lo != hi else f"{lo}",
                "valor": label(column, value),
            }
        )
    dic = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    ).sort_values(["id_tabela", "nome_coluna", "chave"])

    target = Path(output_dir) / "dicionario"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    arrays = [
        pa.array(_stringify(dic[c], "STRING"), type=pa.string())
        for c in dic.columns
    ]
    pq.write_table(
        pa.Table.from_arrays(arrays, names=list(dic.columns)),
        target / "data.parquet",
        compression="snappy",
    )
    return len(dic)


def source_max_date(input_dir: Path) -> str:
    """Latest report year present in the source file, as ``YYYY`` (for the poll)."""
    years = pd.to_numeric(
        read_complete(input_dir)["Year"], errors="coerce"
    ).dropna()
    return str(int(years.max()))
