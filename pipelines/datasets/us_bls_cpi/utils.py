"""Download + cleaning transform for us_bls_cpi (shared by the pipeline and the
one-shot bootstrap in models/us_bls_cpi/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
`clean_all` directly. Schema/column order come from the architecture CSVs (the
single source of truth).
"""

import csv
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_bls_cpi.constants import constants

log = logging.getLogger("us_bls_cpi")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}
SURVEYS = constants.SURVEYS.value
KEYS = constants.KEYS.value
_ARCH = constants.ARCHITECTURE_DIR.value

_SURVEY_LABEL = {
    "CPI-U": "All Urban Consumers",
    "CPI-W": "Urban Wage Earners and Clerical Workers",
}
_SEASONAL_LABEL = {"S": "Seasonally Adjusted", "U": "Not Seasonally Adjusted"}


# ── download ────────────────────────────────────────────────────────────────
def download_flatfiles(input_dir: Path) -> Path:
    """Fetch the CPI-U and CPI-W dimension and full-history data files.

    Files land in ``input_dir/<survey>/``. A browser-like User-Agent carrying a
    contact email is mandatory: download.bls.gov returns 403 without one.

    Args:
        input_dir: Directory to download into; created if absent.

    Returns:
        The same ``input_dir``, for chaining.

    Raises:
        requests.HTTPError: If any file fails to download.
    """
    headers = {"User-Agent": constants.USER_AGENT.value}
    base = constants.BASE_URL.value
    for s in SURVEYS:
        sdir = input_dir / s
        sdir.mkdir(parents=True, exist_ok=True)
        names = [f"{s}.{d}" for d in constants.DIM_FILES.value] + [
            f"{s}.data.{g}" for g in constants.DATA_GROUPS.value
        ]
        for name in names:
            r = requests.get(
                f"{base}/{s}/{name}", headers=headers, timeout=300
            )
            r.raise_for_status()
            (sdir / name).write_bytes(r.content)
        log.info(f"{s}: downloaded {len(names)} files -> {sdir}")
    return input_dir


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Column order and BigQuery types come from here, never from the raw files, so
    the pipeline and the one-shot bootstrap cannot drift apart.

    Args:
        table: Table slug (e.g. ``"monthly"``), matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


# ── transform (ported from the validated bootstrap) ─────────────────────────
def load_series(input_dir: Path, survey_dir: str) -> pd.DataFrame:
    """Load one survey's ``.series`` file — the authoritative dimension table.

    Args:
        input_dir: Root of the downloaded files.
        survey_dir: Survey code, ``"cu"`` (CPI-U) or ``"cw"`` (CPI-W).

    Returns:
        One row per series, with ``series_id`` and its dimensions (survey,
        seasonal_adjustment, area_id, item_id, base_period).
    """
    s = survey_dir
    df = pd.read_csv(
        input_dir / s / f"{s}.series", sep="\t", dtype=str, na_filter=False
    )
    df.columns = [c.strip() for c in df.columns]
    return pd.DataFrame(
        {
            "series_id": df["series_id"].str.strip(),
            "survey": SURVEYS[s],
            "seasonal_adjustment": df["seasonal"].str.strip(),
            "area_id": df["area_code"].str.strip(),
            "item_id": df["item_code"].str.strip(),
            "base_period": df["base_period"].str.strip(),
        }
    )


def load_data(input_dir: Path, survey_dir: str) -> pd.DataFrame:
    """Load and concatenate one survey's by-group observation files.

    Skips ``.data.0.Current``, which is a recent-only subset of the by-group
    files and would duplicate rows. BLS writes ``-`` for a missing observation;
    those become NULL rather than 0.

    Args:
        input_dir: Root of the downloaded files.
        survey_dir: Survey code, ``"cu"`` or ``"cw"``.

    Returns:
        Long observations: ``series_id``, ``year``, ``period``, ``index_value``.
    """
    s = survey_dir
    frames = []
    for path in sorted((input_dir / s).glob(f"{s}.data.*")):
        if path.name.endswith(".0.Current"):
            continue
        d = pd.read_csv(path, sep="\t", dtype=str, na_filter=False)
        d.columns = [c.strip() for c in d.columns]
        frames.append(d[["series_id", "year", "period", "value"]])
    df = pd.concat(frames, ignore_index=True)
    df["series_id"] = df["series_id"].str.strip()
    df["period"] = df["period"].str.strip()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("int64")
    df["index_value"] = pd.to_numeric(
        # pyrefly: ignore [bad-argument-type]
        df["value"].str.strip().replace({"-": ""}).replace({"": None}),
        errors="coerce",
    )
    return df[["series_id", "year", "period", "index_value"]]


def build_long(input_dir: Path) -> pd.DataFrame:
    """Join both surveys' observations to their dimensions into one long frame.

    The by-group data files cross-list roughly 1,400 series in more than one
    group with identical values, so observations are deduplicated on
    ``(series_id, year, period)`` before the join. Without this the percent-change
    self-merge fans out and row counts explode.

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        One row per (series, year, period) with dimensions attached.
    """
    series = pd.concat(
        [load_series(input_dir, s) for s in SURVEYS], ignore_index=True
    )
    data = pd.concat(
        [load_data(input_dir, s) for s in SURVEYS], ignore_index=True
    )
    # by-group files cross-list ~1,400 series (identical values) -> dedup.
    data = data.drop_duplicates(
        ["series_id", "year", "period"], ignore_index=True
    )
    df = data.merge(series, on="series_id", how="left", validate="many_to_one")
    df = df[df["survey"].notna()]
    for c in [*KEYS, "base_period"]:
        df[c] = df[c].astype("category")
    return df


def _add_change(df, ordinal, lag, name):
    """Add a percent-change column by self-merging on a lagged time ordinal.

    Merging on an integer ordinal (rather than shifting rows) keeps the result
    correct across gaps: a series missing a month yields NULL for that comparison
    instead of silently comparing to the wrong period. BLS publishes index levels
    only — every percent change is computed here.

    Args:
        df: Frame carrying ``_key`` (series identity), the ordinal, and
            ``index_value``.
        ordinal: Integer time-ordinal column name (e.g. months since epoch).
        lag: Periods back to compare against — 1 for period-on-period, 12 for
            year-on-year monthly.
        name: Name of the percent-change column to add.

    Returns:
        ``df`` with ``name`` added, rounded to 4 decimals.
    """
    prev = df[["_key", ordinal, "index_value"]].copy()
    prev[ordinal] = prev[ordinal] + lag
    prev = prev.rename(columns={"index_value": "_prev"})
    df = df.merge(prev, on=["_key", ordinal], how="left")
    df[name] = ((df["index_value"] / df["_prev"] - 1.0) * 100).round(4)
    return df.drop(columns=["_prev"])


def build_table(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Slice the long frame into one output table and attach its percent changes.

    Period selection per table:

    - ``monthly``: ``M01`` to ``M12``; adds monthly and 12-month change.
    - ``annual``: ``M13`` (annual average) only; adds annual change.
    - ``semiannual``: ``S01``/``S02``; adds semiannual change.

    Args:
        df: Long frame from :func:`build_long`.
        table: One of ``"monthly"``, ``"annual"``, ``"semiannual"``.

    Returns:
        The table's rows, with period columns and change columns added.

    Raises:
        ValueError: If ``table`` is not one of the three known slugs.
    """
    p = df["period"]
    if table == "monthly":
        sub = df[p.str.startswith("M") & (p != "M13")].copy()
        sub["month"] = sub["period"].str.slice(1).astype("int64")
        sub["_key"] = sub[KEYS].astype(str).agg("|".join, axis=1)
        sub["_t"] = sub["year"] * 12 + (sub["month"] - 1)
        sub = _add_change(sub, "_t", 1, "monthly_change")
        sub = _add_change(sub, "_t", 12, "twelve_month_change")
    elif table == "annual":
        # M13 only (annual average of monthly series); S03 excluded — it collides
        # on the natural key with M13 and equals the mean of the two halves.
        sub = df[p == "M13"].copy()
        sub["_key"] = sub[KEYS].astype(str).agg("|".join, axis=1)
        sub["_t"] = sub["year"]
        sub = _add_change(sub, "_t", 1, "annual_change")
    elif table == "semiannual":
        sub = df[(p == "S01") | (p == "S02")].copy()
        sub["half"] = sub["period"].str.slice(2).astype("int64")
        sub["_key"] = sub[KEYS].astype(str).agg("|".join, axis=1)
        sub["_t"] = sub["year"] * 2 + (sub["half"] - 1)
        sub = _add_change(sub, "_t", 1, "semiannual_change")
    else:
        raise ValueError(table)
    return sub


def write_partitioned(sub: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write a table as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column to its real type, and ``pipelines.utils.gcs.dump_header``
    stringifies the header file that BigQuery infers the staging schema from.
    Emitting typed parquet against that STRING schema makes BigQuery reject the
    files ("Parquet column 'index_value' has type DOUBLE which does not match the
    target cpp_type STRING_PIECE"). See [[project_dump_header_parquet_bug]].

    Values still pass through the architecture's real types first, so ``year``
    serializes as ``"1959"`` rather than ``"1959.0"``, and only then cast to
    string via arrow — never ``astype(str)``, which would render a NULL as the
    literal ``"nan"`` and defeat the dbt ``safe_cast``. ``index_value`` has
    genuine NULLs wherever BLS printed ``-``.

    Args:
        sub: Rows for one table, from :func:`build_table`.
        table: Table slug, used for the architecture lookup and output path.
        output_dir: Root output directory.

    Returns:
        The table's directory, ``<output_dir>/<table>/year=<YYYY>/data.parquet``.
    """
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed_schema = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    for c in [*KEYS, "base_period"]:
        if c in sub:
            sub[c] = sub[c].astype("object")
    out = sub[order]
    tdir = output_dir / table
    for year, g in out.groupby("year", sort=True):
        # pyrefly: ignore [bad-argument-type]
        pdir = tdir / f"year={int(year)}"
        pdir.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(g, schema=typed_schema, preserve_index=False)
        at = at.cast(string_schema)
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
    log.info(f"{table}: {len(out):,} rows -> {tdir}")
    return tdir


def _dim_map(input_dir: Path, fname, code_col, name_col) -> dict:
    """Build a code→label map from a dimension file, merged across both surveys.

    CPI-U and CPI-W ship overlapping area and item files. ``cu`` is read last so
    its labels win where the two disagree.

    Args:
        input_dir: Root of the downloaded files.
        fname: Dimension file suffix, e.g. ``"area"`` for ``cu.area``.
        code_col: Column holding the code.
        name_col: Column holding the human-readable label.

    Returns:
        Mapping of code to label.
    """
    out = {}
    for s in ("cw", "cu"):  # cu last so it overrides cw on overlap
        df = pd.read_csv(
            input_dir / s / f"{s}.{fname}",
            sep="\t",
            dtype=str,
            na_filter=False,
        )
        df.columns = [c.strip() for c in df.columns]
        for _, r in df.iterrows():
            out[r[code_col].strip()] = r[name_col].strip()
    return out


def build_dicionario(input_dir: Path, output_dir: Path) -> Path:
    """Build the ``dicionario`` table mapping coded columns to their labels.

    Covers ``survey``, ``seasonal_adjustment``, ``area_id`` and ``item_id``, for
    each data table. Column names stay Portuguese (``id_tabela``, ``nome_coluna``,
    ``chave``, ``cobertura_temporal``, ``valor``) even though this dataset is
    English: the platform's dictionary renderer expects that schema.

    Args:
        input_dir: Root of the downloaded files, for the area/item dimensions.
        output_dir: Root output directory.

    Returns:
        The dictionary's output directory.
    """
    maps = {
        "survey": _SURVEY_LABEL,
        "seasonal_adjustment": _SEASONAL_LABEL,
        "area_id": _dim_map(input_dir, "area", "area_code", "area_name"),
        "item_id": _dim_map(input_dir, "item", "item_code", "item_name"),
    }
    rows = [
        (t, col, key, None, label)
        for t in constants.DATA_TABLES.value
        for col, mapping in maps.items()
        for key, label in mapping.items()
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    schema = pa.schema(
        [
            pa.field(a["name"], PA[a["bigquery_type"]])
            for a in read_arch("dicionario")
        ]
    )
    tdir = output_dir / "dicionario"
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        tdir / "data.parquet",
        compression="snappy",
    )
    log.info(f"dicionario: {len(df):,} rows -> {tdir}")
    return tdir


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build all four tables from the downloaded flat files.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.us_bls_cpi.tasks.clean_cpi`) and the one-shot
    bootstrap in ``models/us_bls_cpi/code/``.

    Args:
        input_dir: Root of the downloaded files.
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to output directory, plus ``"max_year_month"`` —
        the latest ``"YYYY-MM"`` in the monthly table, used to poll whether BLS
        has published a new period. None if the monthly table is empty.
    """
    df = build_long(input_dir)
    result = {}
    max_ym = None
    for table in constants.DATA_TABLES.value:
        sub = build_table(df, table)
        result[table] = write_partitioned(sub, table, output_dir)
        if table == "monthly" and len(sub):
            latest = sub.sort_values(["year", "month"]).iloc[-1]
            max_ym = f"{int(latest['year'])}-{int(latest['month']):02d}"
    result["dicionario"] = build_dicionario(input_dir, output_dir)
    # pyrefly: ignore [unsupported-operation]
    result["max_year_month"] = max_ym
    return result
