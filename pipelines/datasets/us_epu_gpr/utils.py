"""Download + cleaning transform for us_epu_gpr (shared by the pipeline and the
one-shot bootstrap in models/us_epu_gpr/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
`clean_all` directly. Schema/column order come from the architecture CSVs (the
single source of truth).

The two source families ship as heterogeneous wide spreadsheets; this module
reshapes them into one LONG monthly table and one LONG daily table keyed by
``(year[, month, day], country_id, index_family, index_name)`` with a single
``value`` column. ``country_id`` is ISO 3166-1 alpha-3 (NULL for global
aggregates); ``index_name`` is a coded series identifier resolved by the
``dicionario`` table.
"""

import csv
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_epu_gpr.constants import constants

log = logging.getLogger("us_epu_gpr")

PA = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}
_ARCH = constants.ARCHITECTURE_DIR.value

# ── crosswalks ───────────────────────────────────────────────────────────────
# EPU All_Country_Data column header -> (ISO3, index_name). Plain-English country
# headers; the three China columns are distinct newspaper panels for CHN. The
# GEPU_* columns are taken from the Global file instead, and 'US' from the US
# file's Main News Index (which extends back to 1900), so both are skipped here.
EPU_COUNTRY_COL = {
    "Australia": ("AUS", "epu"),
    "Brazil": ("BRA", "epu"),
    "Canada": ("CAN", "epu"),
    "Chile": ("CHL", "epu"),
    "China": ("CHN", "epu"),
    "France": ("FRA", "epu"),
    "Germany": ("DEU", "epu"),
    "Greece": ("GRC", "epu"),
    "India": ("IND", "epu"),
    "Ireland": ("IRL", "epu"),
    "Italy": ("ITA", "epu"),
    "Japan": ("JPN", "epu"),
    "Korea": ("KOR", "epu"),
    "Mexico": ("MEX", "epu"),
    "Pakistan": ("PAK", "epu"),
    "Russia": ("RUS", "epu"),
    "Spain": ("ESP", "epu"),
    "Singapore": ("SGP", "epu"),
    "SCMP China": ("CHN", "epu_scmp"),
    "Mainland China": ("CHN", "epu_mainland"),
}
_EPU_COUNTRY_SKIP = {"Year", "Month", "GEPU_current", "GEPU_ppp", "UK", "US"}

# US Categorical_EPU_Data column header -> index_name (all USA).
EPU_CATEGORY_COL = {
    "1. Economic Policy Uncertainty": "epu_cat_overall",
    "2. Monetary policy": "epu_cat_monetary",
    "Fiscal Policy (Taxes OR Spending)": "epu_cat_fiscal",
    "3. Taxes": "epu_cat_taxes",
    "4. Government spending": "epu_cat_gov_spending",
    "5. Health care": "epu_cat_health_care",
    "6. National security": "epu_cat_national_security",
    "7. Entitlement programs": "epu_cat_entitlement",
    "8. Regulation": "epu_cat_regulation",
    "Financial Regulation": "epu_cat_financial_regulation",
    "9. Trade policy": "epu_cat_trade_policy",
    "10. Sovereign debt, currency crises": "epu_cat_sovereign_debt",
}

# GPR export global series -> index_name.
GPR_GLOBAL_COL = {
    "GPR": "gpr",
    "GPRT": "gpr_threats",
    "GPRA": "gpr_acts",
    "GPRH": "gpr_historical",
    "GPRHT": "gpr_historical_threats",
    "GPRHA": "gpr_historical_acts",
}

# index_name -> human label (drives the dicionario).
INDEX_NAME_LABELS = {
    "gepu_current": "Global Economic Policy Uncertainty Index, current-price GDP weighted",
    "gepu_ppp": "Global Economic Policy Uncertainty Index, PPP-GDP weighted",
    "epu": "Economic Policy Uncertainty Index, news-based (country-specific base period)",
    "epu_news": "News-based Economic Policy Uncertainty Index (headline series)",
    "epu_scmp": "Economic Policy Uncertainty Index, South China Morning Post coverage of China",
    "epu_mainland": "Economic Policy Uncertainty Index, Mainland China newspapers",
    "epu_three_component": "US three-component Economic Policy Uncertainty Index (news, tax-code expirations, forecaster disagreement)",
    "epu_fedstatelocal_disagreement": "US EPU component: dispersion of federal/state/local expenditure forecasts",
    "epu_cpi_disagreement": "US EPU component: dispersion of CPI forecasts",
    "epu_tax_expiration": "US EPU component: dollar-weighted federal tax-code provisions set to expire",
    "epu_cat_overall": "US categorical EPU: overall Economic Policy Uncertainty",
    "epu_cat_monetary": "US categorical EPU: monetary policy",
    "epu_cat_fiscal": "US categorical EPU: fiscal policy (taxes or spending)",
    "epu_cat_taxes": "US categorical EPU: taxes",
    "epu_cat_gov_spending": "US categorical EPU: government spending",
    "epu_cat_health_care": "US categorical EPU: health care",
    "epu_cat_national_security": "US categorical EPU: national security",
    "epu_cat_entitlement": "US categorical EPU: entitlement programs",
    "epu_cat_regulation": "US categorical EPU: regulation",
    "epu_cat_financial_regulation": "US categorical EPU: financial regulation",
    "epu_cat_trade_policy": "US categorical EPU: trade policy",
    "epu_cat_sovereign_debt": "US categorical EPU: sovereign debt and currency crises",
    "gpr": "Geopolitical Risk Index (global: index 1985:2019=100; country: share of geopolitically-focused articles, percent)",
    "gpr_threats": "Geopolitical Risk Threats sub-index",
    "gpr_acts": "Geopolitical Risk Acts sub-index",
    "gpr_historical": "Historical Geopolitical Risk Index (global: index 1900:2019=100; country: share of articles, percent)",
    "gpr_historical_threats": "Historical Geopolitical Risk Threats sub-index",
    "gpr_historical_acts": "Historical Geopolitical Risk Acts sub-index",
}
INDEX_FAMILY_LABELS = {
    "epu": "Economic Policy Uncertainty (Baker, Bloom and Davis)",
    "gpr": "Geopolitical Risk (Caldara and Iacoviello)",
}

_LONG_COLS = [
    "year",
    "month",
    "country_id",
    "index_family",
    "index_name",
    "value",
]


# ── download ────────────────────────────────────────────────────────────────
def download_all(input_dir: Path) -> Path:
    """Fetch every EPU and GPR source file into ``input_dir``.

    A browser User-Agent is used throughout: policyuncertainty.com sits behind a
    WAF that can block bare clients. matteoiacoviello.com is unprotected.

    Args:
        input_dir: Directory to download into; created if absent.

    Returns:
        The same ``input_dir``, for chaining.

    Raises:
        requests.HTTPError: If any file fails to download.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": constants.USER_AGENT.value}
    for name, url in constants.SOURCE_FILES.value.items():
        r = requests.get(url, headers=headers, timeout=300)
        r.raise_for_status()
        (input_dir / name).write_bytes(r.content)
        log.info(f"downloaded {name} ({len(r.content):,} bytes)")
    return input_dir


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Args:
        table: Table slug (e.g. ``"index_monthly"``), matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="") as fh:
        return list(csv.DictReader(fh))


# ── transform helpers ────────────────────────────────────────────────────────
def _drop_note_rows(
    df: pd.DataFrame, year_col="Year", month_col="Month"
) -> pd.DataFrame:
    """Drop trailing citation/source rows the EPU spreadsheets append.

    Every EPU workbook ends with free-text note rows whose Year/Month cells are
    blank. Coercing Year and Month to numeric and dropping non-numeric rows
    removes them without hardcoding a row count.
    """
    df = df.copy()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
    df[month_col] = pd.to_numeric(df[month_col], errors="coerce")
    return df[df[year_col].notna() & df[month_col].notna()]


def _melt(
    df: pd.DataFrame,
    colmap: dict,
    family: str,
    year_col="Year",
    month_col="Month",
) -> pd.DataFrame:
    """Melt selected wide columns into LONG rows, dropping missing observations.

    Args:
        df: Wide frame with year/month columns and one column per series.
        colmap: source column -> (country_id, index_name); ``country_id`` may be
            None for a global aggregate.
        family: ``"epu"`` or ``"gpr"``.
        year_col, month_col: names of the year and month columns in ``df``.

    Returns:
        Frame with the six LONG columns; rows with a missing value are dropped.
    """
    out = []
    for src, (country_id, index_name) in colmap.items():
        if src not in df.columns:
            log.warning(f"{family}: expected column {src!r} absent")
            continue
        v = pd.to_numeric(df[src], errors="coerce")
        m = v.notna()
        out.append(
            pd.DataFrame(
                {
                    "year": df[year_col][m].astype("int64"),
                    "month": df[month_col][m].astype("int64"),
                    "country_id": country_id,
                    "index_family": family,
                    "index_name": index_name,
                    "value": v[m].astype("float64"),
                }
            )
        )
    return (
        pd.concat(out, ignore_index=True)
        if out
        else pd.DataFrame(columns=_LONG_COLS)
    )


# ── monthly ──────────────────────────────────────────────────────────────────
def build_monthly(input_dir: Path) -> pd.DataFrame:
    """Reshape all monthly EPU and GPR sources into one LONG frame.

    Assembles: global EPU (GEPU current/PPP), country EPU (news-based, plus the
    two extra China panels), the US headline news index (1900+), the four US
    legacy components, the twelve US categorical indices, global GPR (recent and
    historical, total/threats/acts) and country GPR (recent and historical).

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        One row per (year, month, country_id, index_family, index_name) with a
        ``value``, deduplicated and sorted.
    """
    frames = []

    # 1. Global EPU (GEPU) — authoritative from the Global file.
    g = _drop_note_rows(
        pd.read_excel(input_dir / "Global_Policy_Uncertainty_Data.xlsx")
    )
    frames.append(
        _melt(
            g,
            {
                "GEPU_current": (None, "gepu_current"),
                "GEPU_ppp": (None, "gepu_ppp"),
            },
            "epu",
        )
    )

    # 2. Country EPU panel (skip GEPU_* and the US column; US comes from below).
    ac = _drop_note_rows(
        pd.read_excel(input_dir / "All_Country_Data.xlsx", sheet_name="EPU")
    )
    colmap = {
        c: EPU_COUNTRY_COL[c] for c in ac.columns if c in EPU_COUNTRY_COL
    }
    frames.append(_melt(ac, colmap, "epu"))

    # 3. US headline news-based EPU (1900+) as USA country 'epu'.
    us_main = _drop_note_rows(
        pd.read_excel(
            input_dir / "US_Policy_Uncertainty_Data.xlsx",
            sheet_name="Main News Index",
        )
    )
    frames.append(
        _melt(
            us_main, {"News_Based_Policy_Uncert_Index": ("USA", "epu")}, "epu"
        )
    )

    # 4. US legacy components (skip News_Based — covered by the headline above).
    us_leg = _drop_note_rows(
        pd.read_excel(
            input_dir / "US_Policy_Uncertainty_Data.xlsx",
            sheet_name="Legacy Three Component EPU",
        )
    )
    frames.append(
        _melt(
            us_leg,
            {
                "Three_Component_Index": ("USA", "epu_three_component"),
                "FedStateLocal_Ex_disagreement": (
                    "USA",
                    "epu_fedstatelocal_disagreement",
                ),
                "CPI_disagreement": ("USA", "epu_cpi_disagreement"),
                "Tax_expiration": ("USA", "epu_tax_expiration"),
            },
            "epu",
        )
    )

    # 5. US categorical EPU.
    cat = _drop_note_rows(
        pd.read_excel(
            input_dir / "Categorical_EPU_Data.xlsx", sheet_name="Indices"
        )
    )
    frames.append(
        _melt(cat, {c: ("USA", n) for c, n in EPU_CATEGORY_COL.items()}, "epu")
    )

    # 6. GPR — global + country, recent + historical.
    gpr = pd.read_excel(input_dir / "data_gpr_export.xls", engine="xlrd")
    gpr = gpr[gpr["month"].notna()].copy()
    gpr["Year"] = pd.to_datetime(gpr["month"]).dt.year
    gpr["Month"] = pd.to_datetime(gpr["month"]).dt.month
    gpr_map: dict = {c: (None, n) for c, n in GPR_GLOBAL_COL.items()}
    for c in gpr.columns:
        if isinstance(c, str) and c.startswith("GPRC_"):
            gpr_map[c] = (c[5:], "gpr")
        elif isinstance(c, str) and c.startswith("GPRHC_"):
            gpr_map[c] = (c[6:], "gpr_historical")
    frames.append(_melt(gpr, gpr_map, "gpr"))

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(
        ["year", "month", "country_id", "index_family", "index_name"],
        ignore_index=True,
    )
    return df.sort_values(_LONG_COLS[:5], ignore_index=True)


# ── daily ────────────────────────────────────────────────────────────────────
def build_daily(input_dir: Path) -> pd.DataFrame:
    """Reshape the daily EPU and GPR sources into one LONG frame.

    Daily series shipped by the sources: the US news-based EPU (country USA) and
    the global daily GPR total/threats/acts. No country daily series exist.

    Args:
        input_dir: Root of the downloaded files.

    Returns:
        One row per (date, country_id, index_family, index_name) with a
        ``value``; carries year/month/day plus a DATE column.
    """
    frames = []

    # US daily EPU.
    d = pd.read_csv(input_dir / "All_Daily_Policy_Data.csv")
    d = d[pd.to_numeric(d["year"], errors="coerce").notna()].copy()
    v = pd.to_numeric(d["daily_policy_index"], errors="coerce")
    m = v.notna()
    frames.append(
        pd.DataFrame(
            {
                "year": d["year"][m].astype("int64"),
                "month": d["month"][m].astype("int64"),
                "day": d["day"][m].astype("int64"),
                "country_id": "USA",
                "index_family": "epu",
                "index_name": "epu_news",
                "value": v[m].astype("float64"),
            }
        )
    )

    # Global daily GPR.
    gd = pd.read_excel(input_dir / "data_gpr_daily_recent.xls", engine="xlrd")
    gd = gd[gd["date"].notna()].copy()
    dt = pd.to_datetime(gd["date"])
    for src, name in {
        "GPRD": "gpr",
        "GPRD_ACT": "gpr_acts",
        "GPRD_THREAT": "gpr_threats",
    }.items():
        v = pd.to_numeric(gd[src], errors="coerce")
        m = v.notna()
        frames.append(
            pd.DataFrame(
                {
                    "year": dt[m].dt.year.astype("int64"),
                    "month": dt[m].dt.month.astype("int64"),
                    "day": dt[m].dt.day.astype("int64"),
                    "country_id": None,
                    "index_family": "gpr",
                    "index_name": name,
                    "value": v[m].astype("float64"),
                }
            )
        )

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=df["day"])
    ).dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates(
        ["date", "country_id", "index_family", "index_name"], ignore_index=True
    )
    return df.sort_values(
        ["year", "month", "day", "country_id", "index_family", "index_name"],
        ignore_index=True,
    )


# ── dicionario ───────────────────────────────────────────────────────────────
def build_dicionario_df() -> pd.DataFrame:
    """Build the dictionary mapping coded columns to labels.

    Covers ``index_family`` and ``index_name`` for both data tables. Column names
    stay Portuguese (``id_tabela``, ``nome_coluna``, ``chave``,
    ``cobertura_temporal``, ``valor``) — the platform's dictionary renderer
    expects that schema even on an English dataset.
    """
    rows = []
    for table in constants.DATA_TABLES.value:
        for key, label in INDEX_FAMILY_LABELS.items():
            rows.append((table, "index_family", key, None, label))
        for key, label in INDEX_NAME_LABELS.items():
            rows.append((table, "index_name", key, None, label))
    return pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )


# ── write ────────────────────────────────────────────────────────────────────
def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write a table as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column, and ``pipelines.utils.gcs.dump_header`` stringifies the header
    file BigQuery infers the staging schema from. Values pass through the
    architecture's real types first (so ``year`` serializes ``"1959"`` not
    ``"1959.0"`` and ``date`` stays ``"1985-01-01"``), then cast to string via
    arrow — never ``astype(str)``, which renders NULL as ``"nan"``.

    Args:
        df: Rows for one table (LONG monthly/daily or the dicionario).
        table: Table slug, used for the architecture lookup and output path.
        output_dir: Root output directory.

    Returns:
        The table's directory.
    """
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed_schema = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    string_schema = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    # DATE columns must be arrow date32; feed them as datetime.date via strings.
    out = df.reindex(columns=order)

    tdir = output_dir / table
    if "year" in out.columns and table != "dicionario":
        for year, g in out.groupby("year", sort=True):
            pdir = tdir / f"year={int(year)}"
            pdir.mkdir(parents=True, exist_ok=True)
            _write_group(g, typed_schema, string_schema, pdir / "data.parquet")
    else:
        tdir.mkdir(parents=True, exist_ok=True)
        _write_group(out, typed_schema, string_schema, tdir / "data.parquet")
    log.info(f"{table}: {len(out):,} rows -> {tdir}")
    return tdir


def _write_group(
    g: pd.DataFrame, typed_schema, string_schema, path: Path
) -> None:
    """Cast one partition through its real types, then to all-STRING, and write."""
    cols = {}
    for f in typed_schema:
        s = g[f.name]
        if pa.types.is_date(f.type):
            cols[f.name] = pa.array(
                pd.to_datetime(s).dt.date, type=pa.date32()
            )
        else:
            cols[f.name] = pa.array(s, type=f.type)
    at = pa.table(cols, schema=typed_schema).cast(string_schema)
    pq.write_table(at, path, compression="snappy")


# ── entry point ──────────────────────────────────────────────────────────────
def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build all three tables from the downloaded sources.

    The single entry point shared by the recurring pipeline and the one-shot
    bootstrap in ``models/us_epu_gpr/code/``.

    Args:
        input_dir: Root of the downloaded files.
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to output directory, plus ``"max_year_month"`` —
        the latest ``"YYYY-MM"`` in the monthly table, used to poll whether the
        sources have published a new period.
    """
    monthly = build_monthly(input_dir)
    daily = build_daily(input_dir)
    result: dict[str, object] = {
        "index_monthly": write_partitioned(
            monthly, "index_monthly", output_dir
        ),
        "index_daily": write_partitioned(daily, "index_daily", output_dir),
        "dicionario": write_partitioned(
            build_dicionario_df(), "dicionario", output_dir
        ),
    }
    if len(monthly):
        last = monthly.sort_values(["year", "month"]).iloc[-1]
        result["max_year_month"] = (
            f"{int(last['year'])}-{int(last['month']):02d}"
        )
    else:
        result["max_year_month"] = None
    return result
