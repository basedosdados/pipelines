"""Pure download and cleaning functions for us_census_trade.

No Prefect imports here. ``tasks.py`` wraps these; the one-shot bootstrap under
``models/us_census_trade/code/`` imports them rather than duplicating the
transform.

The column set, order and types of every table come from the architecture CSVs
in ``models/us_census_trade/code/architecture``. Nothing in this module
re-declares a schema: the API variables to request are read from each
architecture row's ``original_name``.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.us_census_trade.constants import constants

log = logging.getLogger(__name__)

PA_TYPES = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
}


# --------------------------------------------------------------------------- #
# Table specifications
# --------------------------------------------------------------------------- #
# endpoint: path under the intltrade API root.
# place: the API variable naming the place dimension.
# grain: the cleaned columns that must uniquely identify a row.
TABLE_SPECS = {
    "import": {
        "endpoint": "imports/hs",
        "place": "DISTRICT",
        "grain": [
            "year",
            "month",
            "country_code",
            "district_code",
            "hs6_code",
        ],
    },
    "export": {
        "endpoint": "exports/hs",
        "place": "DISTRICT",
        "grain": [
            "year",
            "month",
            "country_code",
            "district_code",
            "hs6_code",
            "domestic_foreign_code",
        ],
    },
    "import_port": {
        "endpoint": "imports/porths",
        "place": "PORT",
        "grain": ["year", "month", "country_code", "port_code", "hs6_code"],
    },
    "export_port": {
        "endpoint": "exports/porths",
        "place": "PORT",
        "grain": ["year", "month", "country_code", "port_code", "hs6_code"],
    },
    "import_state": {
        "endpoint": "imports/statehs",
        "place": "STATE",
        "grain": [
            "year",
            "month",
            "country_code",
            "state_abbreviation",
            "hs6_code",
        ],
    },
    "export_state": {
        "endpoint": "exports/statehs",
        "place": "STATE",
        "grain": [
            "year",
            "month",
            "country_code",
            "state_abbreviation",
            "hs6_code",
        ],
    },
}

FACT_TABLES = tuple(TABLE_SPECS)

# Columns computed in this module rather than read from the API response.
DERIVED = {
    "country_iso2_code",
    "hs4_code",
    "hs2_code",
    "hs_revision",
    "state_id",
}

# Non-additive columns: carried through a grain collapse by first non-null
# value rather than summed.
NON_ADDITIVE_SUFFIXES = ("_unit",)

# FIPS state code by two-letter abbreviation, for the state_id directory link.
# 50 states, DC and the five inhabited territories. Census trade STATE values
# that are not in this map (unknown or unallocated codes) get a null state_id.
STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "AS": "60",
    "GU": "66",
    "MP": "69",
    "PR": "72",
    "VI": "78",
}


# --------------------------------------------------------------------------- #
# Architecture
# --------------------------------------------------------------------------- #
def read_arch(table: str) -> list[dict]:
    """Return the architecture rows for one table, in column order."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"{table}.csv"
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def api_variables(table: str) -> list[str]:
    """API variables to request for a table, derived from the architecture.

    A column's ``original_name`` is the API variable it comes from. Derived
    columns share the source of the column they are computed from, so the set
    is deduplicated. ``COMM_LVL`` and ``SUMMARY_LVL`` are always requested:
    they are filtered again client-side, because getting either wrong silently
    inflates every total.
    """
    wanted = {
        row["original_name"]
        for row in read_arch(table)
        if row["original_name"] and row["name"] not in ("year", "month")
    }
    return sorted(wanted | {"YEAR", "MONTH", "COMM_LVL", "SUMMARY_LVL"})


# --------------------------------------------------------------------------- #
# API key and HTTP
# --------------------------------------------------------------------------- #
def _key() -> str:
    """Return the Census API key: ``CENSUS_API_KEY`` env var if set, else Vault.

    Locally the key is provided via the environment. On the deployed Prefect
    worker there is no such env var, so it is read from HashiCorp Vault at
    ``constants.VAULT_SECRET_PATH`` under ``constants.VAULT_KEY``.

    Raises:
        RuntimeError: If the key is found in neither the environment nor Vault.
    """
    k = os.environ.get(constants.ENV_KEY.value, "").strip()
    if k:
        return k
    # Deployed worker: read from Vault. Imported lazily so local use (and unit
    # tests) never require hvac / Vault connectivity.
    from pipelines.utils.vault import get_credentials_from_secret

    tokens = get_credentials_from_secret(constants.VAULT_SECRET_PATH.value)
    k = str(tokens.get(constants.VAULT_KEY.value, "")).strip()
    if not k:
        raise RuntimeError(
            "CENSUS_API_KEY not set in environment and not found in Vault at "
            f"{constants.VAULT_SECRET_PATH.value!r}. Every api.census.gov "
            "request needs a key; anonymous requests are redirected to a "
            "'Missing Key' page."
        )
    return k


_last_call = [0.0]


def _throttle() -> None:
    interval = constants.MIN_INTERVAL.value
    dt = time.monotonic() - _last_call[0]
    if dt < interval:
        time.sleep(interval - dt)
    _last_call[0] = time.monotonic()


def _redact(url: str) -> str:
    """Strip the API key from a URL so it never reaches a log."""
    return urllib.parse.urlsplit(url)._replace(query="").geturl() + "?<params>"


class CensusAPIError(RuntimeError):
    pass


def _get(endpoint: str, params: dict, timeout: int = 600) -> list[list[str]]:
    """Call one Census API endpoint and return its raw JSON rows.

    The API answers a keyless or malformed request with an HTML page rather
    than an error status, so a non-JSON body is raised rather than parsed.
    """
    query = dict(params)
    query["key"] = _key()
    url = (
        constants.BASE_URL.value
        + endpoint
        + "?"
        + urllib.parse.urlencode(query)
    )
    _throttle()
    req = urllib.request.Request(url, headers=constants.HTTP_HEADERS.value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300] if exc.fp else ""
        # 204 is how the API reports a month it has not published yet.
        if exc.code == 204:
            return []
        raise CensusAPIError(
            f"{_redact(url)} -> HTTP {exc.code}: {detail}"
        ) from exc
    stripped = body.lstrip()
    if not stripped:
        return []
    if not stripped.startswith("["):
        raise CensusAPIError(
            f"{_redact(url)} -> non-JSON response (likely a missing or "
            f"rejected API key): {stripped[:200]!r}"
        )
    return json.loads(body)


def fetch_month(table: str, year: int, month: int) -> pd.DataFrame:
    """Fetch one month of one table from the API, as raw API columns.

    Returns an empty frame when the month is not yet published.
    """
    spec = TABLE_SPECS[table]
    params = {
        "get": ",".join(api_variables(table)),
        "COMM_LVL": constants.COMM_LVL.value,
        "time": f"{year:04d}-{month:02d}",
    }
    rows = _get(spec["endpoint"], params)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    log.info("%s %04d-%02d: %s raw rows", table, year, month, f"{len(df):,}")
    return df


# --------------------------------------------------------------------------- #
# Cleaning
# --------------------------------------------------------------------------- #
def hs_revision(year: int) -> str:
    """Harmonized System revision in force in a given year."""
    for lo, hi, name in constants.HS_REVISIONS.value:
        if lo <= year <= hi:
            return name
    raise ValueError(f"no HS revision mapped for year {year}")


def _to_null(series: pd.Series) -> pd.Series:
    return series.replace(list(constants.MISSING_TOKENS.value), pd.NA)


def load_country_iso2(schedule_c_text: str) -> dict[str, str]:
    """Parse Schedule C into a Census country code -> ISO2 map.

    The file is a fixed pipe-delimited listing whose header and rule lines are
    skipped. Codes with no ISO country (unidentified areas) are omitted, so the
    resulting ``country_iso2_code`` is null for them.
    """
    out: dict[str, str] = {}
    for line in schedule_c_text.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue
        code, _name, iso = parts[0], parts[1], parts[2]
        if not (len(code) == 4 and code.isdigit()):
            continue
        if len(iso) == 2 and iso.isalpha():
            out[code] = iso.upper()
    if not out:
        raise ValueError("Schedule C parsed to zero country codes")
    return out


def clean_table(
    raw: pd.DataFrame, table: str, iso2_by_code: dict[str, str]
) -> pd.DataFrame:
    """Map raw API columns onto the architecture schema for one table.

    Applies, in order: the ``COMM_LVL`` / ``SUMMARY_LVL`` filters that keep the
    grain honest, the derived columns, numeric coercion, and a collapse onto the
    declared grain.
    """
    if raw.empty:
        return pd.DataFrame(columns=[a["name"] for a in read_arch(table)])

    spec = TABLE_SPECS[table]
    df = raw.copy()

    # The API returns HS2, HS4, HS6 and HS10 rows together, and detail rows
    # alongside country GROUPINGS. Both filters are applied as predicates too;
    # repeating them here is the guard that matters, because either one going
    # missing inflates every total rather than raising.
    before = len(df)
    if "COMM_LVL" in df:
        df = df[df["COMM_LVL"] == constants.COMM_LVL.value]
    if "SUMMARY_LVL" in df:
        df = df[df["SUMMARY_LVL"] == constants.SUMMARY_LVL.value]
    if len(df) != before:
        log.info(
            "%s: %s of %s rows kept after COMM_LVL/SUMMARY_LVL filters",
            table,
            f"{len(df):,}",
            f"{before:,}",
        )
    if df.empty:
        return pd.DataFrame(columns=[a["name"] for a in read_arch(table)])

    arch = read_arch(table)
    commodity_src = (
        "I_COMMODITY" if table.startswith("import") else "E_COMMODITY"
    )
    out = pd.DataFrame(index=df.index)

    for row in arch:
        name, btype, src = (
            row["name"],
            row["bigquery_type"],
            row["original_name"],
        )
        if name == "year":
            out[name] = pd.to_numeric(df["YEAR"], errors="coerce")
        elif name == "month":
            out[name] = pd.to_numeric(df["MONTH"], errors="coerce")
        elif name == "country_iso2_code":
            out[name] = df["CTY_CODE"].str.strip().map(iso2_by_code)
        elif name == "hs6_code":
            out[name] = df[commodity_src].str.strip().str.zfill(6)
        elif name == "hs4_code":
            out[name] = df[commodity_src].str.strip().str.zfill(6).str[:4]
        elif name == "hs2_code":
            out[name] = df[commodity_src].str.strip().str.zfill(6).str[:2]
        elif name == "hs_revision":
            out[name] = pd.to_numeric(df["YEAR"], errors="coerce").map(
                lambda y: hs_revision(int(y)) if pd.notna(y) else pd.NA
            )
        elif name == "state_id":
            out[name] = df["STATE"].str.strip().str.upper().map(STATE_FIPS)
        elif name == "district_code":
            out[name] = df["DISTRICT"].str.strip().str.zfill(2)
        elif name == "port_code":
            out[name] = df["PORT"].str.strip().str.zfill(4)
        elif name == "country_code":
            out[name] = df["CTY_CODE"].str.strip().str.zfill(4)
        elif src and src in df.columns:
            if btype in ("INT64", "FLOAT64"):
                out[name] = pd.to_numeric(_to_null(df[src]), errors="coerce")
            else:
                out[name] = _to_null(df[src].str.strip())
        else:
            raise KeyError(
                f"{table}.{name}: architecture names source {src!r}, absent "
                f"from the API response (columns: {sorted(df.columns)})"
            )

    return collapse_to_grain(out, table, spec["grain"], arch)


def collapse_to_grain(
    df: pd.DataFrame, table: str, grain: list[str], arch: list[dict]
) -> pd.DataFrame:
    """Sum additive measures onto the declared grain.

    The import endpoints carry ``RP`` (rate provision) and ``CTY_SUBCODE`` as
    required predicates, which may split a key into several rows. Summing is
    the arithmetically correct operation for the value, quantity, duty and
    weight columns; unit columns are non-additive and take the first non-null
    value, which is constant within a commodity.

    A collapse that changes nothing is the expected case and is not an error --
    it is logged so the ratio is visible on every run.
    """
    if df.empty:
        return df
    order = [a["name"] for a in arch]
    measures = [c for c in order if c not in grain]
    non_additive = [
        c for c in measures if c.endswith(NON_ADDITIVE_SUFFIXES)
    ] + [c for c in measures if c in DERIVED or c in ("country_iso2_code",)]
    additive = [c for c in measures if c not in non_additive]

    if not df.duplicated(subset=grain).any():
        return df[order]

    before = len(df)
    grouped = df.groupby(grain, dropna=False, sort=False)
    # min_count=1 keeps an all-missing group missing. Plain sum() would return
    # 0, silently converting "Census published nothing here" into "Census
    # published a zero" -- the two are different, and the API distinguishes
    # them with its own true-zero flags.
    parts = [grouped[additive].sum(min_count=1)] if additive else []
    if non_additive:
        parts.append(grouped[non_additive].first())
    out = pd.concat(parts, axis=1).reset_index()
    log.info(
        "%s: collapsed %s rows to %s on grain %s",
        table,
        f"{before:,}",
        f"{len(out):,}",
        grain,
    )
    return out[order]


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def open_writers(table: str, output_dir: Path) -> dict:
    """State for a streaming per-year parquet write. See ``write_month``."""
    return {"table": table, "output_dir": Path(output_dir), "writers": {}}


def write_month(state: dict, df: pd.DataFrame) -> None:
    """Append one month to its year's parquet file as an all-STRING row group.

    Staging is all-STRING by Data Basis convention: ``pipelines.utils.gcs
    .dump_header`` stringifies the header file BigQuery infers the staging
    schema from, so typed parquet is rejected outright. The dbt model
    ``safe_cast``s each column back to its architecture type.

    Values pass through the architecture's real types FIRST and are only then
    cast to string via arrow -- never ``astype(str)``, which renders a NULL as
    the literal ``"nan"`` that ``safe_cast`` will not turn back into NULL, and
    would serialize ``year`` as ``"2024.0"``.

    One file per year, written as successive row groups, so a partition refresh
    replaces exactly one object per year and never leaves a stale part behind.
    Empty months are skipped: a zero-row first partition would make
    ``dump_header`` infer the wrong staging schema.
    """
    if df.empty:
        return
    table = state["table"]
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed = pa.schema(
        [pa.field(a["name"], PA_TYPES[a["bigquery_type"]]) for a in arch]
    )
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])

    for year, group in df[order].groupby("year", sort=True):
        year = int(year)
        writer = state["writers"].get(year)
        if writer is None:
            pdir = state["output_dir"] / table / f"year={year}"
            pdir.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(
                pdir / "data.parquet", strings, compression="snappy"
            )
            state["writers"][year] = writer
        at = pa.Table.from_pandas(group, schema=typed, preserve_index=False)
        writer.write_table(at.cast(strings))


def close_writers(state: dict) -> Path:
    for writer in state["writers"].values():
        writer.close()
    return state["output_dir"] / state["table"]


# --------------------------------------------------------------------------- #
# Dictionary
# --------------------------------------------------------------------------- #
HS_REVISION_LABELS = {
    "HS2007": "Harmonized System 2007 revision, in force 2010-2011",
    "HS2012": "Harmonized System 2012 revision, in force 2012-2016",
    "HS2017": "Harmonized System 2017 revision, in force 2017-2021",
    "HS2022": "Harmonized System 2022 revision, in force from 2022",
}

DF_LABELS = {
    "1": "Domestic exports, of goods grown, produced or manufactured in the United States",
    "2": "Foreign exports, that is re-exports of goods of foreign origin",
}


def parse_schedule_c(text: str) -> list[tuple[str, str]]:
    """Return (country code, country name) pairs from Schedule C."""
    out = []
    for line in text.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 2 and len(parts[0]) == 4 and parts[0].isdigit():
            out.append((parts[0], parts[1]))
    if not out:
        raise ValueError("Schedule C parsed to zero country codes")
    return out


def parse_schedule_d(
    text: str,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Return (district code, name) and (port code, name) pairs from Schedule D.

    The file lists a district line with its code in the first column, then its
    ports with their four-digit codes in the second.
    """
    districts, ports = [], []
    for line in text.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue
        dist, port, name = parts[0], parts[1], parts[2]
        if not name or name.startswith("-"):
            continue
        if len(dist) == 2 and dist.isdigit():
            districts.append((dist, name))
        elif len(port) == 4 and port.isdigit():
            ports.append((port, name))
    if not districts or not ports:
        raise ValueError(
            f"Schedule D parsed to {len(districts)} districts and "
            f"{len(ports)} ports; expected both to be non-empty"
        )
    return districts, ports


def build_dicionario(schedule_c: str, schedule_d: str) -> pd.DataFrame:
    """Build the dicionario table from the published Census code schedules.

    Built from Schedule C and Schedule D rather than from the values observed
    in a window of the facts, so the dictionary is complete rather than limited
    to whatever traded in the months last downloaded. The two schedules are the
    Census Bureau's own authoritative code lists and need no API key.
    """
    countries = parse_schedule_c(schedule_c)
    districts, ports = parse_schedule_d(schedule_d)

    rows: list[dict] = []

    def add(tables, column, pairs):
        for table in tables:
            for key, value in pairs:
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": value,
                    }
                )

    add(FACT_TABLES, "country_code", countries)
    add(("import", "export"), "district_code", districts)
    add(("import_port", "export_port"), "port_code", ports)
    add(FACT_TABLES, "hs_revision", sorted(HS_REVISION_LABELS.items()))
    add(("export",), "domestic_foreign_code", sorted(DF_LABELS.items()))

    out = pd.DataFrame(
        rows,
        columns=[
            "id_tabela",
            "nome_coluna",
            "chave",
            "cobertura_temporal",
            "valor",
        ],
    )
    dupes = out.duplicated(subset=["id_tabela", "nome_coluna", "chave"])
    if dupes.any():
        raise ValueError(
            f"dicionario has {int(dupes.sum())} duplicate (table, column, key) rows"
        )
    return out


def write_dicionario(df: pd.DataFrame, output_dir: Path) -> Path:
    """Write the dicionario as a single unpartitioned all-STRING parquet."""
    schema = pa.schema([pa.field(c, pa.string()) for c in df.columns])
    tdir = Path(output_dir) / "dicionario"
    tdir.mkdir(parents=True, exist_ok=True)
    at = pa.Table.from_pandas(
        df.astype("string"), schema=schema, preserve_index=False
    )
    pq.write_table(at, tdir / "data.parquet", compression="snappy")
    log.info("dicionario: %s rows -> %s", f"{len(df):,}", tdir)
    return tdir


def download_schedules() -> tuple[str, str]:
    """Fetch Schedule C and Schedule D. No API key required for these."""
    out = []
    for url in (
        constants.SCHEDULE_C_URL.value,
        constants.SCHEDULE_D_URL.value,
    ):
        req = urllib.request.Request(url, headers=constants.HTTP_HEADERS.value)
        with urllib.request.urlopen(req, timeout=120) as resp:
            text = resp.read().decode("utf-8", "replace")
        if "Request Rejected" in text:
            raise RuntimeError(
                f"{url} returned the census.gov firewall page, not the schedule"
            )
        out.append(text)
    return out[0], out[1]


# --------------------------------------------------------------------------- #
# Orchestration helpers
# --------------------------------------------------------------------------- #
def latest_available_month(
    probe_table: str = "import", look_back: int = 6
) -> str:
    """Return the newest published month as ``YYYY-MM``.

    The API exposes no "latest" endpoint, so this walks back from the current
    month until a request returns rows. Census publishes a reference month
    roughly five weeks later, so the newest month is normally one or two behind.

    Raises:
        CensusAPIError: If no month in the look-back window has data, which
            means the API changed shape rather than that trade stopped.
    """
    today = pd.Timestamp.utcnow().normalize()
    for back in range(look_back + 1):
        stamp = (today - pd.DateOffset(months=back)).to_period("M")
        params = {
            "get": "YEAR,MONTH",
            "COMM_LVL": constants.COMM_LVL.value,
            "time": str(stamp),
        }
        try:
            rows = _get(
                TABLE_SPECS[probe_table]["endpoint"], params, timeout=180
            )
        except CensusAPIError as exc:
            log.info("probe %s: %s", stamp, exc)
            continue
        if len(rows) > 1:
            log.info("latest published month: %s", stamp)
            return str(stamp)
    raise CensusAPIError(
        f"no published month found in the {look_back} months before "
        f"{today:%Y-%m}; the API contract may have changed"
    )


def months_between(first: str, last: str) -> list[tuple[int, int]]:
    """Inclusive list of (year, month) from ``YYYY-MM`` to ``YYYY-MM``."""
    rng = pd.period_range(first, last, freq="M")
    return [(p.year, p.month) for p in rng]


def refresh_window(latest: str, years_back: int | None = None) -> str:
    """First month of the refresh window, given the newest published month.

    Census revises year-to-date months at every release and revises all
    previously released data with the publication of April statistics. Taking
    January of ``years_back`` years before the newest month covers both,
    without special-casing April.
    """
    if years_back is None:
        years_back = constants.REFRESH_YEARS_BACK.value
    year = int(latest[:4]) - years_back
    return f"{max(year, constants.FIRST_YEAR.value):04d}-01"


def harvest(
    tables: list[str],
    first_month: str,
    last_month: str,
    output_dir: Path,
    iso2_by_code: dict[str, str],
) -> dict[str, Path]:
    """Download, clean and write every requested table over a month range.

    One month is held in memory at a time and appended to its year's parquet as
    a row group, so peak memory tracks the largest single month rather than the
    whole range.

    Returns the output directory per table, ready for ``upload_to_gcs``.
    """
    output_dir = Path(output_dir)
    months = months_between(first_month, last_month)
    out: dict[str, Path] = {}
    for table in tables:
        state = open_writers(table, output_dir)
        total = 0
        for year, month in months:
            raw = fetch_month(table, year, month)
            if raw.empty:
                log.info("%s %04d-%02d: no rows published", table, year, month)
                continue
            clean = clean_table(raw, table, iso2_by_code)
            if clean.empty:
                continue
            total += len(clean)
            write_month(state, clean)
        path = close_writers(state)
        if total == 0:
            raise CensusAPIError(
                f"{table}: harvest produced zero rows over {first_month}..{last_month}. "
                "An empty harvest is never uploaded -- an empty or missing first "
                "partition makes dump_header infer the wrong staging schema."
            )
        log.info("%s: %s rows total -> %s", table, f"{total:,}", path)
        out[table] = path
    return out
