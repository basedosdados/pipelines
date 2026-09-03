"""Pure download + cleaning functions for us_epa_ghgrp (no Prefect imports).

Shared by the recurring Prefect pipeline (``flows.py``) and the one-shot
bootstrap (``models/us_epa_ghgrp/code/clean_data.py``), so the transform exists
in exactly one place.

Source: the Envirofacts GHG REST API, which publishes the FLIGHT data model —
``pub_dim_facility`` (one row per facility-year), ``pub_facts_subp_ghg_emission``
(facility x year x subpart x gas) and ``pub_facts_sector_ghg_emission``
(facility x year x sector x subsector x gas), plus four small dimension tables.
"""

import csv
import datetime as dt
import io
import logging
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_epa_ghgrp.constants import constants

log = logging.getLogger("us_epa_ghgrp")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}

_ARCH = constants.ARCHITECTURE_DIR.value
_UA = {"User-Agent": constants.USER_AGENT.value}
_API = constants.API_BASE.value
_DIMS = constants.DIM_TABLES.value
_FACTS = constants.FACT_TABLES.value
_CHUNK = constants.ROW_CHUNK.value
_FIRST_YEAR = constants.FIRST_YEAR.value
_STATE_FIPS = constants.STATE_FIPS.value

# Reporter type of a subpart or sector, from the API dimension tables.
_REPORTER_TYPE_LABELS = {
    "E": "Direct emitter: greenhouse gases emitted at the facility",
    "S": (
        "Supplier: greenhouse gas quantity associated with the fuels or "
        "industrial gases supplied, not emissions at the facility"
    ),
    "I": "CO2 injection: carbon dioxide received for injection or sequestered",
}

# Labels for the coded facility columns that have no dimension table in the API.
_FACILITY_LABELS = {
    "reporting_status": {
        "STOPPED_REPORTING_VALID_REASON": (
            "Facility stopped reporting for a valid reason (e.g. emissions fell "
            "below the threshold or the facility closed) and did not submit a "
            "report for the year"
        ),
        "STOPPED_REPORTING_UNKNOWN_REASON": (
            "Facility stopped reporting without a known reason and did not "
            "submit a report for the year"
        ),
    },
    "cems_used": {
        "Y": "Yes, the facility uses continuous emissions monitoring (CEMS)"
    },
    "co2_captured": {
        "Y": (
            "Yes, some CO2 is collected on site and used to manufacture other "
            "products, so it is not emitted from the affected process units "
            "(reported under subpart G or S)"
        )
    },
    "co2_supplied": {
        "Y": (
            "Yes, some CO2 reported as emissions under subpart AA, G or P is "
            "collected and transferred off site or injected (reported under "
            "subpart PP)"
        )
    },
}


# ── architecture ────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Column order and BigQuery types come from here, never from the raw files, so
    the pipeline and the one-shot bootstrap cannot drift apart.

    Args:
        table: Table slug (e.g. ``"facility"``), matching the CSV filename.

    Returns:
        One dict per column, in architecture order.
    """
    with open(_ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ── download ────────────────────────────────────────────────────────────────
def _get(url: str, tries: int = 4) -> requests.Response:
    """GET with retries; Envirofacts occasionally returns an empty 200."""
    last: requests.Response | None = None
    for attempt in range(tries):
        last = requests.get(url, headers=_UA, timeout=600)
        if last.ok and last.text.strip():
            return last
        time.sleep(5 * (attempt + 1))
    status = last.status_code if last is not None else None
    raise RuntimeError(f"Envirofacts request failed ({status}): {url}")


def _count(table: str, year: int | None = None) -> int:
    """Row count of an API table, optionally restricted to one reporting year."""
    url = f"{_API}/{table}" + (f"/year/{year}" if year else "") + "/count/JSON"
    return int(_get(url).json()[0]["TOTALQUERYRESULTS"])


def source_max_year() -> str:
    """Latest reporting year present in the facility table, as ``"YYYY"``.

    Walks back from the current year until a year with rows is found. This is
    what the source poll compares against the registered coverage, and it costs
    a handful of tiny count requests rather than a full download.
    """
    for year in range(dt.date.today().year, _FIRST_YEAR - 1, -1):
        if _count("pub_dim_facility", year) > 0:
            return f"{year:04d}"
    raise RuntimeError("Envirofacts returned no facility rows for any year")


def _download_fact_table(table: str, input_dir: Path) -> list[Path]:
    """Fetch one large API table, one reporting year at a time, as CSV.

    Each year is requested in ``ROW_CHUNK`` row windows and the row count is
    asserted against the API's own count, so a truncated or partial response
    fails loudly rather than shipping a short year.
    """
    paths = []
    total, got = _count(table), 0
    for year in range(_FIRST_YEAR, dt.date.today().year + 1):
        n = _count(table, year)
        if n == 0:
            continue
        parts = []
        for start in range(0, n, _CHUNK):
            r = _get(
                f"{_API}/{table}/year/{year}/rows/{start}:{start + _CHUNK - 1}/CSV"
            )
            text = r.text
            if start > 0:  # drop the repeated header
                text = text.split("\n", 1)[1]
            parts.append(text.rstrip("\n") + "\n")
        dest = input_dir / f"{table}_{year}.csv"
        dest.write_text("".join(parts), encoding="utf-8")
        rows = sum(1 for _ in csv.reader(io.StringIO(dest.read_text()))) - 1
        if rows != n:
            raise RuntimeError(
                f"{table} {year}: expected {n} rows, got {rows}"
            )
        log.info(f"downloaded {table} {year}: {rows:,} rows")
        got += rows
        paths.append(dest)
    if got != total:
        raise RuntimeError(
            f"{table}: expected {total} rows in total, got {got}"
        )
    return paths


def download_all(input_dir: Path) -> Path:
    """Download every GHG API table needed to build the dataset.

    Args:
        input_dir: Directory to download into; files land in ``<input_dir>/api``.

    Returns:
        The ``api`` directory holding the CSVs.
    """
    api_dir = input_dir / "api"
    api_dir.mkdir(parents=True, exist_ok=True)
    for table in _DIMS:
        dest = api_dir / f"{table}.csv"
        dest.write_text(_get(f"{_API}/{table}/CSV").text, encoding="utf-8")
        log.info(f"downloaded {table}")
    for table in _FACTS:
        _download_fact_table(table, api_dir)
    return api_dir


# ── read ────────────────────────────────────────────────────────────────────
def _read_dim(api_dir: Path, table: str) -> pd.DataFrame:
    return pd.read_csv(api_dir / f"{table}.csv", dtype=str)


def _read_fact(api_dir: Path, table: str) -> pd.DataFrame:
    files = sorted(api_dir.glob(f"{table}_*.csv"))
    if not files:
        raise FileNotFoundError(f"no {table}_<year>.csv under {api_dir}")
    return pd.concat(
        [pd.read_csv(f, dtype=str, keep_default_na=True) for f in files],
        ignore_index=True,
    )


def _strip_html(s: pd.Series) -> pd.Series:
    return s.str.replace(r"</?sub>", "", regex=True)


def _lookup(keys: pd.Series, values: pd.Series) -> dict[str, str]:
    return dict(zip(keys, values, strict=True))


def _dims(api_dir: Path) -> dict[str, dict[str, str]]:
    """Id -> code maps for the four dimension tables, plus code -> label maps."""
    sector = _read_dim(api_dir, "pub_dim_sector")
    subsector = _read_dim(api_dir, "pub_dim_subsector")
    ghg = _read_dim(api_dir, "pub_dim_ghg")
    subpart = _read_dim(api_dir, "pub_dim_subpart")
    return {
        "sector_id": _lookup(sector.sector_id, sector.sector_code),
        "subsector_id": _lookup(
            subsector.subsector_id, subsector.subsector_name
        ),
        "gas_id": _lookup(ghg.gas_id, ghg.gas_code),
        "subpart_id": _lookup(subpart.subpart_id, subpart.subpart_name),
        # code -> label, for the dictionary
        "sector": _lookup(sector.sector_code, sector.sector_name),
        "subsector": _lookup(
            subsector.subsector_name, subsector.subsector_desc
        ),
        "gas": _lookup(ghg.gas_code, _strip_html(ghg.gas_name)),
        "subpart": _lookup(
            subpart.subpart_name, _strip_html(subpart.subpart_category)
        ),
        # code -> reporter type (E direct emitter, S supplier, I CO2 injection)
        "subpart_type": _lookup(subpart.subpart_name, subpart.subpart_type),
        "sector_type": _lookup(sector.sector_code, sector.sector_type),
    }


# ── coerce ──────────────────────────────────────────────────────────────────
def _coerce(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Select and order the architecture columns and apply their pandas dtypes."""
    arch = read_arch(table)
    out = df[[a["name"] for a in arch]].copy()
    for a in arch:
        col, typ = a["name"], a["bigquery_type"]
        if typ == "INT64":
            out[col] = pd.to_numeric(out[col], errors="raise").astype("Int64")
        elif typ == "FLOAT64":
            out[col] = pd.to_numeric(out[col], errors="raise").astype(
                "Float64"
            )
        else:
            out[col] = out[col].astype("string").str.strip()
            out[col] = out[col].mask(out[col] == "", pd.NA)
    return out


def _map_codes(
    df: pd.DataFrame, column: str, mapping: dict[str, str]
) -> pd.Series:
    """Replace dimension ids by their codes; an unknown id is a hard error."""
    unknown = set(df[column].dropna()) - set(mapping)
    if unknown:
        raise ValueError(
            f"{column}: ids without a dimension row: {sorted(unknown)}"
        )
    return df[column].map(mapping)


# ── build ───────────────────────────────────────────────────────────────────
def build_facility(api_dir: Path) -> pd.DataFrame:
    """One row per facility-year, from ``pub_dim_facility``.

    Facilities that stopped reporting are carried by the source with
    ``reporting_status`` set and no submission for the year; they are kept, so
    a user can tell "did not report" from "not in the program".
    """
    raw = _read_fact(api_dir, "pub_dim_facility")
    unknown = set(raw["state"].dropna()) - set(_STATE_FIPS)
    if unknown:
        raise ValueError(
            f"state abbreviations without a FIPS code: {sorted(unknown)}"
        )
    df = pd.DataFrame(
        {
            "year": raw["year"],
            "facility_id": raw["facility_id"],
            "frs_id": raw["frs_id"],
            "state_id": raw["state"].map(_STATE_FIPS),
            "county_id": raw["county_fips"].str.zfill(5),
            "naics_id": raw["naics_code"],
            "facility_name": raw["facility_name"],
            "parent_company": raw["parent_company"],
            "facility_type": raw["facility_types"],
            "industry_type": raw["reported_industry_types"],
            "reporting_status": raw["reporting_status"],
            "state_abbreviation": raw["state"],
            "county_name": raw["county"],
            "city": raw["city"],
            "zip_code": raw["zip"],
            "address": raw["address1"]
            .str.cat(raw["address2"], sep=", ", na_rep="")
            .str.strip(", ")
            .replace("", pd.NA),
            "latitude": raw["latitude"],
            "longitude": raw["longitude"],
            "cems_used": raw["cems_used"],
            "co2_captured": raw["co2_captured"],
            "co2_supplied": raw["emitted_co2_supplied"],
        }
    )
    # A dozen ZIP codes lost their leading zero upstream (4 characters).
    df["zip_code"] = df["zip_code"].str.zfill(5)
    # ~0.5% of county FIPS codes sit in a different state from the reported
    # state (corporate-office counties on basin-level and supplier reporters).
    # Both are kept as reported; the disagreement is documented, not "fixed".
    mismatch = df.dropna(subset=["county_id"])
    mismatch = mismatch[mismatch["county_id"].str[:2] != mismatch["state_id"]]
    log.info(
        f"facility: {len(mismatch):,} rows with county_id outside state_id"
    )
    df = _coerce(df, "facility")
    _assert_unique(df, "facility", ["year", "facility_id"])
    return df


def build_emission_subpart(api_dir: Path, dims: dict) -> pd.DataFrame:
    """Facility x year x subpart x gas, from ``pub_facts_subp_ghg_emission``."""
    raw = _read_fact(api_dir, "pub_facts_subp_ghg_emission")
    df = pd.DataFrame(
        {
            "year": raw["year"],
            "facility_id": raw["facility_id"],
            "subpart": _map_codes(raw, "sub_part_id", dims["subpart_id"]),
            "gas": _map_codes(raw, "gas_id", dims["gas_id"]),
            "emission": raw["co2e_emission"],
        }
    )
    df["subpart_type"] = _map_codes(df, "subpart", dims["subpart_type"])
    df = _coerce(df, "emission_subpart")
    _assert_unique(
        df, "emission_subpart", ["year", "facility_id", "subpart", "gas"]
    )
    return df


def build_emission_sector(api_dir: Path, dims: dict) -> pd.DataFrame:
    """Facility x year x sector x subsector x gas, from ``pub_facts_sector_ghg_emission``."""
    raw = _read_fact(api_dir, "pub_facts_sector_ghg_emission")
    df = pd.DataFrame(
        {
            "year": raw["year"],
            "facility_id": raw["facility_id"],
            "sector": _map_codes(raw, "sector_id", dims["sector_id"]),
            "subsector": _map_codes(raw, "subsector_id", dims["subsector_id"]),
            "gas": _map_codes(raw, "gas_id", dims["gas_id"]),
            "emission": raw["co2e_emission"],
        }
    )
    df["sector_type"] = _map_codes(df, "sector", dims["sector_type"])
    df = _coerce(df, "emission_sector")
    # ~26k rows carry neither a gas nor a value: sector-membership placeholders
    # for facilities that did not report that year. They hold no data.
    placeholder = df["gas"].isna() & df["emission"].isna()
    if placeholder.any():
        log.info(
            f"emission_sector: dropping {int(placeholder.sum()):,} gas/value-less placeholder rows"
        )
        df = df[~placeholder].reset_index(drop=True)
    # The source publishes ~80 keys as two rows (a zero placeholder beside the
    # value, or two components of one sector total). Summing them reproduces the
    # facility totals of the subpart table exactly, so the rows are summed;
    # a key whose rows are all null stays null (min_count=1).
    key = ["year", "facility_id", "sector", "subsector", "gas"]
    dup = int(df.duplicated(subset=key).sum())
    if dup:
        log.info(f"emission_sector: summing {dup:,} duplicate-key rows")
        df = (
            df.groupby([*key, "sector_type"], dropna=False, sort=False)[
                "emission"
            ]
            .sum(min_count=1)
            .reset_index()
        )
        df = _coerce(df, "emission_sector")
    _assert_unique(df, "emission_sector", key)
    return df


def build_dicionario(
    frames: dict[str, pd.DataFrame], dims: dict
) -> pd.DataFrame:
    """Code -> label table for every dictionary-covered column.

    Only the codes that actually occur in each table are emitted, so the
    dictionary covers the data exactly rather than the union of every level.
    """
    labels = {
        "facility": _FACILITY_LABELS,
        "emission_subpart": {
            "subpart": dims["subpart"],
            "subpart_type": _REPORTER_TYPE_LABELS,
            "gas": dims["gas"],
        },
        "emission_sector": {
            "sector": dims["sector"],
            "sector_type": _REPORTER_TYPE_LABELS,
            "subsector": dims["subsector"],
            "gas": dims["gas"],
        },
    }
    rows = []
    for table, columns in labels.items():
        df = frames[table]
        for column, mapping in columns.items():
            for key in sorted(df[column].dropna().unique()):
                if key not in mapping:
                    raise ValueError(
                        f"{table}.{column}: no label for code {key!r}"
                    )
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": key,
                        "cobertura_temporal": "",
                        "valor": mapping[key],
                    }
                )
    return _coerce(pd.DataFrame(rows), "dicionario")


def _assert_unique(df: pd.DataFrame, table: str, key: list[str]) -> None:
    dup = df.duplicated(subset=key).sum()
    if dup:
        raise ValueError(f"{table}: {dup:,} duplicate rows on {key}")


# ── write ───────────────────────────────────────────────────────────────────
def write_partitioned(df: pd.DataFrame, table: str, output_dir: Path) -> Path:
    """Write a table as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column to its real type, and ``pipelines.utils.gcs.dump_header``
    stringifies the header file that BigQuery infers the staging schema from.
    Emitting typed parquet against that STRING schema makes BigQuery reject the
    files ("Parquet column ... does not match the target cpp_type STRING_PIECE").

    Values pass through the architecture's real types first, so ``year``
    serializes as ``"2010"`` rather than ``"2010.0"``, and only then cast to
    string via arrow — never ``astype(str)``, which would render a NULL as the
    literal ``"nan"`` and defeat the dbt ``safe_cast``.

    ``dicionario`` has no year column and is written unpartitioned.

    Args:
        df: Cleaned frame in architecture column order.
        table: Table slug, used for the architecture lookup and output path.
        output_dir: Root output directory.

    Returns:
        The table's directory, ``<output_dir>/<table>/year=<YYYY>/data.parquet``.
    """
    arch = read_arch(table)
    typed = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    tdir = output_dir / table
    tdir.mkdir(parents=True, exist_ok=True)

    def _write(frame: pd.DataFrame, dest: Path) -> None:
        at = pa.Table.from_pandas(frame, schema=typed, preserve_index=False)
        pq.write_table(at.cast(strings), dest, compression="snappy")

    if "year" not in df.columns:
        _write(df, tdir / "data.parquet")
    else:
        for year, g in df.groupby("year", sort=True):
            # pyrefly: ignore [bad-argument-type]
            pdir = tdir / f"year={int(year)}"
            pdir.mkdir(parents=True, exist_ok=True)
            _write(g, pdir / "data.parquet")
    log.info(f"{table}: {len(df):,} rows -> {tdir}")
    return tdir


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Clean every API table and write the partitioned parquet output.

    Args:
        input_dir: Directory holding the ``api/`` CSVs from :func:`download_all`.
        output_dir: Root output directory for the parquet tables.

    Returns:
        ``{"paths": {table: str}, "counts": {table: int}, "max_year": "YYYY"}``.
        Paths are strings so Prefect can serialize the task result.
    """
    api_dir = input_dir / "api"
    output_dir.mkdir(parents=True, exist_ok=True)
    dims = _dims(api_dir)
    frames = {
        "facility": build_facility(api_dir),
        "emission_subpart": build_emission_subpart(api_dir, dims),
        "emission_sector": build_emission_sector(api_dir, dims),
    }
    frames["dicionario"] = build_dicionario(frames, dims)
    paths, counts = {}, {}
    for table in constants.TABLES.value:
        paths[table] = str(write_partitioned(frames[table], table, output_dir))
        counts[table] = len(frames[table])
    max_year = int(frames["facility"]["year"].max())
    return {"paths": paths, "counts": counts, "max_year": f"{max_year:04d}"}
