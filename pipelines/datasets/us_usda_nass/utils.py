"""Pure transform for us_usda_nass — no Prefect imports (DRY with the bootstrap).

The recurring flow (``tasks.py``) and the one-shot onboarding bootstrap
(``models/us_usda_nass/code/clean.py``) both import from here.

Design:
- Stream each ``.gz`` sector file line by line (never load a whole file), apply
  the seed filter in one pass, and route each kept row to a per-grain table by
  SOURCE_DESC + AGG_LEVEL_DESC (survey_national/state/agricultural_district/county
  and census_of_agriculture_national/state/county). Each table carries only its
  grain's columns (the architecture CSV per table defines the projection).
- Value parsing happens HERE, not in dbt: ``VALUE`` uses thousands-commas and
  suppression sentinels ((D), (Z), …). ``safe_cast('21,000' as float64)`` yields
  NULL, so we strip commas and map sentinels to NULL + a flag before writing.
- Staging parquet is ALL-STRING (arrow cast, never ``astype(str)``); ``year`` is
  written as ``"2007"`` (through its real type first).
- Memory is bounded: rows accumulate in per-(table, year) buffers and are flushed
  to numbered part files, then coalesced to one file per year at the end.
"""

from __future__ import annotations

import csv
import gzip
import re
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_usda_nass.constants import constants

_IDX = {name: i for i, name in enumerate(constants.RAW_COLUMNS.value)}
_ARCH_DIR = Path(constants.ARCHITECTURE_DIR.value)
_FLUSH_ROWS = 500_000

# AGG_LEVEL_DESC -> table suffix.
_GEO_SUFFIX = {
    "NATIONAL": "national",
    "STATE": "state",
    "AGRICULTURAL DISTRICT": "agricultural_district",
    "COUNTY": "county",
}
_SOURCE_BASE = {"SURVEY": "survey", "CENSUS": "census_of_agriculture"}

# Declared published fact tables (see build_architecture.FACT_TABLES). A
# (source, geo) combo not declared here is dropped (e.g. census has no
# agricultural-district grain in QuickStats).
_FACT_TABLES = frozenset(constants.FACT_TABLES.value)


def _read_arch_order(table: str) -> list[str]:
    """Read a table's column order from its architecture CSV.

    Args:
        table: Fact-table slug (an architecture CSV ``<table>.csv`` must exist).

    Returns:
        The column names, in architecture order.
    """
    with open(_ARCH_DIR / f"{table}.csv", encoding="utf-8") as f:
        return [r["name"] for r in csv.DictReader(f)]


# table -> (column order, all-string arrow schema), cached.
_ARCH: dict[str, tuple[list[str], pa.Schema]] = {}


def _arch(table: str) -> tuple[list[str], pa.Schema]:
    """Return a table's ``(column order, all-string arrow schema)``, cached.

    Args:
        table: Fact-table slug.

    Returns:
        A tuple of the architecture column order and a matching all-string
        :class:`pyarrow.Schema` (staging is all-STRING by house convention).
    """
    if table not in _ARCH:
        order = _read_arch_order(table)
        _ARCH[table] = (
            order,
            pa.schema([pa.field(n, pa.string()) for n in order]),
        )
    return _ARCH[table]


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #
def resolve_bulk_urls() -> dict[str, str]:
    """Scrape the NASS datasets index for the current dated sector filenames."""
    index = constants.DATASETS_INDEX.value
    html = requests.get(
        index, headers={"User-Agent": constants.USER_AGENT.value}, timeout=120
    ).text
    urls: dict[str, str] = {}
    for sector in constants.SECTORS.value:
        m = re.search(rf"qs\.{re.escape(sector)}_(\d{{8}})\.txt\.gz", html)
        if not m:
            raise RuntimeError(
                f"bulk file for sector {sector!r} not on {index}"
            )
        urls[sector] = index + m.group(0)
    return urls


def download_bulk(input_dir: Path) -> list[Path]:
    """Download the 5 QuickStats bulk sector files into ``input_dir``."""
    input_dir.mkdir(parents=True, exist_ok=True)
    urls = resolve_bulk_urls()
    paths: list[Path] = []
    headers = {"User-Agent": constants.USER_AGENT.value}
    for sector, url in urls.items():
        dest = input_dir / f"qs.{sector}.txt.gz"
        with requests.get(url, headers=headers, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
        paths.append(dest)
        print(f"  downloaded {dest.name} ({dest.stat().st_size / 1e6:.0f} MB)")
    return paths


# --------------------------------------------------------------------------- #
# Value parsing
# --------------------------------------------------------------------------- #
def clean_value(raw: str) -> tuple[str | None, str | None]:
    """Parse a raw ``VALUE`` into (numeric_string_or_None, suppression_flag)."""
    s = raw.strip()
    if s == "":
        return None, None
    if s.startswith("("):
        return None, s
    s2 = s.replace(",", "")
    try:
        float(s2)
    except ValueError:
        return None, None
    return s2, None


def clean_cv(raw: str) -> str | None:
    """Parse ``CV_%`` into a numeric string or None."""
    s = raw.strip()
    if s == "" or s.startswith("("):
        return None
    s2 = s.replace(",", "")
    try:
        float(s2)
    except ValueError:
        return None
    return s2


# --------------------------------------------------------------------------- #
# Row filter + build
# --------------------------------------------------------------------------- #
def _seed_keep(f: list[str]) -> bool:
    """Return whether a raw row matches the curated seed (commodity/geo/freq/stat).

    Args:
        f: The raw row split into fields, indexed by :data:`_IDX`.

    Returns:
        True if the row's commodity, geography level, frequency and statistic
        category all fall inside the seed scope.
    """
    if f[_IDX["COMMODITY_DESC"]] not in constants.SEED_COMMODITIES.value:
        return False
    if f[_IDX["AGG_LEVEL_DESC"]] not in constants.SEED_GEO_LEVELS.value:
        return False
    if f[_IDX["FREQ_DESC"]] not in constants.SEED_FREQ.value:
        return False
    return f[_IDX["STATISTICCAT_DESC"]].startswith(
        constants.SEED_STAT_PREFIXES.value
    )


def build_row(f: list[str]) -> tuple[str, int, dict] | None:
    """Build (table_slug, year, field_dict) or None if filtered/undeclared."""
    base = _SOURCE_BASE.get(f[_IDX["SOURCE_DESC"]])
    suffix = _GEO_SUFFIX.get(f[_IDX["AGG_LEVEL_DESC"]].strip())
    if base is None or suffix is None:
        return None
    table = f"{base}_{suffix}"
    if table not in _FACT_TABLES:
        return None
    if not _seed_keep(f):
        return None

    try:
        year = int(f[_IDX["YEAR"]].strip())
    except ValueError:
        return None

    state_fips = f[_IDX["STATE_FIPS_CODE"]].strip()
    county_code = f[_IDX["COUNTY_CODE"]].strip()
    county_fips = (
        state_fips.zfill(2) + county_code.zfill(3)
        if suffix == "county" and state_fips and county_code
        else ""
    )
    value, flag = clean_value(f[_IDX["VALUE"]])

    def g(name: str) -> str:
        return f[_IDX[name]].strip()

    row = {
        "year": str(year),
        "state_fips": state_fips,
        "state_abbreviation": g("STATE_ALPHA"),
        "state_name": g("STATE_NAME"),
        "agricultural_district_code": g("ASD_CODE"),
        "agricultural_district_name": g("ASD_DESC"),
        "county_fips": county_fips,
        "county_name": g("COUNTY_NAME"),
        "sector": g("SECTOR_DESC"),
        "commodity_group": g("GROUP_DESC"),
        "commodity": g("COMMODITY_DESC"),
        "commodity_class": g("CLASS_DESC"),
        "production_practice": g("PRODN_PRACTICE_DESC"),
        "utilization_practice": g("UTIL_PRACTICE_DESC"),
        "statistic_category": g("STATISTICCAT_DESC"),
        "unit": g("UNIT_DESC"),
        "short_description": g("SHORT_DESC"),
        "domain": g("DOMAIN_DESC"),
        "domain_category": g("DOMAINCAT_DESC"),
        "frequency": g("FREQ_DESC"),
        "reference_period": g("REFERENCE_PERIOD_DESC"),
        "value": value,
        "value_suppression_flag": flag,
        "coefficient_of_variation": clean_cv(f[_IDX["CV_%"]]),
    }
    # Empty string -> NULL (keep suppression codes as-is).
    row = {k: (v if v not in ("",) else None) for k, v in row.items()}
    return table, year, row


# --------------------------------------------------------------------------- #
# Parquet writing (all-STRING, part files per flush, coalesced per year)
# --------------------------------------------------------------------------- #
def _write_part(
    rows: list[dict], table: str, table_dir: Path, year: int, part: int
) -> None:
    """Write one buffered flush of rows to a numbered part file for a year.

    Args:
        rows: The buffered rows (dicts keyed by architecture column name).
        table: Fact-table slug.
        table_dir: Output directory for this table (``<output>/<table>``).
        year: Partition year; the part lands under ``year=<year>/``.
        part: Monotonic flush counter, used to name ``part-<part>.parquet``.
    """
    order, schema = _arch(table)
    pdir = table_dir / f"year={year}"
    pdir.mkdir(parents=True, exist_ok=True)
    arrays = [
        pa.array([r.get(name) for r in rows], type=pa.string())
        for name in order
    ]
    tbl = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(
        tbl, pdir / f"part-{part:04d}.parquet", compression="snappy"
    )


def coalesce_year_parts(table: str, table_dir: Path) -> int:
    """Rewrite each ``year=<Y>/part-*.parquet`` group as a single ``data.parquet``."""
    _, schema = _arch(table)
    n = 0
    for pdir in sorted(Path(table_dir).glob("year=*")):
        parts = sorted(pdir.glob("part-*.parquet"))
        if not parts:
            continue
        tbl = pq.read_table(parts, schema=schema)
        tmp = pdir / "data.parquet.tmp"
        pq.write_table(tbl, tmp, compression="snappy")
        for p in parts:
            p.unlink()
        tmp.rename(pdir / "data.parquet")
        n += 1
    return n


def build_dicionario_rows() -> list[tuple]:
    """dicionario rows: the value_suppression_flag code map, per fact table."""
    rows: list[tuple] = []
    for table in constants.FACT_TABLES.value:
        for code, label_pt in constants.SUPPRESSION_CODES.value.items():
            rows.append((table, "value_suppression_flag", code, "", label_pt))
    return rows


def _write_dicionario(output_dir: Path, rows: list[tuple]) -> Path:
    """Write the dicionario rows to ``<output>/dicionario/data.parquet``.

    Args:
        output_dir: Root output directory.
        rows: The dicionario rows from :func:`build_dicionario_rows`.

    Returns:
        The dicionario output directory.
    """
    ddir = output_dir / "dicionario"
    ddir.mkdir(parents=True, exist_ok=True)
    names = [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
        "valor",
    ]
    schema = pa.schema([pa.field(n, pa.string()) for n in names])
    cols = list(zip(*rows, strict=True))
    arrays = [pa.array(list(c), type=pa.string()) for c in cols]
    pq.write_table(
        pa.Table.from_arrays(arrays, schema=schema),
        ddir / "data.parquet",
        compression="snappy",
    )
    return ddir


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Stream the sector files, filter to the seed, write per-grain parquet.

    Returns a mapping with each fact table slug -> its output dir (str), plus
    ``dicionario``, ``max_year`` (int) and ``counts`` (per-table row counts).
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    gz_files = sorted(input_dir.glob("qs.*.txt.gz"))
    # Require exactly one bulk file per sector: a missing sector would silently
    # publish partial data (e.g. under --skip-download) and a stale extra file
    # would duplicate records.
    expected = {f"qs.{sector}.txt.gz" for sector in constants.SECTORS.value}
    actual = {p.name for p in gz_files}
    if actual != expected:
        raise ValueError(
            f"invalid bulk file set in {input_dir}: "
            f"missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )

    tables = list(constants.FACT_TABLES.value)
    table_dirs = {t: output_dir / t for t in tables}
    # Clear any prior snapshot before rebuilding: the bootstrap reuses a
    # persistent output_dir, so a year dropped by a later source (or the
    # dicionario from an earlier run) must not survive into the new upload.
    for d in (*table_dirs.values(), output_dir / "dicionario"):
        shutil.rmtree(d, ignore_errors=True)
    buffers: dict[tuple[str, int], list[dict]] = {}
    counts = {t: 0 for t in tables}
    buffered = 0
    part = 0
    max_year = 0

    def flush() -> None:
        nonlocal buffered, part
        if buffered == 0:
            return
        for (table, year), rows in buffers.items():
            if rows:
                _write_part(rows, table, table_dirs[table], year, part)
        buffers.clear()
        buffered = 0
        part += 1

    for gz in gz_files:
        print(f"  streaming {gz.name} ...", flush=True)
        with gzip.open(gz, "rt", encoding="utf-8", errors="replace") as fh:
            header = next(fh, "")
            # Validate the full header, not just the first column: a reordered
            # 39-column file would still pass a startswith check while _IDX read
            # every field from the wrong position and silently corrupt the rows.
            source_columns = header.rstrip("\r\n").split("\t")
            if source_columns != constants.RAW_COLUMNS.value:
                raise ValueError(
                    f"unexpected header in {gz.name}: {header[:60]!r}"
                )
            for line in fh:
                f = line.rstrip("\n").split("\t")
                if len(f) != len(constants.RAW_COLUMNS.value):
                    continue
                built = build_row(f)
                if built is None:
                    continue
                table, year, row = built
                buffers.setdefault((table, year), []).append(row)
                counts[table] += 1
                buffered += 1
                if year > max_year:
                    max_year = year
                if buffered >= _FLUSH_ROWS:
                    flush()
    flush()

    for t in tables:
        ny = coalesce_year_parts(t, table_dirs[t])
        print(f"  coalesced {t}: {ny} year partitions ({counts[t]:,} rows)")

    result: dict = {t: str(table_dirs[t]) for t in tables}
    result["dicionario"] = str(
        _write_dicionario(output_dir, build_dicionario_rows())
    )
    result["max_year"] = max_year
    result["counts"] = counts
    print(f"  clean_all done: counts={counts} max_year={max_year}")
    return result
