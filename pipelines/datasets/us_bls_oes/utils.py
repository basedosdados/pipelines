"""Download + cleaning transform for us_bls_oes (shared by the pipeline and the
one-shot bootstrap in models/us_bls_oes/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
`clean_all` directly. Schema and column order come from the architecture CSVs
(the single source of truth).

OEWS publishes the same estimates in two layouts. From 2011 one stacked workbook
(`oesm{YY}all.zip`) holds everything; before that the same rows are split across
`nat`/`st`/`ma`/`in4` zips with the geographic and industry dimensions implied by
the filename rather than stored. Both are read into one union frame and then
split into the `area` (cross-industry, by geography) and `industry` (national, by
NAICS) tables.
"""

import csv
import io
import logging
import math
import re
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_bls_oes.constants import constants

log = logging.getLogger("us_bls_oes")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}

_RENAME = constants.RENAME.value
_SENTINELS = set(constants.SENTINELS.value)
_CROSS = constants.CROSS_INDUSTRY_OWNERSHIP.value
_PSEUDO = constants.PSEUDO_NAICS_OWNERSHIP.value
_OWN_TITLES = constants.OWNERSHIP_TITLE_TO_CODE.value
_WAGE_COLUMNS = (
    constants.HOURLY_WAGE_COLUMNS.value + constants.ANNUAL_WAGE_COLUMNS.value
)
_TRUTHY = {"TRUE", "1", "1.0", "Y", "YES", "T"}


# ── download ────────────────────────────────────────────────────────────────
def release_files(year: int) -> list[str]:
    """Names of the zips making up one OEWS release.

    Args:
        year: Reference year (May of this year).

    Returns:
        One filename for 2011 onward, four for 2003-2010.
    """
    yy = f"{year % 100:02d}"
    if year >= constants.FIRST_ALL_YEAR.value:
        return [f"oesm{yy}all.zip"]
    return [f"oesm{yy}{part}.zip" for part in ("nat", "st", "ma", "in4")]


def download_release(input_dir: Path, year: int) -> list[Path]:
    """Fetch one year's zips into ``input_dir``, skipping files already present.

    A browser-like User-Agent carrying a contact email is mandatory: www.bls.gov
    returns 403 without one.

    Args:
        input_dir: Directory to download into; created if absent.
        year: Reference year.

    Returns:
        Paths of the downloaded (or already present) zips.

    Raises:
        requests.HTTPError: If any file fails to download.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": constants.USER_AGENT.value}
    base = constants.BASE_URL.value
    paths = []
    for name in release_files(year):
        dest = input_dir / name
        if not dest.exists() or dest.stat().st_size == 0:
            r = requests.get(f"{base}/{name}", headers=headers, timeout=900)
            r.raise_for_status()
            dest.write_bytes(r.content)
            log.info(
                f"{year}: downloaded {name} ({dest.stat().st_size:,} bytes)"
            )
        paths.append(dest)
    return paths


def latest_source_year(timeout: int = 120) -> int:
    """Read the OEWS tables page and return the most recent published year.

    The page links one `oesm{YY}all.zip` per release, so the newest release is
    the highest two-digit year among those links.

    Args:
        timeout: Request timeout in seconds.

    Returns:
        Four-digit reference year of the newest release.

    Raises:
        requests.HTTPError: If the page cannot be fetched.
        ValueError: If no release links are found.
    """
    r = requests.get(
        constants.TABLES_URL.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        timeout=timeout,
    )
    r.raise_for_status()
    years = {int(m) for m in re.findall(r"oesm(\d{2})all\.zip", r.text)}
    if not years:
        raise ValueError(
            "no oesm??all.zip links found on the OEWS tables page"
        )
    return 2000 + max(years)


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Args:
        table: Table slug (``"area"``, ``"industry"`` or ``"dicionario"``).

    Returns:
        One dict per column, in architecture order.
    """
    with open(
        constants.ARCHITECTURE_DIR.value / f"{table}.csv", newline=""
    ) as fh:
        return list(csv.DictReader(fh))


def read_area_type_map() -> dict[str, str]:
    """Load the pooled 2011-2013 area code to area type lookup.

    The 2005-2010 metropolitan files do not carry ``area_type``; it is recovered
    from this lookup so that metropolitan divisions are not silently labelled as
    metropolitan statistical areas (they nest inside them, so conflating the two
    would double-count any sum over the area type).

    Returns:
        Mapping of area code to area type.
    """
    path = constants.AREA_TYPE_MAP.value
    with open(path, newline="") as fh:
        return {r["area_id"]: r["area_type"] for r in csv.DictReader(fh)}


# ── cell helpers ────────────────────────────────────────────────────────────
def _norm_header(c) -> str:
    return str(c).strip().lower().replace(" ", "_")


def _is_blank(v) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


def _code(series: pd.Series, width: int | None = None) -> pd.Series:
    """Normalise a code column to a stripped string, optionally zero-padded.

    Excel readers hand back an integer for a purely numeric cell, which drops the
    leading zeros that OEWS area and NAICS codes rely on. Codes that already
    arrive as text are left alone.
    """

    def one(v):
        if _is_blank(v):
            return None
        if isinstance(v, float) and v.is_integer():
            v = int(v)
        t = str(v).strip()
        if width and t.isdigit() and len(t) < width:
            t = t.zfill(width)
        return t or None

    return series.map(one)


def _number(series: pd.Series) -> pd.Series:
    """Coerce a measure column to float, mapping every source sentinel to NULL.

    ``*``, ``**``, ``#`` and ``~`` are all non-numeric, so ``errors="coerce"``
    nulls them. Which of them applied is preserved separately by the
    ``wage_top_coded`` and ``establishments_reporting_below_threshold`` flags.
    """
    return pd.to_numeric(series, errors="coerce")


def _sentinel_mask(series: pd.Series, sentinel: str) -> pd.Series:
    """True where the raw cell is exactly ``sentinel``."""
    return series.map(lambda v: isinstance(v, str) and v.strip() == sentinel)


def _truth(series: pd.Series) -> pd.Series:
    """Normalise a source flag column to ``TRUE``/``FALSE``.

    OEWS wrote ``1`` in the older releases and ``TRUE`` in the newer ones; blank
    means the flag does not apply.
    """
    return series.map(
        lambda v: (
            "FALSE"
            if _is_blank(v) or str(v).strip().upper() not in _TRUTHY
            else "TRUE"
        )
    )


# ── reading ─────────────────────────────────────────────────────────────────
def _read_sheet(zip_path: Path, member: str) -> pd.DataFrame:
    """Read one workbook out of a zip and rename its columns to architecture names.

    Headers vary across releases in case, spacing and naming (``LOC_Q`` vs
    ``loc_quotient`` vs ``LOC QUOTIENT``, ``occ code`` vs ``occ_code``);
    :data:`constants.RENAME` covers every one of the 14 distinct shapes.
    Unrecognised columns are dropped.

    Engine is chosen by extension: calamine reads the large modern ``.xlsx``
    workbooks an order of magnitude faster than openpyxl, but panics on some of
    the legacy ``.xls`` files, which xlrd reads without complaint.
    """
    with zipfile.ZipFile(zip_path) as z:
        buf = io.BytesIO(z.read(member))
    engine = "xlrd" if member.lower().endswith(".xls") else "calamine"
    df = pd.read_excel(buf, engine=engine)
    df.columns = [_norm_header(c) for c in df.columns]
    keep = {c: _RENAME[c] for c in df.columns if c in _RENAME}
    df = df[list(keep)].rename(columns=keep)
    # A few workbooks carry a trailing blank row.
    return df.dropna(how="all").reset_index(drop=True)


def _members(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path) as z:
        return [
            n
            for n in z.namelist()
            if n.lower().endswith((".xls", ".xlsx"))
            and "field_desc" not in n.lower()
        ]


# Width an area code is zero-padded to, by area type. Excel readers hand back
# `AREA` as an integer, which drops the leading zeros that state FIPS codes ("01")
# and OEWS nonmetropolitan codes ("0100001") carry in the published files.
_AREA_WIDTH = {"1": None, "2": 2, "3": 2, "4": 5, "5": 5, "6": 7}


def _pad_area_id(ids: pd.Series, types: pd.Series) -> pd.Series:
    """Zero-pad each area code to the width its area type implies."""

    def one(a, t):
        if a is None:
            return None
        w = _AREA_WIDTH.get(str(t))
        return a.zfill(w) if w and a.isdigit() and len(a) < w else a

    return pd.Series(
        [one(a, t) for a, t in zip(ids, types, strict=True)],
        index=ids.index,
        dtype="object",
    )


def _read_all_release(input_dir: Path, year: int) -> pd.DataFrame:
    """Read a 2011-onward release: one stacked workbook holding every estimate."""
    path = input_dir / release_files(year)[0]
    member = _members(path)[0]
    df = _read_sheet(path, member)
    missing = {"area_id", "area_type", "industry_id", "ownership_id"} - set(df)
    if missing:
        raise ValueError(f"{year}: {member} is missing {sorted(missing)}")
    df["area_type"] = _code(df["area_type"])
    unknown = set(df["area_type"].dropna()) - set(_AREA_WIDTH)
    if unknown:
        raise ValueError(f"{year}: unknown area_type {sorted(unknown)}")
    df["area_id"] = _pad_area_id(_code(df["area_id"]), df["area_type"])
    df["industry_id"] = _code(df["industry_id"])
    df["ownership_id"] = _derive_ownership(
        df["industry_id"], _code(df["ownership_id"]), year
    )
    df["_side"] = df["industry_id"].map(
        lambda n: "area" if n in _CROSS else "industry"
    )
    _assert_cross_industry_side(df, year)
    return df


def _legacy_group(member: str) -> str:
    """Classify a 2003-2010 workbook by filename into a file group.

    The filename is the only place the geographic level and the ownership split
    are recorded for these releases.
    """
    b = Path(member).name.lower()
    if b.startswith("national"):
        return "area_national_ownership" if "owner" in b else "area_national"
    if b.startswith("state"):
        return "area_state"
    if b.startswith("bos"):
        return "area_nonmetropolitan"
    if b.startswith(("msa", "amsa")):
        return "area_metropolitan"
    if re.match(r"nat(3d|4d|5d|sector)", b):
        return "industry_ownership" if "owner" in b else "industry"
    raise ValueError(f"unclassified OEWS workbook: {member}")


def _ownership_from_title(series: pd.Series) -> pd.Series:
    """Map a 2003-2010 ownership title onto the modern ownership code."""

    def one(v):
        if _is_blank(v):
            return None
        key = " ".join(str(v).split()).strip().lower()
        if key not in _OWN_TITLES:
            raise ValueError(f"unknown OEWS ownership title: {v!r}")
        return _OWN_TITLES[key]

    return series.map(one)


def _read_legacy_release(input_dir: Path, year: int) -> pd.DataFrame:
    """Read a 2003-2010 release and rebuild the columns the layout leaves implicit."""
    area_types = read_area_type_map()
    use_lookup = year >= constants.FIRST_CBSA_YEAR.value
    metro_width = 5 if use_lookup else 4
    frames, fallbacks = [], 0

    for name in release_files(year):
        path = input_dir / name
        for member in _members(path):
            group = _legacy_group(member)
            df = _read_sheet(path, member)
            if df.empty:
                continue
            df["_side"] = "area" if group.startswith("area") else "industry"

            if group.startswith("area_national"):
                df["area_id"] = "99"
                df["area_type"] = "1"
                df["area_name"] = "U.S."
                df["state_abbreviation"] = "US"
            elif group == "area_state":
                df["area_id"] = _code(df["area_id"], width=2)
                df["area_type"] = df["area_id"].map(
                    lambda a: (
                        "3" if a in constants.TERRITORY_FIPS.value else "2"
                    )
                )
            elif group == "area_nonmetropolitan":
                df["area_id"] = _code(df["area_id"], width=7)
                df["area_type"] = "6"
            elif group == "area_metropolitan":
                df["area_id"] = _code(df["area_id"], width=metro_width)
                if use_lookup:
                    df["area_type"] = df["area_id"].map(
                        lambda a: area_types.get(a, "4")
                    )
                    fallbacks += sum(
                        1 for a in df["area_id"] if a not in area_types
                    )
                else:
                    # 2003-2004 metropolitan areas follow the pre-2003 OMB
                    # definitions and carry 4-digit MSA/PMSA codes, which the
                    # CBSA-based lookup cannot resolve.
                    df["area_type"] = "4"

            if group.endswith("_ownership"):
                df["ownership_id"] = _ownership_from_title(
                    df["_ownership_title"]
                )
            else:
                df["ownership_id"] = constants.ALL_OWNERSHIP_CODE.value

            if "industry_id" in df:
                df["industry_id"] = _code(df["industry_id"])
            frames.append(df)

    if fallbacks:
        log.warning(
            f"{year}: {fallbacks:,} metropolitan rows had no area_type match "
            f"and fell back to 4"
        )
    return pd.concat(frames, ignore_index=True)


def read_release(input_dir: Path, year: int) -> pd.DataFrame:
    """Read one OEWS release into a single union frame with architecture names.

    Args:
        input_dir: Directory holding the downloaded zips.
        year: Reference year.

    Returns:
        Every published row for the year, carrying a ``_side`` column marking
        whether it belongs to the ``area`` or the ``industry`` table.
    """
    if year >= constants.FIRST_ALL_YEAR.value:
        df = _read_all_release(input_dir, year)
    else:
        df = _read_legacy_release(input_dir, year)
    df["year"] = year
    df["occupation_id"] = _code(df["occupation_id"])
    return df


# ── invariants ──────────────────────────────────────────────────────────────
def _derive_ownership(
    industry_id: pd.Series, published: pd.Series, year: int
) -> pd.Series:
    """Take ownership from the OEWS pseudo-NAICS code where there is one.

    OEWS encodes ten ownership universes as pseudo-NAICS codes, so for those rows
    the industry code already determines the ownership. Deriving it rather than
    trusting ``own_code`` makes the area table robust to the May 2012 release,
    which publishes ``own_code = 5`` (Private) on every one of them. Ordinary
    industry rows keep the published code.

    Disagreements are logged rather than raised: a future release could add a
    pseudo-code, and the caller's key assertion is what actually guards the
    table's integrity.
    """
    derived = industry_id.map(_PSEUDO.get)
    disagrees = derived.notna() & published.notna() & (derived != published)
    if disagrees.any():
        counts = (
            pd.DataFrame(
                {
                    "industry_id": industry_id[disagrees],
                    "published": published[disagrees],
                    "derived": derived[disagrees],
                }
            )
            .value_counts()
            .to_dict()
        )
        log.warning(
            f"{year}: own_code disagrees with the OEWS pseudo-NAICS ownership "
            f"on {disagrees.sum():,} rows; using the derived value. "
            f"{ {k: v for k, v in counts.items()} }"
        )
    return derived.fillna(published)


def _assert_cross_industry_side(df: pd.DataFrame, year: int) -> None:
    """Fail loudly if OEWS changes how it encodes cross-industry estimates.

    The area table drops the industry code, which is only safe while every
    cross-industry row carries one of the six known pseudo-NAICS codes and those
    map one to one onto the ownership. A release that broke either would silently
    collapse distinct estimates onto the same key.
    """
    area = df[df["_side"] == "area"]
    if area.empty:
        raise ValueError(f"{year}: no cross-industry rows found")
    mapping = area.groupby("industry_id")["ownership_id"].unique()
    for naics, owners in mapping.items():
        if list(owners) != [_CROSS[str(naics)]]:
            raise ValueError(
                f"{year}: cross-industry NAICS {naics} maps to ownership "
                f"{sorted(owners)}, expected [{_CROSS[str(naics)]}]"
            )
    owners = [o for v in mapping.to_numpy() for o in v]
    if len(owners) != len(set(owners)):
        raise ValueError(
            f"{year}: cross-industry ownership codes are not unique across "
            f"pseudo-NAICS codes ({sorted(owners)}); the area table cannot drop "
            f"the industry code"
        )


def assert_unique_key(df: pd.DataFrame, table: str, year: int) -> None:
    """Fail loudly if a table's key is not unique for the year."""
    key = constants.KEYS.value[table]
    dups = int(df.duplicated(subset=key).sum())
    if dups:
        sample = df[df.duplicated(subset=key, keep=False)][key].head(6)
        raise ValueError(
            f"{year}: {table} has {dups:,} duplicate keys on {key}\n{sample}"
        )


# ── transform ───────────────────────────────────────────────────────────────
def finalise(df: pd.DataFrame, table: str, year: int) -> pd.DataFrame:
    """Type, flag and order one table's rows to the architecture.

    Flags are derived from the raw cells before the numeric coercion nulls them:
    ``#`` marks a top-coded wage and ``~`` a below-threshold reporting percent,
    both of which are bounds rather than missing values.

    Args:
        df: Rows for one side of one release, from :func:`read_release`.
        table: ``"area"`` or ``"industry"``.
        year: Reference year, used in error messages.

    Returns:
        The table's rows in architecture column order, correctly typed.
    """
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    types = {a["name"]: a["bigquery_type"] for a in arch}
    out = pd.DataFrame(index=df.index)

    present_wages = [c for c in _WAGE_COLUMNS if c in df]
    top_coded = pd.Series(False, index=df.index)
    for c in present_wages:
        top_coded |= _sentinel_mask(df[c], constants.TOP_CODE.value)

    below = None
    if "percent_establishments_reporting" in df:
        below = _sentinel_mask(
            df["percent_establishments_reporting"],
            constants.BELOW_THRESHOLD.value,
        )

    for name in order:
        if name == "wage_top_coded":
            out[name] = top_coded.map({True: "TRUE", False: "FALSE"})
            continue
        if name == "establishments_reporting_below_threshold":
            out[name] = (
                below.map({True: "TRUE", False: "FALSE"})
                if below is not None
                else pd.Series(pd.NA, index=df.index, dtype="object")
            )
            continue
        if name not in df:
            # The source field does not exist for this release; see the column's
            # `observations` in the architecture CSV.
            out[name] = pd.Series(pd.NA, index=df.index, dtype="object")
            continue
        if name in ("annual_wage_only", "hourly_wage_only"):
            out[name] = _truth(df[name])
        elif types[name] == "INT64":
            out[name] = _number(df[name]).round().astype("Int64")
        elif types[name] == "FLOAT64":
            out[name] = _number(df[name])
        else:
            out[name] = _code(df[name])

    out = out[order]
    assert_unique_key(out, table, year)
    return out


def split_release(df: pd.DataFrame, year: int) -> dict[str, pd.DataFrame]:
    """Split a union frame into the ``area`` and ``industry`` tables.

    Args:
        df: One release, from :func:`read_release`.
        year: Reference year.

    Returns:
        Table slug to typed, ordered rows.
    """
    tables = {}
    for table in constants.DATA_TABLES.value:
        side = df[df["_side"] == table].reset_index(drop=True)
        if side.empty:
            raise ValueError(f"{year}: no rows for table {table}")
        tables[table] = finalise(side, table, year)
    return tables


# ── output ──────────────────────────────────────────────────────────────────
def write_partitioned(
    sub: pd.DataFrame, table: str, year: int, output_dir: Path
) -> Path:
    """Write one table-year as all-STRING Snappy Parquet, hive-partitioned by year.

    Staging is all-STRING by Data Basis convention — the dbt model ``safe_cast``s
    every column to its real type, and ``pipelines.utils.gcs.dump_header``
    stringifies the header file that BigQuery infers the staging schema from.
    Emitting typed parquet against that STRING schema makes BigQuery reject the
    files. See [[project_dump_header_parquet_bug]].

    Values still pass through the architecture's real types first, so ``year``
    serializes as ``"2025"`` rather than ``"2025.0"``, and only then cast to
    string via arrow — never ``astype(str)``, which would render a NULL as the
    literal ``"nan"`` and defeat the dbt ``safe_cast``.

    Args:
        sub: One table's rows for one year, from :func:`split_release`.
        table: Table slug.
        year: Reference year, used as the partition value.
        output_dir: Root output directory.

    Returns:
        Path of the written parquet file.
    """
    arch = read_arch(table)
    typed = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    pdir = output_dir / table / f"year={year}"
    pdir.mkdir(parents=True, exist_ok=True)
    at = pa.Table.from_pandas(sub, schema=typed, preserve_index=False)
    pq.write_table(
        at.cast(strings), pdir / "data.parquet", compression="snappy"
    )
    return pdir / "data.parquet"


def clean_year(input_dir: Path, output_dir: Path, year: int) -> dict[str, int]:
    """Read, split and write one OEWS release.

    Args:
        input_dir: Directory holding the downloaded zips.
        output_dir: Root output directory.
        year: Reference year.

    Returns:
        Table slug to row count written.
    """
    tables = split_release(read_release(input_dir, year), year)
    counts = {}
    for table, sub in tables.items():
        write_partitioned(sub, table, year, output_dir)
        counts[table] = len(sub)
    log.info(f"{year}: " + ", ".join(f"{t}={n:,}" for t, n in counts.items()))
    return counts


def clean_all(
    input_dir: Path, output_dir: Path, years: list[int]
) -> dict[int, dict[str, int]]:
    """Clean a list of releases, one year at a time.

    Years are processed independently so peak memory stays at one release
    (roughly 400k rows) rather than the whole panel.

    Args:
        input_dir: Directory holding the downloaded zips.
        output_dir: Root output directory.
        years: Reference years to clean.

    Returns:
        Year to table row counts.
    """
    return {y: clean_year(input_dir, output_dir, y) for y in years}


# ── dictionary ──────────────────────────────────────────────────────────────
def build_dicionario(output_dir: Path) -> Path:
    """Build the `dicionario` table from the codes actually present in the output.

    Keys are read back from the written partitions rather than declared, so the
    dictionary can never claim a code the data does not contain, nor miss one it
    does. An unlabelled code is an error, not a silent gap: it means OEWS
    introduced a category the label map has not caught up with.

    Args:
        output_dir: Root output directory holding the cleaned partitions.

    Returns:
        Path of the written parquet file.

    Raises:
        ValueError: If a code present in the data has no label.
    """
    labels = constants.DICTIONARY_LABELS.value
    rows = []
    for table in constants.DATA_TABLES.value:
        columns = {a["name"] for a in read_arch(table)}
        for column, mapping in labels.items():
            if column not in columns:
                continue
            years: dict[str, set[int]] = {}
            for pdir in sorted((output_dir / table).glob("year=*")):
                year = int(pdir.name.split("=")[1])
                part = pd.read_parquet(pdir / "data.parquet", columns=[column])
                for code in part[column].dropna().unique():
                    years.setdefault(str(code), set()).add(year)
            if not years:
                continue
            unknown = sorted(set(years) - set(mapping))
            if unknown:
                raise ValueError(
                    f"{table}.{column}: no label for {unknown} — the source "
                    f"introduced a category the label map does not cover"
                )
            for code in sorted(years, key=lambda c: (len(c), c)):
                span = years[code]
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": code,
                        "cobertura_temporal": f"{min(span)}(1){max(span)}",
                        "valor": mapping[code],
                    }
                )

    order = [a["name"] for a in read_arch("dicionario")]
    df = pd.DataFrame(rows, columns=order)
    dest = output_dir / "dicionario"
    dest.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([pa.field(c, pa.string()) for c in order])
    pq.write_table(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        dest / "data.parquet",
        compression="snappy",
    )
    log.info(f"dicionario: {len(df):,} entries -> {dest}")
    return dest / "data.parquet"
