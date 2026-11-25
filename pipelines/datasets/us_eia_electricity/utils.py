"""Pure download and transform helpers for us_eia_electricity — no Prefect imports.

This module is the single home of the cleaning transform. The one-shot onboarding
scripts under ``models/us_eia_electricity/code/`` import these functions rather than
duplicating them, so the recurring pipeline and the bootstrap cannot drift.

The EIA publishes Forms 860 and 923 as one ZIP of Excel workbooks per report
year, and the layout of those workbooks changes almost every year: files are
renamed, sheets are split and merged, header rows move, and columns are renamed.
Reading 25 vintages by hand is not a schema problem, it is an archaeology
problem, and the Public Utility Data Liberation project (PUDL, MIT licence) has
already done it. Its extraction maps and code vocabularies are vendored verbatim
under ``models/us_eia_electricity/code/pudl/`` — see that directory's README and
``vendor_pudl.py``.

What is reused from PUDL and what is not:

* **Reused** — the (year, page) -> file / sheet / header-rows maps, the
  (year, raw column) -> canonical column maps, the code vocabularies, and the
  mechanical data repairs those vocabularies carry: ``code_fixes`` (a dirty code
  standing for a real one) and ``ignored_codes`` (a code that means nothing).
  Also reused: the two published-value repairs PUDL documents, EIA-923's ``.``
  for NA and its fuel cost being in **cents** per MMBtu.
* **Not reused** — PUDL's *inferential* transforms: backfilling the
  ``prime_mover_code`` missing from the 2001 and 2002 EIA-923 files from other
  years, remapping municipal solid waste to biogenic and non-biogenic splits,
  and aggregating rows that collide on the natural key. Those are analytical
  choices that suit PUDL's harmonised warehouse. Data Basis publishes the
  microdata as filed, so a value the respondent did not report stays null here
  and colliding rows stay as two rows.

Source traps the transform exists to neutralise, each measured over the whole
2001-2026 corpus rather than assumed — see ``models/us_eia_electricity/CLAUDE.md``.

Output parquet is **all-STRING**. ``pipelines.utils.gcs.dump_header`` stringifies
the header BigQuery infers the staging schema from, so typed parquet is rejected;
the dbt model ``safe_cast``s every column to its architecture type.
"""

import csv
import io
import json
import os
import re
import shutil
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_eia_electricity.constants import constants

DOWNLOAD_CHUNK = 8 * 1024 * 1024
REQUEST_TIMEOUT = 900

# PUDL's simplify_columns, reimplemented so the vendored column maps can be used
# without importing PUDL: non-alphanumerics become spaces, letters are lowered,
# internal whitespace is compacted, and the remaining spaces become underscores.
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")
_WS = re.compile(r"\s+")

# EIA's stand-ins for "no value": a bare decimal point, whitespace only, or a run
# of hyphens. Straight from PUDL's standardize_na_values.
_NA_RE = re.compile(r"^(\.|\s*|-+)$")

# "County", "Parish", "Borough", "City and Borough", "Census Area", "Municipio"
# and friends are suffixes the directory carries and the forms often omit.
_COUNTY_SUFFIX = re.compile(
    r"\s+(county|parish|borough|census area|city and borough|municipality"
    r"|municipio|city|planning region)$"
)


# --------------------------------------------------------------------------
# Architecture (the schema source of truth)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Col:
    """One column of an architecture table.

    The transform only needs ``name`` / ``bq_type`` / ``original``; the remaining
    fields are carried so the metadata scripts read the same CSV through the same
    parser rather than writing a second one.

    ``original`` is the **PUDL canonical** column name, not a raw spreadsheet
    header — the raw header differs year by year and the vendored column maps are
    what resolve it. A leading ``@`` marks a column this transform derives rather
    than reads (``@county_id``, ``@operating_date``, ...).
    """

    name: str
    bq_type: str
    original: str
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
                    original=r["original_name"].strip(),
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


def simplify(label: object) -> str:
    """Normalise a raw spreadsheet header the way PUDL's column maps expect."""
    text = _NON_ALNUM.sub(" ", str(label)).lower().strip()
    return _WS.sub("_", text)


# --------------------------------------------------------------------------
# Vendored PUDL maps
# --------------------------------------------------------------------------


def _read_indexed_csv(path: Path) -> dict[str, dict[str, str]]:
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        key = (reader.fieldnames or [""])[0]
        return {row[key]: row for row in reader}


@cache
def _form_meta(form: str) -> dict[str, dict[str, dict[str, str]]]:
    base = Path(constants.PUDL_DIR.value) / form
    return {
        "file": _read_indexed_csv(base / "file_map.csv"),
        "sheet": _read_indexed_csv(base / "page_map.csv"),
        "skiprows": _read_indexed_csv(base / "skiprows.csv"),
        "skipfooter": _read_indexed_csv(base / "skipfooter.csv"),
    }


@cache
def _column_map(form: str, page: str) -> dict[str, dict[str, str]]:
    """``{year: {raw simplified header: canonical name}}`` for one page."""
    path = (
        Path(constants.PUDL_DIR.value) / form / "column_maps" / f"{page}.csv"
    )
    out: dict[str, dict[str, str]] = {}
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        year_key = (reader.fieldnames or [""])[0]
        for row in reader:
            year = row.pop(year_key)
            out[year] = {
                raw.strip(): canonical
                for canonical, raw in row.items()
                if raw.strip()
            }
    return out


@cache
def codes() -> dict[str, dict]:
    """The vendored PUDL code vocabularies, keyed by PUDL table name."""
    return json.loads(
        (Path(constants.PUDL_DIR.value) / "codes.json").read_text()
    )


@cache
def _code_repair(vocabulary: str) -> tuple[dict[str, str], set[str], set[str]]:
    """``(code_fixes, ignored_codes, valid_codes)`` for one vocabulary."""
    entry = codes()[vocabulary]
    valid = {str(row["code"]) for row in entry["rows"]}
    return entry["code_fixes"], set(entry["ignored_codes"]), valid


def repair_code(value: str | None, vocabulary: str) -> str | None:
    """Apply PUDL's ``code_fixes`` and ``ignored_codes`` to one published code.

    A code that is neither valid nor repairable is **kept as published**, not
    nulled: EIA adds codes faster than any vocabulary is updated, and silently
    discarding an unrecognised one would lose data. Only the codes PUDL
    explicitly lists as meaningless become null.
    """
    if value is None:
        return None
    fixes, ignored, _valid = _code_repair(vocabulary)
    if value in fixes:
        return fixes[value]
    stripped = value.strip()
    if stripped in fixes:
        return fixes[stripped]
    if stripped in ignored:
        return None
    return stripped or None


# --------------------------------------------------------------------------
# Source download
# --------------------------------------------------------------------------


def _session(session: requests.Session | None = None) -> requests.Session:
    s = session or requests.Session()
    s.headers.setdefault("User-Agent", constants.USER_AGENT.value)
    return s


def list_source_files(
    form: str, session: requests.Session | None = None
) -> dict[int, dict]:
    """Return ``{year: {"url": ..., "early_release": bool}}`` for one form.

    Scraped from the form's own landing page rather than guessed, because the
    naming is not uniform: EIA-860 archives are ``eia860<YYYY>.zip`` but the
    current early release is ``eia8602025ER.zip``; EIA-923 archives are
    ``f923_<YYYY>.zip`` back to 2008 and ``f906920_<YYYY>.zip`` before that, with
    a lowercase ``f923_2025er.zip`` for the early release. Reading the page keeps
    the pipeline working when EIA moves a year from ``xls/`` to ``archive/xls/``,
    which it does every time a new year is published.
    """
    page = (
        constants.EIA860_PAGE_URL.value
        if form == "eia860"
        else constants.EIA923_PAGE_URL.value
    )
    html = _session(session).get(page, timeout=REQUEST_TIMEOUT)
    html.raise_for_status()
    pattern = r'href="\s*([^"]*?(?:eia860(\d{4})(ER)?|f923_(\d{4})(er)?|f906920_?y?(\d{4}))\.zip)"'
    out: dict[int, dict] = {}
    for match in re.finditer(pattern, html.text, flags=re.IGNORECASE):
        href = match.group(1).strip()
        year = int(
            next(
                g
                for g in (match.group(2), match.group(4), match.group(6))
                if g
            )
        )
        early = bool(match.group(3) or match.group(5))
        if year < constants.FIRST_YEAR.value:
            continue
        url = (
            href
            if href.startswith("http")
            else constants.EIA_BASE.value + "/" + form + "/" + href.lstrip("/")
        )
        # A year listed twice (final in archive/, early release in xls/) keeps
        # the final file: the final revision supersedes the early release, and
        # publishing both would double the year.
        if (
            (year in out and out[year]["early_release"] and not early)
            or year not in out
            or (not out[year]["early_release"] and not early)
        ):
            out[year] = {"url": url, "early_release": early}
    return dict(sorted(out.items()))


def source_zip(input_dir: Path, form: str, year: int) -> Path:
    return Path(input_dir) / form / f"{form}_{year}.zip"


def download_form(
    form: str,
    input_dir: Path,
    listing: dict[int, dict] | None = None,
    years: list[int] | None = None,
    session: requests.Session | None = None,
    force: bool = False,
) -> dict[int, Path]:
    """Download one ZIP per report year into ``<input_dir>/<form>/``."""
    s = _session(session)
    listing = listing if listing is not None else list_source_files(form, s)
    out: dict[int, Path] = {}
    for year, entry in listing.items():
        if years is not None and year not in years:
            continue
        dest = source_zip(input_dir, form, year)
        dest.parent.mkdir(parents=True, exist_ok=True)
        out[year] = dest
        if dest.exists() and dest.stat().st_size > 0 and not force:
            continue
        tmp = dest.with_suffix(".part")
        with s.get(entry["url"], stream=True, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(DOWNLOAD_CHUNK):
                    fh.write(chunk)
        tmp.replace(dest)
    return out


# --------------------------------------------------------------------------
# Reading one page out of one year's ZIP
# --------------------------------------------------------------------------


@dataclass
class YearReader:
    """Reads pages out of one report year's ZIP, parsing each workbook once.

    The generator pages are three sheets of a single 20 MB workbook and the
    EIA-923 pages are up to ten sheets of one workbook, so re-opening the file
    per page costs about twenty seconds each time. ``pandas.ExcelFile`` holds the
    parsed workbook, and the second sheet out of the same object is essentially
    free.
    """

    form: str
    year: int
    path: Path
    _zip: zipfile.ZipFile = field(init=False, repr=False)
    _members: dict[str, str] = field(init=False, repr=False)
    _books: dict[str, pd.ExcelFile] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._zip = zipfile.ZipFile(self.path)
        # Member paths are matched on basename, case-insensitively: some archives
        # nest the workbooks in a folder and some change the case of the
        # extension between vintages.
        self._members = {Path(n).name.lower(): n for n in self._zip.namelist()}

    def close(self) -> None:
        self._books.clear()
        self._zip.close()

    def __enter__(self) -> "YearReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _meta(self, page: str) -> tuple[str, int, int, int] | None:
        meta = _form_meta(self.form)
        year = str(self.year)
        if year not in meta["file"]:
            # A ZIP exists for a report year the vendored maps do not cover. That
            # happens the first time EIA publishes a new year, and silently
            # skipping it would drop a whole year of data with a green run.
            raise KeyError(
                f"{self.form}: no extraction map for report year {self.year}. "
                f"Refresh models/us_eia_electricity/code/pudl/ with vendor_pudl.py against a "
                f"current PUDL checkout, then re-run."
            )
        if page not in meta["file"][year]:
            return None
        name = meta["file"][year][page]
        if name == "-1":
            return None
        return (
            name,
            int(meta["sheet"][year][page]),
            int(meta["skiprows"][year][page]),
            int(meta["skipfooter"][year][page]),
        )

    def _member(self, page: str, name: str) -> str:
        """Locate the workbook for one page, by name first and pattern second.

        EIA renames the live year's file on every release — the EIA-923 name
        carries both the latest data month and the publication date — so the name
        recorded in the extraction map is only reliable for years that have
        settled. See constants.MEMBER_PATTERNS.
        """
        member = self._members.get(name.lower())
        if member is not None:
            return member
        pattern = constants.MEMBER_PATTERNS.value.get((self.form, page))
        if pattern:
            regex = re.compile(pattern.format(year=self.year), re.IGNORECASE)
            hits = sorted(
                {v for k, v in self._members.items() if regex.search(k)}
            )
            if len(hits) == 1:
                return hits[0]
            if len(hits) > 1:
                raise FileNotFoundError(
                    f"{self.path.name}: {name!r} is absent and the fallback pattern "
                    f"for {self.form}/{page} matches {len(hits)} members: {hits}"
                )
        raise FileNotFoundError(
            f"{self.path.name}: no member named {name!r} and no fallback match for "
            f"{self.form}/{page} (has {sorted(self._members)})"
        )

    def source_filename(self, page: str) -> str | None:
        """The name of the workbook actually read, which drives data_maturity."""
        meta = self._meta(page)
        return Path(self._member(page, meta[0])).name if meta else None

    def _raw(
        self, page: str, name: str, sheet: int, skiprows: int, skipfooter: int
    ) -> pd.DataFrame:
        member = self._member(page, name)
        if member.lower().endswith(".dbf"):
            return _read_dbf(self._zip.read(member))
        if member not in self._books:
            # calamine over openpyxl for .xlsx: the EIA-860 generator workbook
            # is 26,856 rows by 73 columns and openpyxl takes 127 s to parse it
            # against calamine's 27 s, for a byte-identical result. Over 26
            # report years that is the difference between a two-hour run and a
            # half-hour one, on a pipeline that rebuilds every year on every
            # run. The .xls vintages stay on xlrd, which calamine's reader does
            # not improve on at their size.
            engine = "xlrd" if member.lower().endswith(".xls") else "calamine"
            self._books[member] = pd.ExcelFile(
                io.BytesIO(self._zip.read(member)), engine=engine
            )
        return self._books[member].parse(
            sheet_name=sheet, skiprows=skiprows, skipfooter=skipfooter
        )

    def page(self, page: str) -> pd.DataFrame | None:
        """Return one page renamed to PUDL canonical column names, or None.

        Fails loudly when a column the vendored map expects is absent, or when a
        column the map does not know about is present. Both mean the layout has
        moved since the map was written, and both would otherwise show up as a
        column silently full of nulls.
        """
        meta = self._meta(page)
        if meta is None:
            return None
        name, sheet, skiprows, skipfooter = meta
        df = self._raw(page, name, sheet, skiprows, skipfooter)
        df.columns = [simplify(c) for c in df.columns]
        # data_maturity is read off the name of the file that was actually
        # opened, not the name the map recorded: on the live year they differ,
        # and the map's name is the one that is out of date.
        actual_name = Path(self._member(page, name)).name
        # A workbook can repeat a header; keep the first, which is what the map
        # was written against.
        df = df.loc[:, ~pd.Index(df.columns).duplicated()]

        colmap = dict(_column_map(self.form, page)[str(self.year)])
        override = constants.MAP_OVERRIDES.value.get(
            (self.form, page, self.year), {}
        )
        # PUDL reads its own archived copy of each release; eia.gov has re-issued
        # a few archive ZIPs since, so a handful of vintages carry a header PUDL's
        # map does not expect. Each divergence is recorded rather than tolerated:
        # `rename` re-points a canonical column at the header this copy actually
        # has, `drop` removes a canonical column this copy does not carry.
        for canonical in override.get("drop", ()):
            colmap = {k: v for k, v in colmap.items() if v != canonical}
        for raw, canonical in override.get("rename", {}).items():
            colmap = {k: v for k, v in colmap.items() if v != canonical}
            colmap[raw] = canonical

        present = {
            raw: canonical
            for raw, canonical in colmap.items()
            if raw in df.columns
        }
        missing = sorted(set(colmap) - set(present))
        extra = sorted(
            c
            for c in df.columns
            if c not in colmap and not c.startswith(("reserved", "unnamed"))
        )
        if missing or extra:
            raise ValueError(
                f"{self.form}/{page}/{self.year} layout has moved: "
                f"expected-but-absent={missing} present-but-unmapped={extra}. "
                f"Add an entry to constants.MAP_OVERRIDES after checking the file."
            )
        df = df.rename(columns=present)[list(present.values())]
        df["data_maturity"] = data_maturity(actual_name)
        df["report_year"] = self.year
        return df


def _read_dbf(payload: bytes) -> pd.DataFrame:
    """Read a DBF payload. The 2001-2003 EIA-860 files are dBase, not Excel."""
    import dbfread

    with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
        tmp.write(payload)
        name = tmp.name
    try:
        table = dbfread.DBF(name, char_decode_errors="ignore", load=True)
        return pd.DataFrame(iter(table))
    finally:
        os.unlink(name)


def data_maturity(file_name: str) -> str:
    """How settled the numbers in a file are, from its published name.

    EIA marks an early release in the file name and nowhere inside the file, so
    the name is the only signal. Reproduces PUDL's rule:

    * ``provisional`` — an early release, later superseded by a final revision;
    * ``incremental_ytd`` — a within-year EIA-923 monthly file, which carries the
      months published so far and is superseded by every later release;
    * ``final`` — everything else.
    """
    lowered = file_name.lower()
    if "early" in lowered:
        return "provisional"
    if "eia923_schedule" in lowered and "final" not in lowered:
        return "incremental_ytd"
    return "final"


# --------------------------------------------------------------------------
# Value cleaning
# --------------------------------------------------------------------------


def clean_text(series: pd.Series) -> pd.Series:
    """Strip, collapse internal whitespace, and turn EIA's NA stand-ins into None.

    EIA-923 writes a bare ``.`` where a respondent reported nothing, and both
    forms use whitespace-only cells and runs of hyphens for the same thing. A
    naive read leaves ``"."`` sitting in a code column, where it looks like a
    real value and defeats every downstream count.
    """

    # pd.isna on the scalar, not `isinstance(v, float)`: pandas has three
    # missing values and only one of them is a float. A blank cell in a column
    # pandas types as datetime is pd.NaT and one in a nullable column is pd.NA,
    # neither of which is a float — str() would render them "NaT" and "<NA>",
    # and _NA_RE matches neither, so the literal would survive into a STRING
    # column as a real value. Not observed in the 2001-2026 corpus (every such
    # column comes back as object dtype), but one cleanly-typed EIA release
    # would be enough.
    def _text(value: Any) -> str | None:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            # pd.isna raises on a list-like; such a cell is not missing.
            pass
        return str(value)

    out = series.astype("object").map(_text)
    out = out.map(lambda v: None if v is None else _WS.sub(" ", v).strip())
    return out.map(lambda v: None if v is None or _NA_RE.match(v) else v)


def as_id(series: pd.Series) -> pd.Series:
    """Render a numeric identifier as a plain integer string.

    Plant, utility and operator ids are numbers in the workbook, so pandas hands
    them back as floats and ``str()`` would produce ``"3.0"`` for plant 3. They
    are identifiers, not quantities, so they are STRING in the schema — but the
    string has to be ``"3"``, or nothing joins.
    """
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


def as_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(clean_text(series), errors="coerce")


def as_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(clean_text(series), errors="coerce").astype("Int64")


def year_month_date(year: pd.Series, month: pd.Series) -> pd.Series:
    """Build ``YYYY-MM-01`` from the separate year and month the forms publish.

    EIA-860 never publishes a day for an operating, retirement or planned date —
    only a month and a year — so the day is fixed at 01 and the column's
    observations say so. A row with no year yields null even when a month is
    present; a row with a year and no month yields January, which is what the
    source's own annual aggregates assume.
    """
    y = as_int(year)
    m = as_int(month).fillna(1)
    m = m.where(m.between(1, 12), 1)
    out = pd.Series([None] * len(y), index=y.index, dtype="object")
    ok = y.notna() & y.between(1850, 2100)
    out[ok] = [
        f"{int(a):04d}-{int(b):02d}-01"
        for a, b in zip(y[ok], m[ok], strict=True)
    ]
    return out


# --------------------------------------------------------------------------
# Geography
# --------------------------------------------------------------------------


def _norm_county(name: str) -> str:
    text = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    )
    text = _NON_ALNUM.sub(" ", text).lower().strip()
    text = _WS.sub(" ", text)
    return _COUNTY_SUFFIX.sub("", text).strip()


@cache
def _directory() -> tuple[dict[str, str], dict[tuple[str, str], str]]:
    """``({state abbreviation: state FIPS}, {(abbrev, normalised county): FIPS})``.

    Read from the committed export of ``br_bd_diretorios_us``. The forms publish
    a county *name*, never a FIPS code, so the only way to reach the directory is
    by name — and the names do not match exactly, hence ``_norm_county``.
    """
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
    """Two-letter postal abbreviation -> state FIPS, or null."""
    states, _ = _directory()
    return clean_text(series).map(
        lambda v: states.get(v.upper()) if isinstance(v, str) else None
    )


def county_id_for(state: pd.Series, county: pd.Series) -> pd.Series:
    """(state abbreviation, county name) -> 5-digit county FIPS, or null."""
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


def county_id_from_fips(state: pd.Series, county_fips: pd.Series) -> pd.Series:
    """(state abbreviation, 3-digit county code) -> 5-digit county FIPS.

    EIA-923's coal mine county *is* published as a FIPS code, but as the bare
    three-digit county part with the leading zeros stripped by Excel, so it has
    to be zero-padded and prefixed with the state's own FIPS code.
    """
    states, _ = _directory()
    st = clean_text(state)
    cy = as_id(county_fips)
    out = []
    for a, b in zip(st, cy, strict=True):
        prefix = states.get(a.upper()) if isinstance(a, str) else None
        out.append(
            f"{prefix}{int(b):03d}"
            if prefix and b is not None and str(b).isdigit()
            else None
        )
    return pd.Series(out, index=state.index, dtype="object")


# --------------------------------------------------------------------------
# Table builders
# --------------------------------------------------------------------------


def _repair_codes(frame: pd.DataFrame, table: str) -> pd.DataFrame:
    """Apply the vendored code repairs to every coded column of a table."""
    for column, vocabulary in constants.CODED_COLUMNS.value.get(
        table, {}
    ).items():
        if column in frame.columns:
            frame[column] = frame[column].map(
                lambda v, voc=vocabulary: repair_code(v, voc)
            )
    return frame


@cache
def _status_groups() -> dict[str, str]:
    """Operational status code -> existing / proposed / retired.

    Taken from the vendored vocabulary's own ``operational_status`` field rather
    than hardcoded, so the mapping cannot drift from the code list it describes.
    """
    return {
        str(row["code"]): row["operational_status"]
        for row in codes()["core_eia__codes_operational_status"]["rows"]
        if row.get("operational_status")
    }


def status_group_for(code: object) -> str | None:
    """Which of the three generator sheets a pre-2009 row would have belonged to."""
    if not isinstance(code, str) or not code.strip():
        return None
    return _status_groups().get(code.strip().upper())


def build_plant(reader: YearReader) -> pd.DataFrame:
    """EIA-860 Schedule 2 — one row per plant per report year."""
    raw = reader.page("plant")
    if raw is None:
        return pd.DataFrame()
    src = {c: clean_text(raw[c]) for c in raw.columns}

    def col(name: str) -> pd.Series:
        if name in src:
            return src[name]
        return pd.Series([None] * len(raw), index=raw.index, dtype="object")

    state = col("state")
    county = col("county")
    out = pd.DataFrame(
        {
            "year": raw["report_year"],
            "plant_id": as_id(raw["plant_id_eia"]),
            "plant_name": col("plant_name_eia"),
            "utility_id": as_id(raw["utility_id_eia"]),
            "utility_name": col("utility_name_eia"),
            "state_abbreviation": state.map(
                lambda v: v.upper() if isinstance(v, str) else None
            ),
            "state_id": state_id_for(state),
            "county_name": county,
            "county_id": county_id_for(state, county),
            "city": col("city"),
            "street_address": col("street_address"),
            "zip_code": as_id(col("zip_code")),
            "latitude": as_float(col("latitude")),
            "longitude": as_float(col("longitude")),
            "balancing_authority_code": col("balancing_authority_code_eia"),
            "balancing_authority_name": col("balancing_authority_name_eia"),
            "nerc_region": col("nerc_region"),
            "iso_rto_code": col("iso_rto_code"),
            "sector_id": as_id(col("sector_id_eia")),
            "sector_name": col("sector_name_eia"),
            "primary_purpose_naics_code": as_id(
                col("primary_purpose_id_naics")
            ),
            "regulatory_status_code": col("regulatory_status_code"),
            "service_area": col("service_area"),
            "water_source": col("water_source"),
            "grid_voltage_1_kv": as_float(col("grid_voltage_1_kv")),
            "grid_voltage_2_kv": as_float(col("grid_voltage_2_kv")),
            "grid_voltage_3_kv": as_float(col("grid_voltage_3_kv")),
            "ferc_cogen_status": col("ferc_cogen_status"),
            "ferc_small_power_producer": col("ferc_small_power_producer"),
            "ferc_exempt_wholesale_generator": col(
                "ferc_exempt_wholesale_generator"
            ),
            "transmission_distribution_owner_id": as_id(
                col("transmission_distribution_owner_id")
            ),
            "transmission_distribution_owner_name": col(
                "transmission_distribution_owner_name"
            ),
            "transmission_distribution_owner_state": col(
                "transmission_distribution_owner_state"
            ),
            "natural_gas_pipeline_name_1": col("natural_gas_pipeline_name_1"),
            "natural_gas_pipeline_name_2": col("natural_gas_pipeline_name_2"),
            "natural_gas_pipeline_name_3": col("natural_gas_pipeline_name_3"),
            "natural_gas_local_distribution_company": col(
                "natural_gas_local_distribution_company"
            ),
            "natural_gas_storage": col("natural_gas_storage"),
            "liquefied_natural_gas_storage": col(
                "liquefied_natural_gas_storage"
            ),
            "ash_impoundment": col("ash_impoundment"),
            "ash_impoundment_lined": col("ash_impoundment_lined"),
            "ash_impoundment_status": col("ash_impoundment_status"),
            "energy_storage": col("energy_storage"),
            "has_net_metering": col("has_net_metering"),
            "datum": col("datum"),
            "data_maturity": raw["data_maturity"],
        }
    )
    out = out[out["plant_id"].notna()]
    return _repair_codes(out, "plant")


def build_generator(reader: YearReader) -> pd.DataFrame:
    """EIA-860 Schedule 3 — one row per generator per report year.

    From 2009 the form splits generators across three sheets (operable, proposed,
    retired and cancelled) and before that carries operable and retired together
    in one sheet with proposed generators in a separate file. The three sheets
    are unioned into one table and ``generator_status_group`` records which sheet
    a row came from; on the pre-2009 combined sheet the group is derived from
    ``operational_status_code`` instead, so the column means the same thing over
    the whole record.
    """
    frames = []
    for page, group in constants.GENERATOR_PAGE_GROUP.value.items():
        raw = reader.page(page)
        if raw is None or raw.empty:
            continue
        raw = raw.copy()
        raw["_group"] = group
        frames.append(raw)
    if not frames:
        return pd.DataFrame()
    # concat aligns on column name, never on position: the pages carry different
    # column sets and a positional union would silently shift values between
    # columns.
    raw = pd.concat(frames, ignore_index=True, sort=False)

    def col(name: str) -> pd.Series:
        if name in raw.columns:
            return clean_text(raw[name])
        return pd.Series([None] * len(raw), index=raw.index, dtype="object")

    state = col("state")
    county = col("county")
    # Repair the status code BEFORE deriving the group from it. The pre-2009
    # sheets file standby generators as `BU`, which PUDL's code_fixes maps to
    # `SB`; deriving the group from the raw value left 332 rows across 2004-2006
    # with no group at all, because `BU` is not in the vocabulary the group
    # mapping reads.
    status = col("operational_status_code").map(
        lambda v: repair_code(v, "core_eia__codes_operational_status")
    )
    group = raw["_group"].where(
        raw["_group"].ne(""), status.map(status_group_for)
    )
    out = pd.DataFrame(
        {
            "year": raw["report_year"],
            "plant_id": as_id(raw["plant_id_eia"]),
            "generator_id": col("generator_id"),
            "plant_name": col("plant_name_eia"),
            "utility_id": as_id(raw["utility_id_eia"]),
            "utility_name": col("utility_name_eia"),
            "state_abbreviation": state.map(
                lambda v: v.upper() if isinstance(v, str) else None
            ),
            "state_id": state_id_for(state),
            "county_name": county,
            "county_id": county_id_for(state, county),
            "generator_status_group": group,
            "operational_status_code": status,
            "unit_id": col("unit_id_eia"),
            "ownership_code": col("ownership_code"),
            "sector_id": as_id(col("sector_id_eia")),
            "sector_name": col("sector_name_eia"),
            "prime_mover_code": col("prime_mover_code"),
            "technology_description": col("technology_description"),
            "capacity_mw": as_float(col("capacity_mw")),
            "summer_capacity_mw": as_float(col("summer_capacity_mw")),
            "winter_capacity_mw": as_float(col("winter_capacity_mw")),
            "minimum_load_mw": as_float(col("minimum_load_mw")),
            "planned_new_capacity_mw": as_float(
                col("planned_new_capacity_mw")
            ),
            "turbines_num": as_int(col("turbines_num")),
            "turbines_inverters_hydrokinetics": as_int(
                col("turbines_inverters_hydrokinetics")
            ),
            "energy_source_code_1": col("energy_source_code_1"),
            "energy_source_code_2": col("energy_source_code_2"),
            "energy_source_code_3": col("energy_source_code_3"),
            "energy_source_code_4": col("energy_source_code_4"),
            "energy_source_code_5": col("energy_source_code_5"),
            "energy_source_code_6": col("energy_source_code_6"),
            "can_burn_multiple_fuels": col("can_burn_multiple_fuels"),
            "can_cofire_fuels": col("can_cofire_fuels"),
            "switch_oil_gas": col("switch_oil_gas"),
            "operating_date": year_month_date(
                col("generator_operating_year"),
                col("generator_operating_month"),
            ),
            "retirement_date": year_month_date(
                col("generator_retirement_year"),
                col("generator_retirement_month"),
            ),
            "planned_retirement_date": year_month_date(
                col("planned_generator_retirement_year"),
                col("planned_generator_retirement_month"),
            ),
            "planned_operating_date": year_month_date(
                col("current_planned_generator_operating_year"),
                col("current_planned_generator_operating_month"),
            ),
            "carbon_capture": col("carbon_capture"),
            "solid_fuel_gasification": col("solid_fuel_gasification"),
            "pulverized_coal_tech": col("pulverized_coal_tech"),
            "fluidized_bed_tech": col("fluidized_bed_tech"),
            "stoker_tech": col("stoker_tech"),
            "other_combustion_tech": col("other_combustion_tech"),
            "subcritical_tech": col("subcritical_tech"),
            "supercritical_tech": col("supercritical_tech"),
            "ultrasupercritical_tech": col("ultrasupercritical_tech"),
            "duct_burners": col("duct_burners"),
            "bypass_heat_recovery": col("bypass_heat_recovery"),
            "associated_combined_heat_power": col(
                "associated_combined_heat_power"
            ),
            "topping_bottoming_code": col("topping_bottoming_code"),
            "synchronized_transmission_grid": col(
                "synchronized_transmission_grid"
            ),
            "rto_iso_lmp_node_id": col("rto_iso_lmp_node_id"),
            "data_maturity": raw["data_maturity"],
        }
    )
    out = out[out["plant_id"].notna() & out["generator_id"].notna()]
    return _repair_codes(out, "generator")


def build_generation_fuel(reader: YearReader) -> pd.DataFrame:
    """EIA-923 page 1 — melted to one row per plant, fuel, prime mover and month.

    The form publishes this page **wide**: one row per (plant, energy source,
    prime mover) carrying twelve columns for each of six measures, plus an annual
    total per measure. Shipping it wide would make the single most useful table
    in the dataset unusable in SQL, so it is melted to long here and the annual
    totals are dropped — they are the sum of the twelve monthly columns and would
    otherwise be double counted by anyone summing the table.

    A (row, month) pair where every one of the six measures is null is dropped.
    That is what makes a partial year work: the 2026 file carries twelve months
    of columns but only reports through the latest published month, and the
    unreported months are empty rather than zero. A reported **zero** is kept —
    it means the plant ran no fuel that month, which is a fact.
    """
    raw = reader.page("generation_fuel")
    if raw is None or raw.empty:
        return pd.DataFrame()

    def col(name: str) -> pd.Series:
        if name in raw.columns:
            return clean_text(raw[name])
        return pd.Series([None] * len(raw), index=raw.index, dtype="object")

    state = col("plant_state")
    identity = pd.DataFrame(
        {
            "year": raw["report_year"],
            "plant_id": as_id(raw["plant_id_eia"]),
            "plant_name": col("plant_name_eia"),
            "operator_id": as_id(col("operator_id")),
            "operator_name": col("operator_name"),
            "state_abbreviation": state.map(
                lambda v: v.upper() if isinstance(v, str) else None
            ),
            "state_id": state_id_for(state),
            "census_region": col("census_region"),
            "nerc_region": col("nerc_region"),
            "balancing_authority_code": col("balancing_authority_code_eia"),
            "sector_id": as_id(col("sector_id_eia")),
            "sector_name": col("sector_name_eia"),
            "naics_code": as_id(col("naics_code")),
            "prime_mover_code": col("prime_mover_code"),
            "energy_source_code": col("energy_source_code"),
            "fuel_type_code_agg": col("fuel_type_code_agg"),
            "nuclear_unit_id": as_id(col("nuclear_unit_id")),
            "associated_combined_heat_power": col(
                "associated_combined_heat_power"
            ),
            "reporting_frequency_code": col("reporting_frequency_code"),
            "fuel_unit": col("fuel_unit"),
            "data_maturity": raw["data_maturity"],
        }
    )

    months = constants.MONTHS.value
    measures = constants.GENERATION_FUEL_MEASURES.value
    pieces = []
    for index, month in enumerate(months, start=1):
        block = identity.copy()
        block.insert(1, "month", index)
        empty = pd.Series(True, index=raw.index)
        for source, target in measures.items():
            column = f"{source}_{month}"
            values = (
                as_float(raw[column])
                if column in raw.columns
                else pd.Series(
                    [None] * len(raw), index=raw.index, dtype="float64"
                )
            )
            block[target] = values
            empty &= values.isna()
        pieces.append(block[~empty.to_numpy()])
    out = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    if out.empty:
        return out
    out = out[out["plant_id"].notna()]
    return _repair_codes(out, "generation_fuel")


def build_fuel_receipts_costs(reader: YearReader) -> pd.DataFrame:
    """EIA-923 Schedule 2 Part C — one row per fuel delivery to a plant.

    Already long: the form reports each delivery with its own year and month.
    Two published-value repairs apply here, both documented by EIA and by PUDL:
    the delivered fuel cost is published in **cents** per MMBtu and is converted
    to dollars, and a suppressed or unreported value arrives as a bare ``.``.
    """
    raw = reader.page("fuel_receipts_costs")
    if raw is None or raw.empty:
        return pd.DataFrame()

    def col(name: str) -> pd.Series:
        if name in raw.columns:
            return clean_text(raw[name])
        return pd.Series([None] * len(raw), index=raw.index, dtype="object")

    plant_state = col("plant_state")
    mine_state = col("state")
    out = pd.DataFrame(
        {
            "year": raw["report_year"],
            "month": as_int(col("report_month")),
            "plant_id": as_id(raw["plant_id_eia"]),
            "plant_name": col("plant_name_eia"),
            "state_abbreviation": plant_state.map(
                lambda v: v.upper() if isinstance(v, str) else None
            ),
            "state_id": state_id_for(plant_state),
            "operator_id": as_id(col("operator_id")),
            "operator_name": col("operator_name"),
            "balancing_authority_code": col("balancing_authority_code_eia"),
            "energy_source_code": col("energy_source_code"),
            "fuel_group_code": col("fuel_group_code"),
            "contract_type_code": col("contract_type_code"),
            "contract_expiration_date": contract_expiration(
                col("contract_expiration_date")
            ),
            "supplier_name": col("supplier_name"),
            "mine_name": col("mine_name"),
            "mine_id_msha": as_id(col("mine_id_msha")),
            "mine_type_code": col("mine_type_code"),
            "mine_state_abbreviation": mine_state.map(
                lambda v: v.upper() if isinstance(v, str) else None
            ),
            "mine_county_id": county_id_from_fips(
                mine_state, col("county_id_fips")
            ),
            "primary_transportation_mode_code": col(
                "primary_transportation_mode_code"
            ),
            "secondary_transportation_mode_code": col(
                "secondary_transportation_mode_code"
            ),
            "natural_gas_transport_code": col("natural_gas_transport_code"),
            "natural_gas_delivery_contract_type_code": col(
                "natural_gas_delivery_contract_type_code"
            ),
            "fuel_received_units": as_float(col("fuel_received_units")),
            "fuel_mmbtu_per_unit": as_float(col("fuel_mmbtu_per_unit")),
            # Published in cents per MMBtu; converted to dollars.
            "fuel_cost_per_mmbtu": as_float(col("fuel_cost_per_mmbtu")) / 100,
            "sulfur_content_pct": as_float(col("sulfur_content_pct")),
            "ash_content_pct": as_float(col("ash_content_pct")),
            "moisture_content_pct": as_float(col("moisture_content_pct")),
            "mercury_content_ppm": as_float(col("mercury_content_ppm")),
            "chlorine_content_ppm": as_float(col("chlorine_content_ppm")),
            "regulated": col("regulated"),
            "reporting_frequency_code": col("reporting_frequency_code"),
            "data_maturity": raw["data_maturity"],
        }
    )
    out = out[out["plant_id"].notna()]
    return _repair_codes(out, "fuel_receipts_costs")


def contract_expiration(series: pd.Series) -> pd.Series:
    """``MMYY`` (or ``MYY``) as published -> ``YYYY-MM-01``.

    The form asks for the contract expiry as a four-digit month-and-year, and
    Excel strips the leading zero, so January 2015 arrives as ``115`` and October
    2015 as ``1015``. The two-digit year is resolved against a 1990 pivot, which
    is safe here: the earliest EIA-923 delivery is 2008 and no coal contract in
    the record expires before 1990 or after 2089.
    """

    def one(value):
        if value is None:
            return None
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        if not text.isdigit() or len(text) not in (3, 4):
            return None
        text = text.zfill(4)
        month, year = int(text[:2]), int(text[2:])
        if not 1 <= month <= 12:
            return None
        return (
            f"{1900 + year if year >= 90 else 2000 + year:04d}-{month:02d}-01"
        )

    return series.map(one)


# --------------------------------------------------------------------------
# Writing all-STRING parquet
# --------------------------------------------------------------------------


BUILDERS = {
    "plant": build_plant,
    "generator": build_generator,
    "generation_fuel": build_generation_fuel,
    "fuel_receipts_costs": build_fuel_receipts_costs,
}

TABLE_FORM = {
    "plant": "eia860",
    "generator": "eia860",
    "generation_fuel": "eia923",
    "fuel_receipts_costs": "eia923",
}


def _stringify(series: pd.Series, bq_type: str) -> list[str | None]:
    """Render one typed column as the strings the staging table will hold.

    Types are passed through before stringifying, never ``astype(str)``: that
    renders a missing value as the literal ``"nan"``, which ``safe_cast`` will
    not turn back into NULL, and renders the integer 1959 read out of Excel as
    ``"1959.0"``, which ``safe_cast(... as int64)`` turns into NULL.
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
    """Write one year's rows as ``<out_dir>/year=<year>/data.parquet``.

    A year with no rows writes **no file**. A zero-row parquet would be the first
    file ``dump_header`` finds for some table and BigQuery would infer a non-
    string schema from it, after which every real partition fails to read.
    """
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


def clean_year(
    year: int,
    input_dir: Path,
    output_dir: Path,
    tables: list[str] | None = None,
) -> dict[str, int]:
    """Clean one report year into partitioned parquet, one partition per table."""
    tables = tables or list(constants.DATA_TABLES.value)
    counts: dict[str, int] = {}
    for form in ("eia860", "eia923"):
        wanted = [t for t in tables if TABLE_FORM[t] == form]
        if not wanted:
            continue
        path = source_zip(input_dir, form, year)
        if not path.exists():
            continue
        with YearReader(form, year, path) as reader:
            for table in wanted:
                frame = BUILDERS[table](reader)
                counts[table] = write_partition(
                    frame, load_cols(table), Path(output_dir) / table, year
                )
    return counts


def clean_all(
    input_dir: Path,
    output_dir: Path,
    years: list[int] | None = None,
    tables: list[str] | None = None,
    log=print,
) -> dict[str, int]:
    """Clean every requested year. Totals per table are returned."""
    years = years or sorted(
        {
            int(p.stem.rsplit("_", 1)[1])
            for form in ("eia860", "eia923")
            for p in (Path(input_dir) / form).glob(f"{form}_*.zip")
        }
    )
    totals: dict[str, int] = {}
    for year in years:
        counts = clean_year(year, input_dir, output_dir, tables)
        for table, n in counts.items():
            totals[table] = totals.get(table, 0) + n
        log(
            f"{year}: "
            + ", ".join(f"{t}={n:,}" for t, n in sorted(counts.items()))
        )
    return totals


def assert_all_string(path: Path) -> None:
    """Fail loudly if any parquet file under ``path`` carries a non-string column."""
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
            raise AssertionError(
                f"{file}: zero rows — an empty partition poisons the staging schema"
            )


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------


@cache
def _labels(vocabulary: str) -> dict[str, str]:
    """Code -> human-readable description, from the vendored PUDL vocabulary."""
    return {
        str(row["code"]): (
            row.get("description") or row.get("label") or str(row["code"])
        )
        for row in codes()[vocabulary]["rows"]
    }


# Vocabularies the forms use that PUDL does not carry a code table for. They are
# short, closed and published in the form instructions, so they are written out
# here rather than left as bare codes a reader cannot interpret.
LOCAL_LABELS: dict[str, dict[str, str]] = {
    "data_maturity": {
        "final": "Definitive file",
        "provisional": "Early release, later superseded by a final revision",
        "incremental_ytd": "Within-year monthly file, superseded by every later release",
    },
    "generator_status_group": {
        "existing": "Generator reported as operable, standby or out of service",
        "proposed": "Generator reported as planned, under construction or cancelled",
        "retired": "Generator reported as retired",
    },
    "_yes_no": {
        "Y": "Yes",
        "N": "No",
        "Yes": "Yes",
        "No": "No",
        "X": "Yes",
    },
    "ownership_code": {
        "S": "Single ownership by the respondent",
        "J": "Jointly owned with another entity",
        "W": "Wholly owned by an entity other than the respondent",
    },
    "topping_bottoming_code": {
        "T": "Topping cycle cogeneration",
        "B": "Bottoming cycle cogeneration",
    },
    "census_region": {
        "NEW": "New England",
        "MAT": "Middle Atlantic",
        "ENC": "East North Central",
        "WNC": "West North Central",
        "SAT": "South Atlantic",
        "ESC": "East South Central",
        "WSC": "West South Central",
        "MTN": "Mountain",
        "PCC": "Pacific Contiguous",
        "PCN": "Pacific Noncontiguous",
    },
    "natural_gas_transport_code": {
        "F": "Firm transportation service",
        "I": "Interruptible transportation service",
    },
    "fuel_group_code": {
        "Coal": "Coal",
        "Natural Gas": "Natural gas",
        "Petroleum": "Petroleum",
        "Petroleum Coke": "Petroleum coke",
        "Other Gas": "Other gas",
    },
}


def dicionario_label(table: str, column: str, value: str) -> str:
    """The human-readable meaning of one stored code, or the value itself."""
    vocabulary = constants.CODED_COLUMNS.value.get(table, {}).get(column)
    if vocabulary:
        label = _labels(vocabulary).get(value)
        if label:
            return label
    for key in (column, "_yes_no"):
        if key in LOCAL_LABELS and value in LOCAL_LABELS[key]:
            return LOCAL_LABELS[key][value]
    return value


def build_dicionario(output_dir: Path, tables: list[str] | None = None) -> int:
    """Build the dicionario table from the cleaned parquet partitions.

    Every column the architecture marks ``covered_by_dictionary`` is enumerated
    over the whole record, so ``cobertura_temporal`` shows when each code was
    actually in use — which is what makes the vocabulary drift visible, and is
    the only place a reader can see that ``prime_mover_code`` gained ``BA`` and
    ``WS`` decades after the rest of the list.
    """
    import pyarrow.dataset as ds

    tables = tables or list(constants.DATA_TABLES.value)
    rows = []
    for table in tables:
        part = Path(output_dir) / table
        if not part.exists():
            continue
        columns = [c.name for c in load_cols(table) if c.covered_by_dictionary]
        if not columns:
            continue
        # No hive partitioning: the year is already a column inside every file,
        # and letting pyarrow also derive it from the directory name yields an
        # int32 partition field that will not merge with the string column.
        data = ds.dataset(part, format="parquet")
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
        for (column, value), years in sorted(seen.items()):
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": value,
                    "cobertura_temporal": f"{min(years)}(1){max(years)}",
                    "valor": dicionario_label(table, column, value),
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
    pq.write_table(
        pa.Table.from_pandas(
            frame.astype("string"), preserve_index=False
        ).cast(pa.schema([(c, pa.string()) for c in frame.columns])),
        out / "data.parquet",
        compression="snappy",
    )
    return len(rows)


# --------------------------------------------------------------------------
# Source coverage, for the poll
# --------------------------------------------------------------------------


def source_max_date(table: str, output_dir: Path) -> str:
    """The latest period the cleaned table actually covers, as ``YYYY-MM-DD``.

    The annual EIA-860 tables report a year, so the answer is the last report
    year with rows. The monthly EIA-923 tables report a month, and a partial year
    is normal — the current-year file carries twelve months of columns but only
    reports through the latest published month — so the answer is the last month
    that actually has data, not December of the last year.
    """
    import pyarrow.dataset as ds

    part = Path(output_dir) / table
    data = ds.dataset(part, format="parquet")
    if TABLE_FORM[table] == "eia860":
        years = {
            int(y)
            for batch in data.to_batches(columns=["year"])
            for y in batch.column(0).to_pylist()
            if y
        }
        if not years:
            raise ValueError(
                f"{table}: no rows carry a report year, so the source has no "
                "coverage date to poll against"
            )
        return f"{max(years)}-01-01"
    latest = (0, 0)
    for batch in data.to_batches(columns=["year", "month"]):
        for year, month in zip(
            batch.column(0).to_pylist(),
            batch.column(1).to_pylist(),
            strict=True,
        ):
            if year and month:
                latest = max(latest, (int(year), int(month)))
    if latest == (0, 0):
        raise ValueError(
            f"{table}: no rows carry both a year and a month, so the source has "
            "no coverage date to poll against"
        )
    return f"{latest[0]:04d}-{latest[1]:02d}-01"
