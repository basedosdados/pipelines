"""Pure download and cleaning helpers for us_census_cog.

No Prefect imports live here: the one-shot onboarding scripts under
``models/us_census_cog/code/`` and the recurring flow both import these
functions, so the transform exists in exactly one place.
"""

from __future__ import annotations

import csv
import io
import re
import tempfile
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import NamedTuple

import requests

from pipelines.datasets.us_census_cog.constants import (
    EMPLOYMENT_BUNDLES,
    EMPLOYMENT_CENSUS_YEARS,
    EMPLOYMENT_DATA_LAYOUTS,
    EMPLOYMENT_UNIT_LAYOUT,
    EMPLOYMENT_UNIT_LAYOUT_PID6,
    EMPLOYMENT_YEARS,
    FINANCE_DATA_LAYOUTS,
    FINANCE_HISTORICAL,
    FINANCE_MODERN_FILES,
    FINANCE_UNIT_LAYOUTS,
    FINANCE_YEARS,
    GOVS_TO_FIPS_STATE,
    GUS_FILES,
    GUS_YEARS,
    WWW2,
    constants,
)

REJECTED = b"<title>Request Rejected</title>"


class SourceUnavailableError(RuntimeError):
    """A source file could not be fetched after every retry."""


def fetch(url: str, timeout: int = 900) -> bytes:
    """Fetch a URL, working around the www2.census.gov firewall.

    ``www2.census.gov`` answers HTTP 200 with a "Request Rejected" HTML body for
    an arbitrary subset of valid URLs, and caches that rejection against the
    exact URL. A genuinely missing file returns 404 instead. So a 200 whose body
    is the rejection page is always the firewall and never a missing file, and
    appending a cache-busting query string gets the real bytes.

    Args:
        url: Absolute URL to fetch.
        timeout: Per-attempt timeout in seconds.

    Returns:
        The response body.

    Raises:
        SourceUnavailable: The URL 404s, or every attempt was rejected.
    """
    headers = {"User-Agent": constants.USER_AGENT.value}
    last = ""
    for attempt in range(constants.HTTP_RETRIES.value):
        target = url if attempt == 0 else f"{url}?attempt={attempt}"
        try:
            r = requests.get(target, headers=headers, timeout=timeout)
        except requests.RequestException as exc:  # network flake
            last = str(exc)
            continue
        if r.status_code == 404:
            raise SourceUnavailableError(f"404 Not Found: {url}")
        if r.status_code == 200 and REJECTED not in r.content[:2048]:
            return r.content
        last = f"HTTP {r.status_code}, {len(r.content)} bytes"
    raise SourceUnavailableError(f"{url}: {last}")


def head_ok(url: str) -> bool:
    """Report whether a URL resolves to a real file.

    Used to pick between the several names a given year's employment files are
    published under.
    """
    headers = {"User-Agent": constants.USER_AGENT.value}
    try:
        r = requests.head(
            url, headers=headers, timeout=60, allow_redirects=True
        )
    except requests.RequestException:
        return False
    return r.status_code == 200 and "text/html" not in r.headers.get(
        "content-type", ""
    )


# --------------------------------------------------------------------------
# Source URL resolution
# --------------------------------------------------------------------------


def gus_url(year: int) -> str:
    """URL of the Government Units Survey workbook for ``year``."""
    return f"{WWW2}/gus/datasets/{year}/{GUS_FILES[year]}"


def employment_urls(year: int) -> list[str]:
    """Candidate URLs holding the employment files for ``year``.

    Later years ship one bundle; 1992-2011 ship a zipped data/directory pair,
    with an uncompressed ``.dat``/``.id`` fallback and a ``c`` stem in census
    years.
    """
    if year in EMPLOYMENT_BUNDLES:
        return [f"{WWW2}/apes/datasets/{EMPLOYMENT_BUNDLES[year]}"]
    yy = f"{year % 100:02d}"
    stem = f"{yy}cemp" if year in EMPLOYMENT_CENSUS_YEARS else f"{yy}emp"
    base = f"{WWW2}/apes/datasets/{year}/annual-apes"
    return [
        f"{base}/{stem}st.zip",
        f"{base}/{stem}id.zip",
        # 2004 publishes no standalone data file and 2005 no standalone unit
        # directory; both sit inside the year bundle, as nested zips.
        f"{base}/{year}_downloadable_data.zip",
    ]


def finance_url(year: int) -> str:
    """URL of the finance individual-unit file for a 2013-2018 ``year``."""
    return f"{WWW2}/gov-finances/datasets/{FINANCE_MODERN_FILES[year]}"


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------


def _save(path: Path, url: str) -> Path:
    """Fetch ``url`` into ``path`` unless it is already there."""
    if path.exists() and path.stat().st_size > 0:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_bytes(fetch(url))
    tmp.rename(path)
    return path


def download_gus(input_dir: Path, years: Iterable[int]) -> list[Path]:
    """Download the Government Units workbooks for ``years``."""
    return [
        _save(input_dir / "gus" / f"govt_units_{y}.zip", gus_url(y))
        for y in years
    ]


def download_employment(input_dir: Path, years: Iterable[int]) -> list[Path]:
    """Download every employment archive for ``years``.

    A year is satisfied when at least one candidate URL resolves; the several
    naming schemes are tried in turn and the misses are ignored, because no year
    publishes under all of them.
    """
    out: list[Path] = []
    for year in years:
        got = False
        for url in employment_urls(year):
            name = url.rsplit("/", 1)[-1].replace("%20", "_")
            try:
                out.append(_save(input_dir / "apes" / f"{year}_{name}", url))
                got = True
            except SourceUnavailableError:
                continue
        if not got:
            raise SourceUnavailableError(
                f"no employment file found for {year}"
            )
    return out


def download_finance(input_dir: Path, years: Iterable[int]) -> list[Path]:
    """Download the finance archives covering ``years``."""
    out: list[Path] = []
    if any(y <= 2012 for y in years):
        out.append(
            _save(
                input_dir / "fin" / "IndFin_1967_2012.zip", FINANCE_HISTORICAL
            )
        )
    for year in sorted(y for y in years if y in FINANCE_MODERN_FILES):
        name = finance_url(year).rsplit("/", 1)[-1]
        out.append(
            _save(input_dir / "fin" / f"{year}_{name}", finance_url(year))
        )
    return out


# --------------------------------------------------------------------------
# Archive helpers
# --------------------------------------------------------------------------


def zip_members(path: Path, suffixes: tuple[str, ...]) -> list[str]:
    """Names of the members of ``path`` ending in one of ``suffixes``."""
    with zipfile.ZipFile(path) as zf:
        return [
            n
            for n in zf.namelist()
            if n.lower().endswith(suffixes) and not n.endswith("/")
        ]


def read_member(path: Path, member: str) -> bytes:
    """Read one member out of a zip archive."""
    with zipfile.ZipFile(path) as zf:
        return zf.read(member)


def read_lines(path: Path, member: str | None = None) -> Iterator[str]:
    """Yield decoded lines from a plain, zipped or doubly-zipped file.

    The 2004 and 2005 employment files ship as zip archives nested inside the
    year bundle, so a member whose own name ends in ``.zip`` is opened in turn.
    """
    if member is None:
        raw = path.read_bytes()
    else:
        raw = read_member(path, member)
        if member.lower().endswith(".zip"):
            inner = zipfile.ZipFile(io.BytesIO(raw))
            names = [n for n in inner.namelist() if not n.endswith("/")]
            if len(names) != 1:
                raise ValueError(
                    f"{path.name}:{member} holds {len(names)} files"
                )
            raw = inner.read(names[0])
    text = raw.decode("latin-1")
    for line in text.splitlines():
        if line.strip():
            yield line


def read_csv_rows(data: bytes) -> Iterator[list[str]]:
    """Yield rows from comma-delimited bytes, header included."""
    yield from csv.reader(io.StringIO(data.decode("latin-1")))


# --------------------------------------------------------------------------
# Architecture
# --------------------------------------------------------------------------


class Col(NamedTuple):
    """One column as declared in the architecture CSV."""

    name: str
    bigquery_type: str
    original_name: str
    description: str


def load_cols(table: str) -> list[Col]:
    """Read the architecture CSV for ``table`` and return its columns in order."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"sheet_{table}.csv"
    with path.open(newline="") as fh:
        return [
            Col(
                r["name"],
                r["bigquery_type"],
                r["original_name"],
                r["description_pt"],
            )
            for r in csv.DictReader(fh)
        ]


# --------------------------------------------------------------------------
# Field parsing
# --------------------------------------------------------------------------


def cut(line: str, span: tuple[int, int]) -> str:
    """Slice a 1-based inclusive span out of a fixed-width record."""
    return line[span[0] - 1 : span[1]].strip()


def to_int(value: str | None) -> int | None:
    """Parse an integer, treating blanks and non-numeric filler as missing."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {".", "-", "NA", "N/A"}:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return None


def to_float(value: str | None) -> float | None:
    """Parse a float, treating blanks as missing."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == ".":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def clean_text(value: object) -> str | None:
    """Collapse whitespace and map empty strings to missing."""
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def geography_code(state_id: str | None, place_code: str | None) -> str | None:
    """Build a 7-digit state-plus-code geography id, dropping the pseudo-codes.

    The Census assigns ``99<county>`` in the place field to units that sit in a
    county area rather than a named geography. Those are not real codes and must
    not be emitted as a foreign key.
    """
    if not state_id or not place_code:
        return None
    code = str(place_code).strip().zfill(5)
    if not code.isdigit() or code.startswith("99") or code == "00000":
        return None
    return f"{state_id}{code}"


def split_geography(
    government_type: str | None, state_id: str | None, place_code: str | None
) -> tuple[str | None, str | None]:
    """Route the source's one geography code to the column it belongs in.

    The Census writes a single ``FIPS place`` field for both municipalities and
    townships, but the two are different code spaces: a municipality carries an
    incorporated-place code, a township a county-subdivision code. Measured on
    the 2022 government units file, every one of the 16,214 township codes is
    absent from the place directory while 19,462 of 19,491 municipal codes are
    present, so treating them as one column would put a 45% failure rate on a
    foreign key that is otherwise sound.

    Returns:
        ``(place_id, county_subdivision_id)``, at most one of them set.
    """
    code = geography_code(state_id, place_code)
    if code is None:
        return None, None
    if government_type == "3":
        return None, code
    if government_type == "2":
        return code, None
    return None, None


def pad_code(value: str | None, width: int) -> str | None:
    """Zero-pad a fixed-width code, which older files publish unpadded.

    The employment unit directory writes ``3`` where later years write ``03``,
    which would otherwise split one code into two dictionary keys.
    """
    text = clean_text(value)
    if text is None:
        return None
    return text.zfill(width) if text.isdigit() and len(text) < width else text


def fips_state(value: str | None) -> str | None:
    """Normalise a FIPS state field, treating the federal ``00`` as missing.

    The employment unit directory writes ``00`` for the federal record, which is
    not a state and is absent from every state directory.
    """
    text = clean_text(value)
    if text is None:
        return None
    text = text.zfill(2)
    return None if text == "00" else text


def county_id(state_id: str | None, county_code: str | None) -> str | None:
    """Build the 5-digit FIPS county id from its state and county parts."""
    if not state_id or not county_code:
        return None
    code = str(county_code).strip().zfill(3)
    if not code.isdigit() or code == "000":
        return None
    return f"{state_id}{code}"


def split_coded_name(value: object) -> tuple[str | None, str | None]:
    """Split a ``"91 - WATER SUPPLY UTILITY"`` label into code and name.

    Older survey years publish the code and the name in separate columns; from
    2012 on they are concatenated into one. Returns ``(None, name)`` when the
    value carries no code prefix.
    """
    text = clean_text(value)
    if text is None:
        return None, None
    if " - " in text:
        code, _, name = text.partition(" - ")
        code = code.strip()
        if code:
            return code, clean_text(name)
    return None, text


# --------------------------------------------------------------------------
# Government units (GUS)
# --------------------------------------------------------------------------

# The workbooks rename their columns from year to year. Every spelling maps to
# one clean column here; anything not listed is dropped on purpose.
GUS_RENAMES = {
    "CENSUS_ID_PID6": "government_id",
    "CENSUS_ID_GIDID": "government_id_govs",
    "CENSUS_ID": "government_id_govs",
    "NAME": "unit_name",
    "UNIT_NAME": "unit_name",
    "UNIT_TYPE": "government_type",
    "FUNCTION_NAME": "function_name",
    "FUNCTION": "function_name",
    "FUNCTION CODE": "function_code",
    "ACTIVITY_NAME": "function_name",
    "SCHOOL_LEVEL_DESCRIPTION": "school_level_code",
    "POLITICAL_CODE_DESCRIPTION": "political_description",
    "POLITICAL DESCRIPTION": "political_description",
    "TITLE": "officer_title",
    "ADDRESS1": "address_line_1",
    "ADDRESS 1": "address_line_1",
    "ADDRESS2": "address_line_2",
    "ADDRESS 2": "address_line_2",
    "CITY": "city",
    "STATE": "state_abbreviation",
    "STATE_AB": "state_abbreviation",
    "STATE AB": "state_abbreviation",
    "ZIP": "zip_code",
    "ZIP4": "zip_code_extension",
    "WEB_ADDRESS": "web_address",
    "POPULATION": "population",
    "POPULATION_YEAR": "population_year",
    "POPULATION_SOURCE_YEAR": "population_year",
    "ENROLLMENT": "school_enrollment",
    "SCHOOL_ENROLLMENT": "school_enrollment",
    "ENROLLMENT_YEAR": "enrollment_year",
    "FIPS_STATE": "state_id",
    "FIPS STATE": "state_id",
    "FIPS_COUNTY": "county_code",
    "FIPS COUNTY": "county_code",
    "FIPS_PLACE": "place_code",
    "FIPS PLACE": "place_code",
    "COUNTY_AREA_NAME": "county_area_name",
    "COUNTY AREA NAME": "county_area_name",
    "IS_ACTIVE": "is_active",
    "ACTIVE": "is_active",
    "PARENT_CENSUS_ID_PID6": "parent_government_id",
    # 1997 publishes the identifier in parts rather than as one string.
    "STATE CODE": "state_code_govs",
    "TYPE CODE": "government_type",
    "COUNTY CODE": "county_code_govs",
    "UNIT CODE": "unit_code_govs",
    "SUPP CODE": "supplement_code_govs",
}
# The worksheet a row came from is itself information: it separates independent
# governments from the dependent school systems and pension systems that the
# Census lists alongside them but does not count as governments. Each worksheet
# maps to a category and, where the workbook publishes no type of its own, to a
# type of government.
GUS_SHEET_CATEGORY = {
    "general purpose": ("general_purpose", None),
    "generalpurp": ("general_purpose", None),
    "county governments": ("general_purpose", "1"),
    "municipal governments": ("general_purpose", "2"),
    "township governments": ("general_purpose", "3"),
    "special district": ("special_district", "4"),
    "special district govs": ("special_district", "4"),
    "specialdist": ("special_district", "4"),
    "school district": ("school_district", "5"),
    "school district govs": ("school_district", "5"),
    "schooldist": ("school_district", "5"),
    "educational svc agencies": ("school_district", "5"),
    "dep school dist": ("dependent_school_system", None),
    "dep school district": ("dependent_school_system", None),
    "depschooldist": ("dependent_school_system", None),
    "state dep school systems": ("dependent_school_system", "0"),
    "public pension sys": ("public_pension_system", None),
}


def _gus_rows(path: Path) -> Iterator[tuple[str, dict]]:
    """Yield ``(sheet_title, row_dict)`` for every data row in a GUS workbook."""
    import openpyxl

    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        for sheet in workbook.worksheets:
            rows = sheet.iter_rows(values_only=True)
            header = [clean_text(h) for h in next(rows)]
            for values in rows:
                if all(v is None for v in values):
                    continue
                yield sheet.title, dict(zip(header, values, strict=False))
    finally:
        workbook.close()


def clean_government_unit(input_dir: Path, year: int) -> list[dict]:
    """Parse one year of the Government Units Survey into clean rows."""
    archive = input_dir / "gus" / f"govt_units_{year}.zip"
    members = zip_members(archive, (".xlsx",))
    if not members:
        raise SourceUnavailableError(f"no workbook inside {archive}")
    workbook = Path(tempfile.gettempdir()) / f"cog_gus_{year}.xlsx"
    workbook.write_bytes(read_member(archive, members[0]))

    out: list[dict] = []
    for sheet_title, raw in _gus_rows(workbook):
        row: dict = {}
        for source_name, value in raw.items():
            target = GUS_RENAMES.get(str(source_name).strip().upper())
            if target:
                row[target] = value

        category, sheet_type = GUS_SHEET_CATEGORY.get(
            sheet_title.strip().lower(), (None, None)
        )
        government_type, _ = split_coded_name(row.get("government_type"))
        if government_type is None:
            government_type = clean_text(row.get("government_type"))
        if government_type is None or len(government_type) != 1:
            government_type = None

        function_code, function_name = split_coded_name(
            row.get("function_name")
        )
        if function_code is None:
            function_code = clean_text(row.get("function_code"))
        school_level_code, _ = split_coded_name(row.get("school_level_code"))

        state_id = fips_state(row.get("state_id"))
        county_code = clean_text(row.get("county_code"))

        govs = clean_text(row.get("government_id_govs"))
        if govs is None and row.get("state_code_govs") is not None:
            govs = "".join(
                [
                    str(clean_text(row.get("state_code_govs")) or "").zfill(2),
                    str(government_type or "0"),
                    str(clean_text(row.get("county_code_govs")) or "").zfill(
                        3
                    ),
                    str(clean_text(row.get("unit_code_govs")) or "").zfill(3),
                    str(
                        clean_text(row.get("supplement_code_govs")) or "0"
                    ).zfill(3),
                    "00",
                ]
            )

        # Prefer the type published as its own column, fall back to the third
        # character of the legacy identifier, and only then to the worksheet.
        # 2012 publishes no type for general-purpose units and 2021 none for
        # dependent school systems, so the identifier carries the difference.
        if government_type is None and govs and len(govs) >= 3:
            government_type = govs[2]
        if government_type is None:
            government_type = sheet_type

        place, subdivision = split_geography(
            government_type, state_id, row.get("place_code")
        )
        government_id = clean_text(row.get("government_id"))
        out.append(
            {
                "year": year,
                "government_id": government_id.zfill(6)
                if government_id
                else None,
                "government_id_govs": govs,
                "government_type": government_type,
                "unit_category": category,
                "unit_name": clean_text(row.get("unit_name")),
                "function_code": function_code,
                "function_name": function_name,
                "school_level_code": school_level_code,
                "political_description": clean_text(
                    row.get("political_description")
                ),
                "officer_title": clean_text(row.get("officer_title")),
                "address_line_1": clean_text(row.get("address_line_1")),
                "address_line_2": clean_text(row.get("address_line_2")),
                "city": clean_text(row.get("city")),
                "state_abbreviation": clean_text(
                    row.get("state_abbreviation")
                ),
                "zip_code": (
                    str(to_int(row.get("zip_code"))).zfill(5)
                    if to_int(row.get("zip_code")) is not None
                    else None
                ),
                "zip_code_extension": clean_text(
                    row.get("zip_code_extension")
                ),
                "web_address": clean_text(row.get("web_address")),
                "population": to_int(row.get("population")),
                "population_year": to_int(row.get("population_year")),
                "school_enrollment": to_int(row.get("school_enrollment")),
                "enrollment_year": to_int(row.get("enrollment_year")),
                "state_id": state_id,
                "county_id": county_id(state_id, county_code),
                "place_id": place,
                "county_subdivision_id": subdivision,
                "county_area_name": clean_text(row.get("county_area_name")),
                "parent_government_id": clean_text(
                    row.get("parent_government_id")
                ),
                "is_active": clean_text(row.get("is_active")),
            }
        )
    workbook.unlink(missing_ok=True)
    return out


# --------------------------------------------------------------------------
# Employment (APES / COG-E)
# --------------------------------------------------------------------------


def _employment_member(
    input_dir: Path, year: int, kind: str
) -> tuple[Path, str | None]:
    """Locate the individual-unit data or directory file for ``year``.

    Every year also publishes a ``emptot`` / ``empest`` state-and-national
    estimates product under a confusingly similar name. Only the ``empst`` and
    ``empid`` members are individual-unit files, so the others are excluded
    explicitly rather than by ordering luck.
    """
    wanted = re.compile(r"emp" + ("id" if kind == "unit" else "st"), re.I)
    excluded = re.compile(r"emptot|empest", re.I)
    for path in sorted((input_dir / "apes").glob(f"{year}_*")):
        if path.suffix.lower() == ".zip":
            for member in zip_members(path, (".dat", ".txt", ".id", ".zip")):
                if wanted.search(member) and not excluded.search(member):
                    return path, member
        elif wanted.search(path.name) and not excluded.search(path.name):
            return path, None
    raise SourceUnavailableError(f"no employment {kind} file for {year}")


def clean_employment(input_dir: Path, year: int) -> Iterator[dict]:
    """Parse one year of the employment individual-unit file."""
    path, member = _employment_member(input_dir, year, "data")
    layout: dict | None = None
    for line in read_lines(path, member):
        line = line.rstrip()
        if layout is None:
            layout = EMPLOYMENT_DATA_LAYOUTS.get(len(line))
            if layout is None:
                raise ValueError(
                    f"{year}: unknown employment record length {len(line)}"
                )
        unit_id = cut(line, layout["unit_id_govs"])
        state_govs = unit_id[:2]
        government_id = (
            cut(line, layout["government_id"])
            if "government_id" in layout
            else None
        )
        yield {
            "year": year,
            "government_id": government_id or None,
            "government_id_govs": unit_id or None,
            "state_id": GOVS_TO_FIPS_STATE.get(state_govs),
            "government_type": unit_id[2:3] or None,
            "function_code": cut(line, layout["function_code"]) or None,
            "full_time_employees": to_int(
                cut(line, layout["full_time_employees"])
            ),
            "full_time_employees_flag": (
                cut(line, layout["full_time_employees_flag"]) or None
                if "full_time_employees_flag" in layout
                else None
            ),
            "full_time_payroll": to_int(
                cut(line, layout["full_time_payroll"])
            ),
            "full_time_payroll_flag": (
                cut(line, layout["full_time_payroll_flag"]) or None
                if "full_time_payroll_flag" in layout
                else None
            ),
            "part_time_employees": to_int(
                cut(line, layout["part_time_employees"])
            ),
            "part_time_employees_flag": (
                cut(line, layout["part_time_employees_flag"]) or None
                if "part_time_employees_flag" in layout
                else None
            ),
            "part_time_payroll": to_int(
                cut(line, layout["part_time_payroll"])
            ),
            "part_time_payroll_flag": (
                cut(line, layout["part_time_payroll_flag"]) or None
                if "part_time_payroll_flag" in layout
                else None
            ),
            "part_time_hours": (
                to_int(cut(line, layout["part_time_hours"]))
                if "part_time_hours" in layout
                else None
            ),
            "full_time_equivalent_employees": (
                to_int(cut(line, layout["full_time_equivalent_employees"]))
                if "full_time_equivalent_employees" in layout
                else None
            ),
        }


def clean_employment_unit(input_dir: Path, year: int) -> Iterator[dict]:
    """Parse one year of the employment individual-unit directory file."""
    path, member = _employment_member(input_dir, year, "unit")
    layout: dict | None = None
    for line in read_lines(path, member):
        line = line.rstrip("\n")
        if layout is None:
            layout = (
                EMPLOYMENT_UNIT_LAYOUT_PID6
                if len(line.rstrip()) > 206
                else EMPLOYMENT_UNIT_LAYOUT
            )
        unit_id = cut(line, layout["unit_id_govs"])
        state_id = fips_state(
            cut(line, layout["state_id"])
            or GOVS_TO_FIPS_STATE.get(unit_id[:2])
        )
        government_id = (
            cut(line, layout["government_id"])
            if "government_id" in layout
            else None
        )
        if government_id in {"", "000000"}:
            government_id = None
        yield {
            "year": year,
            "government_id": government_id,
            "government_id_govs": unit_id or None,
            "government_type": unit_id[2:3] or None,
            "unit_name": clean_text(cut(line, layout["unit_name"])),
            "state_id": state_id,
            "county_id": county_id(state_id, cut(line, layout["county_code"])),
            "county_name": clean_text(cut(line, layout["county_name"])),
            "census_region_code": (
                cut(line, layout["census_region_code"]).lstrip("0") or None
            ),
            "population_enrollment_function": (
                cut(line, layout["population_enrollment_function"]) or None
            ),
            "population_enrollment_year": _two_digit_year(
                cut(line, layout["population_enrollment_year"]), year
            ),
            "school_level_code": _school_level(
                cut(line, layout["school_level_code"])
            ),
            "selection_probability": to_float(
                cut(line, layout["selection_probability"])
            ),
            "worksheet_code": pad_code(cut(line, layout["worksheet_code"]), 2),
        }


def _school_level(value: str | None) -> str | None:
    """Normalise the school level code, treating an all-zero value as missing.

    ``0`` and ``00`` appear where the unit is not a school system, which is not
    one of the seven documented levels.
    """
    text = pad_code(value, 2)
    if text is None or set(text) == {"0"}:
        return None
    return text


def _two_digit_year(value: str, survey_year: int) -> int | None:
    """Expand a two-digit year, anchored so it never lands after the survey."""
    number = to_int(value)
    if number is None:
        return None
    if number > 99:
        return number
    candidate = (survey_year // 100) * 100 + number
    if candidate > survey_year:
        candidate -= 100
    return candidate


# --------------------------------------------------------------------------
# Finance
# --------------------------------------------------------------------------

# The historical archive holds the reference fields and the finance variables in
# one wide record, split across three files. These are the reference columns of
# file a, in file order, up to the first finance variable.
FINANCE_WIDE_REFERENCE = {
    "SortCode": "sort_code",
    "SurveyYr": "survey_year",
    "Year4": "year",
    "ID": "government_id_govs",
    "State Code": "state_code_govs",
    "Type Code": "government_type",
    "County": "county_code_govs",
    "Name": "unit_name",
    "Census Region": "census_region_code",
    "FIPS Code-State": "state_id",
    "Weight": "survey_weight",
    "FYEndDate": "fiscal_year_end",
    "YearPop": "population_year",
    "SchLevCode": "school_level_code",
    "Data_Flag": "data_flag",
    "Imputed Record": "is_imputed_record",
    "Population": "population",
}
# Amounts are published in thousands of dollars in both eras.
FINANCE_AMOUNT_SCALE = 1000
# Index of the first finance variable in the wide record; everything before it
# is a reference field.
FINANCE_WIDE_FIRST_ITEM = 25


def load_finance_catalog() -> list[dict]:
    """Read the committed finance variable catalogue in wide-file column order."""
    path = Path(constants.ARCHITECTURE_DIR.value) / "finance_item_catalog.csv"
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def _wide_parts(
    archive: Path, year: int
) -> dict[str, tuple[list[str], list[str]]]:
    """Return ``{part: (header, lines)}`` for one fiscal year of the wide file."""
    stem = f"IndFin{year % 100:02d}"
    out: dict[str, tuple[list[str], list[str]]] = {}
    with zipfile.ZipFile(archive) as zf:
        names = {n.lower(): n for n in zf.namelist()}
        for part in "abc":
            key = f"{stem}{part}.txt".lower()
            if key not in names:
                raise SourceUnavailableError(
                    f"{key} missing from {archive.name}"
                )
            text = zf.read(names[key]).decode("latin-1")
            lines = text.splitlines()
            out[part] = (next(csv.reader([lines[0]])), lines[1:])
    return out


REPAIRED_WIDE_ROWS: dict[int, int] = {}


def _parse_wide_row(line: str, width: int, year: int) -> list[str]:
    """Parse one wide-file record, repairing the source's unescaped quotes.

    At least one record carries a stray double quote inside a quoted name --
    ``"MORGAN COUNTY SOLID WASTE DISTRICT""`` in fiscal 2012 -- which makes the
    field absorb its neighbour and shifts every later value by one position. A
    shifted record parses without error and lands wrong data in every column, so
    the field count is checked on every record. When it is short, the record is
    re-parsed with quoting disabled and accepted only if that yields exactly the
    expected width.
    """
    row = next(csv.reader([line]))
    if len(row) == width:
        return row
    repaired = [
        field[1:-1]
        if len(field) > 1 and field.startswith('"')
        else field.strip('"')
        for field in next(csv.reader([line], quoting=csv.QUOTE_NONE))
    ]
    if len(repaired) != width:
        raise ValueError(
            f"{year}: record has {len(row)} fields, expected {width}, and could "
            f"not be repaired: {line[:120]!r}"
        )
    REPAIRED_WIDE_ROWS[year] = REPAIRED_WIDE_ROWS.get(year, 0) + 1
    return repaired


def _iter_wide(
    archive: Path, year: int
) -> Iterator[tuple[dict, list[str], list[str]]]:
    """Yield ``(reference_fields, finance_values, finance_labels)`` per record.

    The three files are row-aligned by construction and each carries the sort
    code and identifier, which are checked on every record rather than trusted.
    """
    parts = _wide_parts(archive, year)
    header_a, lines_a = parts["a"]
    header_b, lines_b = parts["b"]
    header_c, lines_c = parts["c"]
    if not len(lines_a) == len(lines_b) == len(lines_c):
        raise ValueError(
            f"{year}: wide files disagree on length: "
            f"{len(lines_a)}, {len(lines_b)}, {len(lines_c)}"
        )
    # The wide record opens with 25 reference columns before the first finance
    # variable. Asserted rather than counted, because a shifted boundary would
    # silently misalign every item code in the melt.
    reference_count = FINANCE_WIDE_FIRST_ITEM
    if header_a[reference_count] != "Total Revenue":
        raise ValueError(
            f"{year}: finance variables do not start at column "
            f"{reference_count}, found {header_a[reference_count]!r}"
        )
    labels = header_a[reference_count:] + header_b[3:] + header_c[3:]
    index_a = {name: i for i, name in enumerate(header_a)}
    for raw_a, raw_b, raw_c in zip(lines_a, lines_b, lines_c, strict=True):
        row_a = _parse_wide_row(raw_a, len(header_a), year)
        row_b = _parse_wide_row(raw_b, len(header_b), year)
        row_c = _parse_wide_row(raw_c, len(header_c), year)
        if row_a[0] != row_b[0] or row_a[0] != row_c[0]:
            raise ValueError(f"{year}: wide files are not row-aligned")
        reference = {
            target: row_a[index_a[source]]
            for source, target in FINANCE_WIDE_REFERENCE.items()
            if source in index_a
        }
        yield (
            reference,
            row_a[reference_count:] + row_b[3:] + row_c[3:],
            labels,
        )


def _govs_id_14(value: object) -> str | None:
    """Pad the historical 9-character GOVS identifier to its 14-character form.

    The archive publishes state, type, county and unit only. From fiscal 2013 the
    same identifier appears with the supplement and sub-code positions spelled
    out, which the documentation says are ``00000`` for a unit that is not part
    of another government. Padding makes one identifier serve the whole finance
    table and match the employment files.
    """
    text = clean_text(value)
    if text is None:
        return None
    return text.ljust(14, "0") if len(text) == 9 else text


def _wide_value(value: object) -> str | None:
    """Clean a wide-file reference field, mapping its blank filler to missing.

    The historical archive fills unused character fields with repeated ``B``
    rather than spaces: ``BB`` for a two-character field, ``BBBB`` for four.
    Left alone these become literal values -- 76,705 governments would show a
    school level of ``BB`` in fiscal 2012 alone.
    """
    text = clean_text(value)
    if text is None or set(text) == {"B"}:
        return None
    return text


def _wide_reference_row(reference: dict, year: int) -> dict:
    """Turn the wide file's reference fields into a finance_unit row."""
    state_id = fips_state(reference.get("state_id"))
    return {
        "year": year,
        "government_id": None,
        "government_id_govs": _govs_id_14(reference.get("government_id_govs")),
        "government_type": _wide_value(reference.get("government_type")),
        "unit_name": clean_text(reference.get("unit_name")),
        "state_id": state_id,
        "county_id": None,
        "county_name": None,
        "place_id": None,
        "county_subdivision_id": None,
        "census_region_code": _wide_value(reference.get("census_region_code")),
        "population": to_int(reference.get("population")),
        "population_year": _two_digit_year(
            str(reference.get("population_year") or ""), year
        ),
        "school_enrollment": None,
        "school_level_code": _school_level(
            _wide_value(reference.get("school_level_code"))
        ),
        "special_district_function_code": None,
        "fiscal_year_end": _fiscal_year_end(reference.get("fiscal_year_end")),
        "survey_weight": to_float(reference.get("survey_weight")),
        "data_flag": _wide_value(reference.get("data_flag")),
        "is_imputed_record": _wide_value(reference.get("is_imputed_record")),
    }


def clean_finance(input_dir: Path, year: int) -> Iterator[dict]:
    """Parse one fiscal year of the finance individual-unit data.

    Fiscal years through 2012 come from the wide historical archive, one record
    per government with 529 finance columns, and are melted to one row per
    non-zero item. Fiscal years from 2013 come already in that long shape.
    """
    if year <= 2012:
        catalog = load_finance_catalog()
        codes = [entry["item_code"] for entry in catalog]
        expected = [entry["column_label"] for entry in catalog]
        archive = input_dir / "fin" / "IndFin_1967_2012.zip"
        checked = False
        for reference, values, labels in _iter_wide(archive, year):
            if not checked:
                if [label.strip() for label in labels] != expected:
                    raise ValueError(
                        f"{year}: wide header does not match the catalogue"
                    )
                checked = True
            unit = _govs_id_14(reference.get("government_id_govs"))
            for code, value in zip(codes, values, strict=True):
                amount = to_int(value)
                if not amount:
                    continue
                yield {
                    "year": year,
                    "government_id": None,
                    "government_id_govs": unit,
                    "item_code": code,
                    "amount": amount * FINANCE_AMOUNT_SCALE,
                    "data_flag": None,
                }
        return

    archive = _finance_archive(input_dir, year)
    member = _finance_member(archive, r"FinEstDAT", year)
    layout: dict | None = None
    for line in read_lines(archive, member):
        line = line.rstrip()
        if layout is None:
            layout = FINANCE_DATA_LAYOUTS.get(len(line))
            if layout is None:
                raise ValueError(
                    f"{year}: unknown finance record length {len(line)}"
                )
        amount = to_int(cut(line, layout["amount"]))
        if amount is None:
            continue
        yield {
            "year": year,
            "government_id": (
                cut(line, layout["government_id"]) or None
                if "government_id" in layout
                else None
            ),
            "government_id_govs": (
                cut(line, layout["government_id_govs"]) or None
                if "government_id_govs" in layout
                else None
            ),
            "item_code": cut(line, layout["item_code"]) or None,
            "amount": amount * FINANCE_AMOUNT_SCALE,
            "data_flag": cut(line, layout["data_flag"]) or None,
        }


def clean_finance_unit(input_dir: Path, year: int) -> Iterator[dict]:
    """Parse one fiscal year of the finance unit directory."""
    if year <= 2012:
        archive = input_dir / "fin" / "IndFin_1967_2012.zip"
        for reference, _values, _labels in _iter_wide(archive, year):
            yield _wide_reference_row(reference, year)
        return

    archive = _finance_archive(input_dir, year)
    member = _finance_member(archive, r"Fin_[GP]ID", year)
    layout: dict | None = None
    for line in read_lines(archive, member):
        line = line.rstrip("\n").rstrip("\r")
        if layout is None:
            layout = FINANCE_UNIT_LAYOUTS.get(len(line.rstrip()))
            if layout is None:
                # The GID files carry one trailing pad character.
                layout = FINANCE_UNIT_LAYOUTS.get(len(line))
            if layout is None:
                raise ValueError(
                    f"{year}: unknown finance directory record length {len(line)}"
                )
        state_id = fips_state(cut(line, layout["state_id"]))
        government_type = cut(line, layout["government_type"]) or None
        place, subdivision = split_geography(
            government_type, state_id, cut(line, layout["place_code"])
        )
        yield {
            "year": year,
            "government_id": (
                cut(line, layout["government_id"]) or None
                if "government_id" in layout
                else None
            ),
            "government_id_govs": (
                cut(line, layout["government_id_govs"]) or None
                if "government_id_govs" in layout
                else None
            ),
            "government_type": government_type,
            "unit_name": clean_text(cut(line, layout["unit_name"])),
            "state_id": state_id,
            "county_id": county_id(state_id, cut(line, layout["county_code"])),
            "county_name": clean_text(cut(line, layout["county_name"])),
            "place_id": place,
            "county_subdivision_id": subdivision,
            "census_region_code": None,
            "population": to_int(cut(line, layout["population"])),
            "population_year": _two_digit_year(
                cut(line, layout["population_year"]), year
            ),
            "school_enrollment": to_int(
                cut(line, layout["school_enrollment"])
            ),
            "school_level_code": cut(line, layout["school_level_code"])
            or None,
            "special_district_function_code": (
                cut(line, layout["special_district_function_code"]) or None
            ),
            "fiscal_year_end": cut(line, layout["fiscal_year_end"]) or None,
            "survey_weight": None,
            "data_flag": None,
            "is_imputed_record": None,
        }


def _fiscal_year_end(value: object) -> str | None:
    """Normalise the fiscal year ending to MMDD.

    Early years drop the leading zero on months before October, so June shows as
    ``630``. Values that are not four digits after padding are published as they
    stand: fiscal 1967 carries impossible dates such as ``8236`` that no amount
    of reformatting makes real.
    """
    text = _wide_value(value)
    if text is None:
        return None
    return text.zfill(4) if len(text) == 3 and text.isdigit() else text


def _finance_archive(input_dir: Path, year: int) -> Path:
    """Locate the downloaded finance archive for a 2013-2018 fiscal year."""
    matches = sorted((input_dir / "fin").glob(f"{year}_*.zip"))
    if not matches:
        raise SourceUnavailableError(f"no finance archive for {year}")
    return matches[0]


def _finance_member(archive: Path, pattern: str, year: int) -> str:
    """Pick the member of a finance archive matching ``pattern``."""
    wanted = re.compile(pattern, re.I)
    for member in zip_members(archive, (".txt", ".dat")):
        if wanted.search(member) and "statetype" not in member.lower():
            return member
    raise SourceUnavailableError(
        f"{year}: no member matching {pattern} in {archive.name}"
    )


# --------------------------------------------------------------------------
# Dictionary
# --------------------------------------------------------------------------

GOVERNMENT_TYPE_LABELS = {
    "0": "State",
    "1": "County",
    "2": "Municipality",
    "3": "Township",
    "4": "Special district",
    "5": "Independent school district or education service agency",
    "6": "Federal government",
}
UNIT_CATEGORY_LABELS = {
    "general_purpose": "General purpose government",
    "special_district": "Special district government",
    "school_district": "Independent school district or education service agency",
    "dependent_school_system": (
        "School system dependent on a parent government, not counted as a "
        "government"
    ),
    "public_pension_system": (
        "Public employee retirement system administered by a parent government"
    ),
}
CENSUS_REGION_LABELS = {
    "1": "Northeast",
    "2": "Midwest",
    "3": "South",
    "4": "West",
}
SCHOOL_LEVEL_LABELS = {
    "01": "Elementary only",
    "02": "Secondary only",
    "03": "Elementary and secondary",
    "04": "Post-secondary",
    "05": "Special or vocational education",
    "06": "Non-operating",
    "07": "Educational service agency",
}
WORKSHEET_LABELS = {
    "00": "Central collection",
    "01": "E-1 State agencies",
    "02": "E-2 State institutions of higher education",
    "03": "E-3 Special districts and local agencies",
    "04": "E-4 Municipalities, counties, townships",
    "05": "E-5 Municipalities and townships",
    "06": "E-6 School systems",
    "07": "E-7 Major special districts and agencies",
    "08": "E-8 Elementary and secondary education",
    "09": "E-9 Police protection agencies",
    "10": "E-10 College and other postsecondary education",
}
# Codes the Census publishes a label for only in the older documentation, which
# the current code list drops.
EMPLOYMENT_FUNCTION_LABELS = {
    "000": "Total, all government employment functions",
    "002": "Space research and technology, federal",
    "006": "National defense and international relations, federal",
    "014": "Postal service, federal",
    "001": "Air transportation",
    "005": "Corrections",
    "012": "Education, elementary and secondary instructional",
    "016": "Education, higher education other",
    "018": "Education, higher education instructional",
    "021": "Education, other",
    "022": "Social insurance administration",
    "023": "Financial administration",
    "024": "Fire protection, firefighters",
    "025": "Judicial and legal",
    "029": "Other government administration",
    "032": "Health",
    "040": "Hospitals",
    "044": "Highways",
    "050": "Housing and community development",
    "052": "Libraries",
    "059": "Natural resources",
    "061": "Parks and recreation",
    "062": "Police protection, persons with power of arrest",
    "079": "Public welfare",
    "080": "Sewerage",
    "081": "Solid waste management",
    "087": "Sea and inland port facilities",
    "089": "All other and unallocable",
    "090": "State liquor stores",
    "091": "Water supply",
    "092": "Electric power",
    "093": "Gas supply",
    "094": "Transit",
    "112": "Education, elementary and secondary other",
    "124": "Fire protection, other",
    "162": "Police protection, other",
}
EMPLOYMENT_FLAG_LABELS = {
    "C": (
        "Reported: analyst corrected data provided in an inappropriate item "
        "code or unit without contacting the respondent"
    ),
    "K": "Reported: analyst corrected improperly keyed data",
    "R": "Reported: data reported directly by the respondent",
    "T": (
        "Reported: respondent reported totals, pro-rated on the prior year "
        "distribution"
    ),
    "U": "Reported: analyst obtained correct data from the respondent",
    "V": "Reported: analyst verified the data with the respondent",
    "Z": (
        "Reported: summation of multiple state agencies or of multiple function "
        "codes"
    ),
    "A": "Imputed: computed from prior year factors or prior year state averages",
    "B": "Imputed: obtained from a report or administrative source",
    "D": "Imputed: obtained from a website",
    "G": "Imputed: prior year adjusted by a growth rate from similar units",
    "J": "Imputed: pro-rated from partially reported unit totals",
    "P": "Imputed: donor value from a similar unit, adjusted per capita",
    "Q": "Imputed: growth rate applied to a prior year value flagged P",
    "X": "Imputed: analyst created the value without contacting the respondent",
}
# Values that appear in the data but in no code list the Census still
# publishes. Recording what is known about them beats either inventing a label
# or leaving the column undocumented; see models/us_census_cog/CLAUDE.md.
UNDOCUMENTED = (
    "Code present in the source data but absent from every code list the "
    "Census Bureau publishes"
)
UNDOCUMENTED_LABELS = {
    # These seven codes carry no label in any published list, but the data
    # settles what they are: across 1992-1998 the value of code 112 equals the
    # sum of the seven for 47,541 of 47,545 units, so they are the components of
    # "Education - Elementary and Secondary Other". Which component each one is
    # remains undocumented.
    ("employment", "function_code"): {
        code: (
            "Component of code 112, Education - Elementary and Secondary Other, "
            f"reported from 1992 to 2000. {UNDOCUMENTED}"
        )
        for code in ("212", "312", "412", "512", "612", "712", "812")
    },
    ("employment", "flag"): {"I": UNDOCUMENTED, "S": UNDOCUMENTED},
    ("employment_unit", "worksheet_code"): dict.fromkeys(
        ("11", "93", "94", "95", "96", "97", "CC"), UNDOCUMENTED
    ),
    ("finance", "data_flag"): dict.fromkeys(("M", "N", "S"), UNDOCUMENTED),
}

IS_ACTIVE_LABELS = {
    "Y": "Active at the survey date",
    "N": "Not active at the survey date",
}
IMPUTED_RECORD_LABELS = {
    "0": "Record not imputed",
    "1": "Record imputed",
}
# Government finance data flags, from the historical archive documentation.
FINANCE_FLAG_LABELS = {
    "R": "Reported by the government",
    "A": "Imputed or adjusted by the Census Bureau",
    "I": "Imputed by the Census Bureau",
    "K": "Keyed correction applied",
    "P": "Imputed from a donor unit",
    "X": "Value created by the analyst",
    "Z": "Summation of multiple records",
}


def build_dicionario(
    special_district_functions: Iterable[tuple[str, str]] = (),
) -> list[dict]:
    """Assemble the dictionary table from every coded column in the dataset.

    Args:
        special_district_functions: ``(code, label)`` pairs observed in the
            government unit and finance files, which the Census does not publish
            as a standalone code list.
    """
    rows: list[dict] = []

    def add(
        table: str, column: str, mapping: Iterable[tuple[str, str]]
    ) -> None:
        for key, value in mapping:
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": "",
                    "valor": value,
                }
            )

    government_type = sorted(GOVERNMENT_TYPE_LABELS.items())
    for table in (
        "government_unit",
        "employment",
        "employment_unit",
        "finance_unit",
    ):
        add(table, "government_type", government_type)
    add(
        "government_unit",
        "unit_category",
        sorted(UNIT_CATEGORY_LABELS.items()),
    )
    add("government_unit", "is_active", sorted(IS_ACTIVE_LABELS.items()))
    for table in ("government_unit", "employment_unit", "finance_unit"):
        add(table, "school_level_code", sorted(SCHOOL_LEVEL_LABELS.items()))
    for table in ("employment_unit", "finance_unit"):
        add(table, "census_region_code", sorted(CENSUS_REGION_LABELS.items()))
    add(
        "employment_unit",
        "worksheet_code",
        sorted(
            WORKSHEET_LABELS.items()
            | UNDOCUMENTED_LABELS[
                ("employment_unit", "worksheet_code")
            ].items()
        ),
    )
    add(
        "employment",
        "function_code",
        sorted(
            EMPLOYMENT_FUNCTION_LABELS.items()
            | UNDOCUMENTED_LABELS[("employment", "function_code")].items()
        ),
    )
    flags = sorted(
        EMPLOYMENT_FLAG_LABELS.items()
        | UNDOCUMENTED_LABELS[("employment", "flag")].items()
    )
    for column in (
        "full_time_employees_flag",
        "full_time_payroll_flag",
        "part_time_employees_flag",
        "part_time_payroll_flag",
    ):
        add("employment", column, flags)
    add(
        "finance",
        "data_flag",
        sorted(
            FINANCE_FLAG_LABELS.items()
            | UNDOCUMENTED_LABELS[("finance", "data_flag")].items()
        ),
    )
    add(
        "finance_unit",
        "is_imputed_record",
        sorted(IMPUTED_RECORD_LABELS.items()),
    )
    functions = sorted(set(special_district_functions))
    add("government_unit", "function_code", functions)
    add("finance_unit", "special_district_function_code", functions)

    for entry in load_finance_catalog():
        rows.append(
            {
                "id_tabela": "finance",
                "nome_coluna": "item_code",
                "chave": entry["item_code"],
                "cobertura_temporal": "",
                "valor": entry["description"],
            }
        )
    return rows


# --------------------------------------------------------------------------
# Parquet output
# --------------------------------------------------------------------------

ARROW_TYPES = {"INT64": "int64", "FLOAT64": "float64", "STRING": "string"}


def assert_all_string(path: Path) -> None:
    """Fail if a parquet file carries any non-string column."""
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(path).schema_arrow
    typed = [
        f"{name}:{dtype}"
        for name, dtype in zip(schema.names, schema.types, strict=True)
        if str(dtype) != "string"
    ]
    if typed:
        raise ValueError(
            f"{path}: staging parquet must be all-string, got {typed}"
        )


def write_partitioned(
    rows: Iterable[dict],
    output_dir: Path,
    table: str,
    year: int | None = None,
    chunk_size: int = 500_000,
) -> int:
    """Write rows to a hive-partitioned, all-string parquet file.

    Staging is all-string by house convention, and ``upload_to_gcs`` infers the
    staging schema from a stringified one-row header, so typed parquet is
    rejected on read. Values are built with their real types first and cast
    through arrow rather than with ``astype(str)``: the latter renders NULL as
    the literal ``"nan"``, which ``safe_cast`` will not turn back into NULL, and
    a float year would serialise as ``"1959.0"``.

    Rows are consumed in chunks and appended to one file rather than
    materialised whole. A single fiscal year of finance melts to about four
    million rows, and holding those as dicts costs several gigabytes.

    A partition with no rows writes no file. An empty first partition would make
    the staging header infer the wrong types for the whole table.

    Returns:
        The number of rows written.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    columns = load_cols(table)
    schema = pa.schema([(c.name, pa.string()) for c in columns])

    destination = output_dir / table
    if year is not None:
        destination = destination / f"year={year}"
    path = destination / "data.parquet"

    def batch_of(buffer: list[dict]):
        arrays = [
            pa.array(
                [row.get(c.name) for row in buffer],
                type=ARROW_TYPES[c.bigquery_type],
            ).cast(pa.string())
            for c in columns
        ]
        return pa.Table.from_arrays(arrays, schema=schema)

    written = 0
    writer = None
    buffer: list[dict] = []
    try:
        for row in rows:
            buffer.append(row)
            if len(buffer) >= chunk_size:
                if writer is None:
                    destination.mkdir(parents=True, exist_ok=True)
                    writer = pq.ParquetWriter(
                        path, schema, compression="snappy"
                    )
                writer.write_table(batch_of(buffer))
                written += len(buffer)
                buffer = []
        if buffer:
            if writer is None:
                destination.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(path, schema, compression="snappy")
            writer.write_table(batch_of(buffer))
            written += len(buffer)
    finally:
        if writer is not None:
            writer.close()

    if written:
        assert_all_string(path)
    return written


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def clean_table_year(
    table: str, input_dir: Path, output_dir: Path, year: int
) -> int:
    """Clean one year of one data table into its parquet partition."""
    cleaners = {
        "government_unit": clean_government_unit,
        "employment": clean_employment,
        "employment_unit": clean_employment_unit,
        "finance": clean_finance,
        "finance_unit": clean_finance_unit,
    }
    rows = cleaners[table](input_dir, year)
    return write_partitioned(rows, output_dir, table, year)


def table_years(table: str) -> list[int]:
    """Years published for a data table."""
    if table == "government_unit":
        return list(GUS_YEARS)
    if table in {"employment", "employment_unit"}:
        return list(EMPLOYMENT_YEARS)
    return list(FINANCE_YEARS)


def collect_function_labels(output_dir: Path) -> list[tuple[str, str]]:
    """Read back the special-district function codes and names already written.

    The Census publishes no standalone code list for these, so the dictionary is
    built from the code and name the government units file carries side by side.
    """
    import pyarrow.parquet as pq

    labels: dict[str, str] = {}
    for path in sorted(
        (output_dir / "government_unit").glob("year=*/data.parquet")
    ):
        # Read the file directly rather than through the dataset API, which
        # would treat the year=<Y> parent as a hive partition and clash with the
        # year column the file itself carries.
        columns = pq.ParquetFile(path).read(
            columns=["function_code", "function_name"]
        )
        for code, name in zip(
            columns.column("function_code").to_pylist(),
            columns.column("function_name").to_pylist(),
            strict=True,
        ):
            if code and name:
                labels.setdefault(code, name.title())
    return sorted(labels.items())
