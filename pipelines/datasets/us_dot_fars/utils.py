"""Pure helpers for the us_dot_fars pipeline: download, codebook, transform.

No Prefect imports live here. The one-shot onboarding bootstrap under
``models/us_dot_fars/code/`` imports these same functions, so the backfill and
the recurring refresh can never drift apart.

Three facts about FARS drive the whole design:

1. NHTSA republishes every year from 1975 as one CSV zip, so a single download
   shape covers the 50-year span. Files sit at the archive root and in UPPERCASE
   before 2015, and under a ``FARS<year>NationalCSV/`` directory in lowercase
   after; 2021 and 2022 additionally carry a UTF-8 BOM on the header line.
2. Variables were renamed repeatedly (``TEST_RES`` -> ``ALC_RES``,
   ``C_M_ZONE`` -> ``WRK_ZONE``). ``original_name`` in the architecture CSV holds
   a ``|``-separated alias list and the first alias present in that year's header
   wins.
3. Almost every coded variable's *code set* was also redefined at least once, so
   codes are carried as STRING and their meaning lives in the ``dicionario``
   table keyed by year range. Casting them to INT64 would be a correctness bug,
   not a style choice: FARS sentinels are dense integers (99 = unknown,
   996 = test not given) that sit in the same range as real values.
"""

from __future__ import annotations

import csv
import io
import os
import re
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_dot_fars.constants import constants

BOM = b"\xef\xbb\xbf"

# Marker for a code that appears in the published data but that no year's format
# file defines. Written in English to sit alongside NHTSA's own labels.
UNDOCUMENTED = "Not documented by the source"

# Labels that mark a code as "no real value here" rather than a category.
SENTINEL_LABEL = re.compile(
    r"unknown|not\s*report|no\s*report|not\s*applicable|none\s*given|"
    r"refus|not\s*given|no\s*driver",
    re.I,
)


# --------------------------------------------------------------------------
# Architecture
# --------------------------------------------------------------------------


@dataclass
class Col:
    """One column of an architecture table.

    The transform needs ``name`` / ``bq_type`` / ``aliases``; the rest is carried
    so the metadata and dbt generators parse the same CSV through this one
    parser rather than writing a second.
    """

    name: str
    bq_type: str
    aliases: tuple[str, ...]
    covered_by_dictionary: bool = False
    directory_column: str = ""
    measurement_unit: str = ""
    temporal_coverage: str = ""
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
                    aliases=tuple(
                        a.strip().upper()
                        for a in r["original_name"].split("|")
                        if a.strip()
                    ),
                    covered_by_dictionary=r["covered_by_dictionary"].strip()
                    == "yes",
                    directory_column=r["directory_column"].strip(),
                    measurement_unit=r["measurement_unit"].strip(),
                    temporal_coverage=r["temporal_coverage"].strip(),
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
# Download
# --------------------------------------------------------------------------


def _get(
    url: str, dest: Path, session: requests.Session | None = None
) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    s = session or requests.Session()
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with s.get(
        url, headers=constants.HEADERS.value, stream=True, timeout=600
    ) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            # decode_content keeps a Content-Encoding-compressed body from being
            # written to disk still encoded.
            r.raw.decode_content = True
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    tmp.rename(dest)
    return dest


def year_is_published(
    year: int, session: requests.Session | None = None
) -> bool:
    """Whether NHTSA has published the annual CSV zip for ``year``."""
    s = session or requests.Session()
    url = constants.DOWNLOAD_URL.value.format(year=year)
    try:
        r = s.head(
            url,
            headers=constants.HEADERS.value,
            timeout=60,
            allow_redirects=True,
        )
    except requests.RequestException:
        return False
    return r.status_code == 200


def latest_published_year(
    start: int, session: requests.Session | None = None
) -> int | None:
    """Walk forward from ``start`` and return the newest published year."""
    s = session or requests.Session()
    latest = None
    year = start
    while year_is_published(year, s):
        latest = year
        year += 1
    return latest


def download_year(year: int, input_dir: Path, session=None) -> Path:
    return _get(
        constants.DOWNLOAD_URL.value.format(year=year),
        Path(input_dir) / f"FARS{year}.zip",
        session,
    )


def download_formats(year: int, input_dir: Path, session=None) -> Path:
    """Fetch the SAS release, which carries NHTSA's own PROC FORMAT source."""
    return _get(
        constants.FORMAT_URL.value.format(year=year),
        Path(input_dir) / f"FARS{year}SAS.zip",
        session,
    )


# --------------------------------------------------------------------------
# Reading the annual CSVs
# --------------------------------------------------------------------------


def _member(zf: zipfile.ZipFile, stem: str) -> str:
    """Locate a source file inside the annual zip, whatever its case or depth."""
    hits = [
        n for n in zf.namelist() if re.search(rf"(^|/){stem}\.csv$", n, re.I)
    ]
    if not hits:
        raise FileNotFoundError(f"{stem}.csv not found in {zf.filename}")
    return hits[0]


def read_rows(zip_path: Path, stem: str):
    """Yield ``(header, row)`` for one source file, BOM and encoding handled.

    2021 and 2022 prefix the header line with a UTF-8 BOM. Read as latin-1 (the
    older files carry latin-1 bytes in free-text fields), so the BOM has to be
    stripped from the byte stream rather than by decoding as utf-8-sig; left in,
    it renames the first column and ``STATE`` silently goes missing.
    """
    with zipfile.ZipFile(zip_path) as zf:
        member = _member(zf, stem)
        raw = zf.open(member)
        if raw.read(3) != BOM:
            raw = zf.open(member)
        reader = csv.reader(
            io.TextIOWrapper(raw, encoding="latin-1", newline="")
        )
        header = [c.strip().strip('"').upper() for c in next(reader)]
        index = {name: i for i, name in enumerate(header)}
        for row in reader:
            yield index, row


def resolve(cols: list[Col], header_index: dict[str, int]) -> dict[str, int]:
    """Map each canonical column to a source position for this year's header.

    The first alias present wins, so a renamed variable resolves to whichever
    name that year actually used. A column absent this year is simply omitted.
    """
    out = {}
    for c in cols:
        for alias in c.aliases:
            if alias in header_index:
                out[c.name] = header_index[alias]
                break
    return out


# --------------------------------------------------------------------------
# Value cleaning
# --------------------------------------------------------------------------


def _int(value: str) -> int | None:
    v = (value or "").strip()
    if not v:
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def num(value: str, sentinels: set[int]) -> str | None:
    """Return a numeric value as text, or None when it is a sentinel code."""
    n = _int(value)
    if n is None or n in sentinels:
        return None
    return str(n)


def code(value: str) -> str | None:
    """Normalise a coded value: strip, drop leading zeros, keep it a string."""
    v = (value or "").strip().strip('"')
    if not v:
        return None
    if v.lstrip("-").isdigit():
        return str(int(v))
    return v


# Bounds of the 50 states plus DC, generously padded. Anything outside is not a
# place in this dataset: 2001 longitude, for one, contains -1000.677775.
LAT_BOUNDS = (17.0, 72.0)
LON_BOUNDS = (-180.0, -64.0)

# "Not reported" fillers. Once the integer era is scaled down (below) both eras
# land on the same six values.
COORD_FILLERS = (77.7777, 88.8888, 99.9999, 777.7777, 888.8888, 999.9999)


def coord(value: str, is_longitude: bool = False) -> str | None:
    """Parse a latitude or longitude across the two FARS encodings.

    1999 and 2000 store the coordinate as an unsigned integer with six implied
    decimals - 32490935 is 32.490935 degrees, and a longitude of 72591700 means
    72.591700 *west*. From 2001 the same columns hold a signed decimal degree.
    Read literally, the early era puts every crash 32 million degrees north and
    the sign loss puts the rest of it in China.

    Scaling the integer era down by a million also makes its fillers line up with
    the decimal era's: 88888888 becomes 88.888888, the same "not reported" code.
    A final bounds check catches what neither rule does, including the
    -1000.677775 that sits in the 2001 file.
    """
    v = (value or "").strip()
    if not v:
        return None
    try:
        f = float(v)
    except ValueError:
        return None
    if f == 0:
        return None
    # No real coordinate exceeds 180 degrees, so a large magnitude means the
    # integer encoding rather than a real place.
    if abs(f) > 1000:
        f = f / 1e6
    for filler in COORD_FILLERS:
        if abs(abs(f) - filler) < 1e-4:
            return None
    # The integer era carries no sign; every US longitude is west of Greenwich.
    if is_longitude and f > 0:
        f = -f
    lo, hi = LON_BOUNDS if is_longitude else LAT_BOUNDS
    if not (lo <= f <= hi):
        return None
    return repr(f)


def model_year(value: str, year: int) -> str | None:
    """Normalise MOD_YEAR across the two-digit and four-digit eras.

    Early FARS stores the model year as two digits, so ``73`` means 1973 and 99
    is Unknown; later years store four digits with 9998 / 9999 as the
    not-reported and unknown codes. Reading the early era literally would record
    a fleet of model-year-73 vehicles.

    The two forms are told apart by magnitude, not by file year: 1998 and 1999
    already publish four-digit model years, so keying on the year would send
    9999 through the two-digit branch and produce model year 11899.
    """
    n = _int(value)
    if n is None or n <= 0:
        # 0 is the missing code in both eras, not model year 1900.
        return None
    if n < 1000:
        return None if n == 99 else str(1900 + n)
    if n in (9998, 9999) or not (1900 <= n <= 2100):
        return None
    return str(n)


def blood_alcohol(value: str, year: int) -> str | None:
    """Convert the FARS alcohol test code to a BAC in g/dL.

    The scale changed in 2015. Through 2014 the code is the concentration times
    100 over 0-94, with 95-99 reserved for refused / not given / unknown. From
    2015 it is the concentration times 1000 over 0-940, with 995-999 reserved.
    Casting the code straight to a number turns "96 = test not given" into a
    lethal 0.96 g/dL, which is roughly 45% of all person rows.
    """
    n = _int(value)
    if n is None:
        return None
    if year <= 2014:
        return f"{n / 100:.2f}" if 0 <= n <= 94 else None
    return f"{n / 1000:.3f}" if 0 <= n <= 940 else None


def date_from(y: str | int | None, m: str, d: str) -> str | None:
    """Build an ISO date, or None when any part carries an unknown code."""
    yy, mm, dd = _int(str(y)), _int(m), _int(d)
    if yy is None or mm is None or dd is None:
        return None
    if not (1900 <= yy <= 2100 and 1 <= mm <= 12 and 1 <= dd <= 31):
        return None
    try:
        import datetime

        return datetime.date(yy, mm, dd).isoformat()
    except ValueError:
        return None


def county_id_for(state: str, county: str) -> str | None:
    """Build a five-digit county FIPS, or None where there is no real county."""
    s, c = _int(state), _int(county)
    if s is None or c is None:
        return None
    if c in constants.COUNTY_SENTINELS.value or c <= 0:
        return None
    return f"{s:02d}{c:03d}"


# --------------------------------------------------------------------------
# Sentinels
# --------------------------------------------------------------------------

# Codes that must not survive as numbers, per canonical numeric column. Each set
# was read off NHTSA's own PROC FORMAT source for the year and confirmed against
# the value distributions, not assumed: the bands differ per variable and moved
# over time, and several values that look like sentinels are real top-codes.
# ``travel_speed`` is the clearest case - 98 means "98 mph or greater" through
# 2008 but "not reported" from 2009, and 997 is a genuine top-code, so a single
# fixed band would either destroy real observations or invent impossible speeds.
SENTINELS: dict[str, dict[str, set[int]]] = {
    "crash": {
        "month": {0, 99},
        "day": {0, 99},
        "hour": {24, 88, 99},
        "minute": {88, 99},
        "notification_hour": {24, 88, 99},
        "notification_minute": {88, 99},
        "arrival_hour": {24, 88, 99},
        "arrival_minute": {88, 99},
        "hospital_arrival_hour": {24, 88, 99},
        "hospital_arrival_minute": {88, 99},
        "speed_limit": {0, 98, 99},
        "milepoint": {0, 99998, 99999},
    },
    "vehicle": {
        "occupants_count": {98, 99, 998, 999},
        "driver_height": {0, 997, 998, 999},
        "driver_weight": {0, 997, 998, 999},
        "previous_crashes_count": {98, 99, 998, 999},
        "previous_suspensions_count": {98, 99, 998, 999},
        "previous_dwi_convictions_count": {98, 99, 998, 999},
        "previous_speeding_convictions_count": {98, 99, 998, 999},
        "previous_other_convictions_count": {98, 99, 998, 999},
        "vehicle_speed_limit": {0, 98, 99},
    },
    "person": {
        # 88 = not applicable (survivor), 97 = redacted, 99 = unknown; 24 is the
        # legacy unknown-hour code used before the 88/99 pair was introduced.
        "death_hour": {24, 88, 97, 99},
        "death_minute": {88, 97, 99},
        # Only 999 is unknown here. 99 is a real lag of 99 hours and must be
        # kept: FARS counts deaths up to 30 days (720 hours) after the crash.
        "survival_hours": {999},
        "survival_minutes": {99},
    },
}


def travel_speed(value: str, year: int) -> str | None:
    """Clean TRAV_SP, whose sentinel band moved in 2009.

    Through 2008 the field is two digits and 98 is the real top-code "98 mph or
    greater", so it is kept: 98 is the correct numeric floor and dropping it
    would delete the high-speed tail for 34 years. Only 99 means unknown.

    From 2009 the field is three digits and 997 means "greater than 151 mph".
    That is a bound, not a speed, so it is nulled rather than stored: leaving it
    in a miles-per-hour column would assert a 997 mph crash, the same class of
    error as reading the alcohol sentinel 96 as a 0.96 g/dL blood alcohol level.
    998 and 999 are not-reported and unknown.
    """
    return num(value, {99} if year <= 2008 else {997, 998, 999})


def age(value: str, year: int) -> str | None:
    """Clean AGE, whose unknown code moved from 99 to 998/999 in 2009.

    0 is a real value ("less than one year") and is kept. Through 2008 the field
    is two digits and 99 is documented as unknown, which is why no pre-2009 year
    reports anyone older than 98.
    """
    return num(value, {99} if year <= 2008 else {998, 999})


# --------------------------------------------------------------------------
# Row transforms
# --------------------------------------------------------------------------


def _plain(name: str, table: str, cols_by_name: dict[str, Col]):
    """Return the default cleaner for a column, chosen by its declared type."""
    col = cols_by_name[name]
    if col.bq_type in ("INT64", "FLOAT64"):
        sentinels = SENTINELS.get(table, {}).get(name, set())
        return lambda v, y: num(v, sentinels)
    return lambda v, y: code(v)


# Columns whose cleaning is not implied by their type alone.
SPECIAL = {
    ("crash", "latitude"): lambda v, y: coord(v),
    ("crash", "longitude"): lambda v, y: coord(v, is_longitude=True),
    ("vehicle", "model_year"): model_year,
    ("vehicle", "travel_speed"): travel_speed,
    ("person", "model_year"): model_year,
    ("person", "age"): age,
}


def clean_table(
    table: str, zip_path: Path, year: int, cols: list[Col]
) -> list[dict]:
    """Clean one source file for one year into a list of canonical-column rows."""
    by_name = {c.name: c for c in cols}
    cleaners = {
        c.name: SPECIAL.get((table, c.name)) or _plain(c.name, table, by_name)
        for c in cols
    }
    stem = constants.SOURCE_FILE.value[table]
    rows: list[dict] = []
    positions: dict[str, int] | None = None

    for index, raw in read_rows(zip_path, stem):
        if positions is None:
            positions = resolve(cols, index)
        row: dict[str, str | None] = {}
        for c in cols:
            pos = positions.get(c.name)
            if pos is None or pos >= len(raw):
                row[c.name] = None
                continue
            row[c.name] = cleaners[c.name](raw[pos], year)

        # year is not a column of vehicle.csv or person.csv, and in accident.csv
        # it is two-digit before 1982; the annual file is the reliable source.
        row["year"] = str(year)

        if "state_id" in row and row["state_id"] is not None:
            row["state_id"] = f"{int(row['state_id']):02d}"

        if table == "crash":
            row["county_id"] = county_id_for(
                row.get("state_id") or "", _raw(index, raw, "COUNTY")
            )
            row["date"] = date_from(
                year, _raw(index, raw, "MONTH"), _raw(index, raw, "DAY")
            )
        elif table == "person":
            row["blood_alcohol_content"] = blood_alcohol(
                _first(index, raw, ("ALC_RES", "TEST_RES")), year
            )
            row["death_date"] = date_from(
                _raw(index, raw, "DEATH_YR"),
                _raw(index, raw, "DEATH_MO"),
                _raw(index, raw, "DEATH_DA"),
            )
            # Before 2010 a survivor's death time is stored as 0 rather than as
            # a not-applicable code, which is indistinguishable from midnight.
            # Gate the whole death block on there being a death date.
            if row["death_date"] is None:
                row["death_hour"] = None
                row["death_minute"] = None
        rows.append(row)
    return rows


def _raw(index: dict[str, int], row: list[str], name: str) -> str:
    pos = index.get(name)
    return row[pos] if pos is not None and pos < len(row) else ""


def _first(
    index: dict[str, int], row: list[str], names: tuple[str, ...]
) -> str:
    for n in names:
        if n in index:
            return _raw(index, row, n)
    return ""


# --------------------------------------------------------------------------
# Codebook
# --------------------------------------------------------------------------

VALUE_RE = re.compile(
    r"\bVALUE\s+(\$?[A-Z0-9_]+)\s*(?:\([^)]*\))?\s*(.*?);", re.S | re.I
)
PAIR_RE = re.compile(
    r"""(?P<k>'[^']*'|"[^"]*"|[-\w.]+(?:\s*-\s*[-\w.]+)?|OTHER)\s*=\s*"""
    r"""(?P<v>'[^']*'|"[^"]*")""",
    re.I,
)


def parse_sas_formats(text: str) -> dict[str, dict[str, str]]:
    """Parse SAS ``PROC FORMAT ... VALUE`` blocks into {format: {code: label}}."""
    out: dict[str, dict[str, str]] = {}
    for m in VALUE_RE.finditer(text):
        name = m.group(1).upper().lstrip("$")
        pairs = {
            p.group("k").strip().strip("'\""): p.group("v")
            .strip()
            .strip("'\"")
            for p in PAIR_RE.finditer(m.group(2))
        }
        if pairs:
            out.setdefault(name, {}).update(pairs)
    return out


def codebook_from_sas(
    year: int, input_dir: Path, session=None
) -> dict[str, dict]:
    """Build {table: {source_var: {code: label}}} from the year's SAS release.

    Through 2014 the CSVs carry codes only. NHTSA's SAS release ships both the
    PROC FORMAT source (format name -> labels) and the sas7bdat column metadata
    (variable -> format name), and the two together are the authoritative
    codebook for that year - better than transcribing the PDF manual, and the
    only machine-readable option for 1975-2014.
    """
    import pandas as pd

    zip_path = download_formats(year, Path(input_dir), session)
    book: dict[str, dict] = {}
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        text = "".join(
            zf.read(n).decode("latin-1") + "\n"
            for n in names
            if n.lower().endswith(".sas")
        )
        formats = parse_sas_formats(text)
        for table, stem in constants.SOURCE_FILE.value.items():
            hits = [
                n
                for n in names
                if re.search(rf"(^|/){stem}\.sas7bdat$", n, re.I)
            ]
            if not hits:
                continue
            with tempfile.NamedTemporaryFile(
                suffix=".sas7bdat", delete=False
            ) as tmp:
                tmp.write(zf.read(hits[0]))
                path = tmp.name
            try:
                reader = pd.read_sas(path, format="sas7bdat", iterator=True)
                # The reader exposes per-column metadata at runtime, which is
                # where the variable -> format-name binding lives; the stub does
                # not declare it.
                bound = {
                    c.name.upper(): (c.format or "").upper()
                    # pyrefly: ignore [missing-attribute]
                    for c in reader.columns
                }
                reader.close()
            finally:
                os.unlink(path)
            book[table] = {
                var: formats[fmt.lstrip("$")]
                for var, fmt in bound.items()
                if fmt and fmt.lstrip("$") in formats
            }
    return book


def codebook_from_names(year: int, input_dir: Path) -> dict[str, dict]:
    """Build the codebook from the ``<VAR>NAME`` companion columns.

    From 2015 the CSVs ship a readable label beside every coded column, so the
    year documents itself and no SAS download is needed.
    """
    zip_path = Path(input_dir) / f"FARS{year}.zip"
    book: dict[str, dict] = {}
    for table, stem in constants.SOURCE_FILE.value.items():
        pairs: dict[str, dict[str, str]] = defaultdict(dict)
        partners: dict[str, tuple[int, int]] = {}
        for index, raw in read_rows(zip_path, stem):
            if not partners:
                partners = {
                    var: (index[var], index[var + "NAME"])
                    for var in index
                    if not var.endswith("NAME") and var + "NAME" in index
                }
            for var, (ci, li) in partners.items():
                if ci < len(raw) and li < len(raw):
                    c, label = raw[ci].strip(), raw[li].strip()
                    if c and label:
                        pairs[var][c] = label
        book[table] = {k: dict(v) for k, v in pairs.items()}
    return book


def codebook_for_year(
    year: int, input_dir: Path, session=None
) -> dict[str, dict]:
    if year <= constants.LAST_FORMAT_YEAR.value:
        return codebook_from_sas(year, input_dir, session)
    return codebook_from_names(year, input_dir)


def build_dicionario(
    years: list[int], input_dir: Path, output_dir: Path
) -> int:
    """Write the dicionario partition from NHTSA's own per-year code sets.

    One row per (table, column, code, era, meaning). ``cobertura_temporal`` is
    not decoration here: nearly every FARS variable had its code set redefined at
    least once - light condition four times, vehicle body type eleven - so the
    same code means different things in different years and a single flat
    code->label map would be wrong for most of the span. Consecutive years that
    agree on a meaning are collapsed into one range; a year that disagrees opens
    a new one.
    """
    tables = {t: load_cols(t) for t in constants.DATA_TABLES.value}
    # canonical name -> the source aliases that may carry it
    wanted = {
        table: {
            alias: c.name
            for c in cols
            if c.covered_by_dictionary
            for alias in c.aliases
        }
        for table, cols in tables.items()
    }

    # (table, column, code) -> {year: label}
    seen: dict[tuple[str, str, str], dict[int, str]] = defaultdict(dict)
    for year in years:
        book = codebook_for_year(year, input_dir)
        for table, by_var in book.items():
            aliases = wanted.get(table, {})
            for var, labels in by_var.items():
                canonical = aliases.get(var.upper())
                if not canonical:
                    continue
                for raw_code, label in labels.items():
                    key = code(raw_code)
                    if key is None or not label.strip():
                        continue
                    seen[(table, canonical, key)][year] = label.strip()

    # Codes NHTSA published in the data but never defined in any year's format
    # file. There are a few hundred, concentrated in the early years and in the
    # two high-cardinality reference fields (city, make-model). They are recorded
    # as undocumented rather than omitted: a dictionary that silently drops the
    # codes it cannot explain is worse than one that names the gap, and it would
    # also leave custom_dictionary_coverage failing with no way to satisfy it.
    for table, observed in _observed_codes(output_dir, tables).items():
        for column, by_code in observed.items():
            for key, years in by_code.items():
                if (table, column, key) not in seen:
                    seen[(table, column, key)] = dict.fromkeys(
                        years, UNDOCUMENTED
                    )

    rows = []
    for (table, column, key), by_year in sorted(seen.items()):
        for lo, hi, label in _runs(by_year):
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": f"{lo}(1){hi}",
                    "valor": label,
                }
            )
    cols = load_cols("dicionario")
    out = Path(output_dir) / "dicionario"
    out.mkdir(parents=True, exist_ok=True)
    schema = _string_schema(cols)
    table = pa.Table.from_arrays(
        [pa.array([r[c.name] for r in rows], type=pa.string()) for c in cols],
        schema=schema,
    )
    pq.write_table(table, out / "data.parquet", compression="snappy")
    return len(rows)


def _observed_codes(
    output_dir: Path, tables: dict[str, list[Col]]
) -> dict[str, dict[str, dict[str, list[int]]]]:
    """Collect {table: {column: {code: [years]}}} from the cleaned parquet."""
    import pyarrow.dataset as pads

    out: dict[str, dict[str, dict[str, list[int]]]] = {}
    for table, cols in tables.items():
        part = Path(output_dir) / table
        if not part.exists():
            continue
        wanted = [c.name for c in cols if c.covered_by_dictionary]
        if not wanted:
            continue
        data = pads.dataset(part, format="parquet")
        per_col: dict[str, dict[str, set[int]]] = {
            c: defaultdict(set) for c in wanted
        }
        for batch in data.to_batches(columns=[*wanted, "year"]):
            years = [int(y) for y in batch.column(len(wanted)).to_pylist()]
            for i, name in enumerate(wanted):
                vals = batch.column(i).to_pylist()
                acc = per_col[name]
                for v, y in zip(vals, years, strict=True):
                    if v is not None and v != "":
                        acc[v].add(y)
        out[table] = {
            c: {k: sorted(v) for k, v in per_col[c].items()} for c in wanted
        }
    return out


def _runs(by_year: dict[int, str]) -> list[tuple[int, int, str]]:
    """Collapse {year: label} into maximal runs of consecutive equal labels."""
    out: list[list] = []
    for year in sorted(by_year):
        label = by_year[year]
        if out and out[-1][2] == label and year == out[-1][1] + 1:
            out[-1][1] = year
        else:
            out.append([year, year, label])
    return [(a, b, lab) for a, b, lab in out]


# --------------------------------------------------------------------------
# Parquet output
# --------------------------------------------------------------------------


def _string_schema(cols: list[Col]) -> pa.Schema:
    """All-STRING schema fixing column order.

    Staging is all-STRING by house convention and ``gcs.dump_header``
    stringifies the header regardless, so the schema fixes order, not types; the
    dbt model does the ``safe_cast`` to each architecture type.
    """
    return pa.schema([(c.name, pa.string()) for c in cols])


def write_partition(
    rows: list[dict], cols: list[Col], out_dir: Path, year: int
) -> int:
    """Write one ``year=<Y>/data.parquet`` partition. Returns the row count.

    An empty year writes no file at all: ``gcs.dump_header`` infers the staging
    table's schema from the first file it finds, and a zero-row parquet gives it
    nothing to go on, so BigQuery types every column INTEGER and every real
    partition then fails to read.
    """
    if not rows:
        return 0
    part = Path(out_dir) / f"year={year}"
    part.mkdir(parents=True, exist_ok=True)
    schema = _string_schema(cols)
    table = pa.Table.from_arrays(
        [
            pa.array([r.get(c.name) or None for r in rows], type=pa.string())
            for c in cols
        ],
        schema=schema,
    )
    pq.write_table(table, part / "data.parquet", compression="snappy")
    return table.num_rows


def assert_all_string(path: Path) -> None:
    """Fail loudly if any parquet file under ``path`` carries a non-string column."""
    for f in sorted(Path(path).rglob("*.parquet")):
        schema = pq.read_schema(f)
        bad = [
            n
            for n, t in zip(schema.names, schema.types, strict=True)
            if not pa.types.is_string(t)
        ]
        if bad:
            raise AssertionError(f"{f}: non-string columns {bad}")


def clean_year(
    year: int, input_dir: Path, output_dir: Path, session=None
) -> dict:
    """Download and clean all three tables for one year. Returns row counts."""
    zip_path = download_year(year, Path(input_dir), session)
    counts = {}
    for table in constants.DATA_TABLES.value:
        cols = load_cols(table)
        rows = clean_table(table, zip_path, year, cols)
        counts[table] = write_partition(
            rows, cols, Path(output_dir) / table, year
        )
    return counts


def clean_all(
    years: list[int], input_dir: Path, output_dir: Path, session=None
) -> dict:
    """Clean every requested year, then rebuild the dicionario over all of them."""
    totals: dict[str, int] = defaultdict(int)
    for year in years:
        for table, n in clean_year(
            year, input_dir, output_dir, session
        ).items():
            totals[table] += n
    totals["dicionario"] = build_dicionario(
        years, Path(input_dir), Path(output_dir)
    )
    return dict(totals)


def source_max_date(year: int) -> str:
    """Coverage date of the newest published annual file, as YYYY-MM-DD."""
    return f"{year}-01-01"
