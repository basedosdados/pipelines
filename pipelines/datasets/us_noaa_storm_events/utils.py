"""Pure download and transform helpers for us_noaa_storm_events — no Prefect imports.

This module is the single home of the cleaning transform. The one-shot onboarding
scripts under ``models/us_noaa_storm_events/code/`` import these functions rather
than duplicating them, so the recurring pipeline and the bootstrap cannot drift.

Four source traps the transform exists to neutralise, each measured over the full
1950-2026 corpus:

* ``DAMAGE_PROPERTY`` / ``DAMAGE_CROPS`` are **text with a magnitude suffix** —
  ``2.5K``, ``1.50M``, ``10.00B``, and also ``.5K`` with no leading zero (17,541
  rows). A naive numeric cast yields null or 2.5 instead of 2,500. They are
  decoded to dollars and the published string is kept beside the number.
* ``BEGIN_DATE_TIME`` is **``DD-MON-YY``** on every one of the 2,041,816 rows,
  not the ``MM/DD/YYYY`` the source's own format document claims, so its
  two-digit year cannot separate 1950 from 2050. Datetimes are built from the
  numeric ``*_YEARMONTH`` / ``*_DAY`` / ``*_TIME`` fields instead. The fatalities
  file then uses ``MM/DD/YYYY`` for its own date column — a third format.
* ``CZ_FIPS`` is a **county code only when ``CZ_TYPE`` is ``C``**; otherwise it
  is an NWS forecast or marine zone number. Concatenating it with the state code
  unconditionally would mint 740,435 bogus county ids.
* ``LAT2`` / ``LON2`` are **dropped**: undocumented, unsigned, and packed in two
  different encodings across eras (``DDMM`` on older rows, ``DDMMMMM`` — degrees
  plus thousandths of a minute — on newer ones). They round-trip to ``LATITUDE``
  / ``LONGITUDE`` and are never present when those are absent, so they carry no
  information and would read as plain numbers to anyone who did not know.

Output parquet is **all-STRING**. ``pipelines.utils.gcs.dump_header`` stringifies
the header BigQuery infers the staging schema from, so typed parquet is rejected;
the dbt model ``safe_cast``s every column to its architecture type.
"""

import csv
import gzip
import re
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_noaa_storm_events.constants import constants

csv.field_size_limit(sys.maxsize)

DOWNLOAD_CHUNK = 8 * 1024 * 1024
REQUEST_TIMEOUT = 300

# StormEvents_<family>-ftp_v<ver>_d<year>_c<created>.csv.gz
FILE_RE = re.compile(
    r"StormEvents_(?P<family>details|fatalities|locations)"
    r"-ftp_v[\d.]+_d(?P<year>\d{4})_c(?P<created>\d{8})\.csv\.gz"
)

# A damage figure: optional leading dot (".5K"), optional magnitude suffix.
DAMAGE_RE = re.compile(r"^(\d*\.?\d+)([A-Za-z]?)$")


@dataclass
class Col:
    """One column of an architecture table.

    The transform only needs ``name`` / ``bq_type`` / ``original``; the remaining
    fields are carried so the metadata scripts read the same TSV through the same
    parser rather than writing a second one.
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
    """Load ordered column specs from the architecture TSV for a table."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"sheet_{table}.tsv"
    cols = []
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
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


# --------------------------------------------------------------------------
# Source listing and download
# --------------------------------------------------------------------------


def list_source_files(session: requests.Session | None = None) -> dict:
    """Return ``{(family, year): {"file": str, "created": "YYYYMMDD"}}``.

    ``created`` is the ``c`` token in the file name. Per the directory's README
    it is bumped both when a year gains new data and when an earlier year is
    corrected, which makes it the signal the refresh keys on.
    """
    s = session or requests.Session()
    r = s.get(constants.LISTING_URL.value, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    out: dict[tuple[str, int], dict] = {}
    for m in FILE_RE.finditer(r.text):
        key = (m.group("family"), int(m.group("year")))
        created = m.group("created")
        # A year should appear once per family; keep the newest if it ever does not.
        if key not in out or created > out[key]["created"]:
            out[key] = {"file": m.group(0), "created": created}
    if not out:
        raise RuntimeError(
            f"no source files parsed from {constants.LISTING_URL.value}"
        )
    return out


def download_file(
    name: str, dest_dir: Path, session: requests.Session | None = None
) -> Path:
    """Download one gzipped CSV into ``dest_dir``, returning its path."""
    s = session or requests.Session()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / name
    url = f"{constants.BASE_URL.value}/{name}"
    with s.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
        r.raise_for_status()
        # decode_content=False: the body is a .gz we want byte-for-byte, and the
        # server does not apply a transfer encoding on top of it.
        with open(dest, "wb") as fh:
            shutil.copyfileobj(r.raw, fh, DOWNLOAD_CHUNK)
    if dest.stat().st_size == 0:
        raise RuntimeError(f"empty download: {name}")
    return dest


def download_years(
    years: list[int], input_dir: Path, listing: dict | None = None
) -> dict:
    """Download every family for each year in ``years``. Returns the listing used."""
    listing = listing or list_source_files()
    s = requests.Session()
    for year in years:
        for family in constants.FAMILIES.value:
            entry = listing.get((family, year))
            if entry is None:
                raise RuntimeError(f"source has no {family} file for {year}")
            target = input_dir / entry["file"]
            if not target.exists():
                download_file(entry["file"], input_dir, session=s)
    return listing


def _source_path(
    input_dir: Path, family: str, year: int, listing: dict
) -> Path:
    entry = listing.get((family, year))
    if entry is None:
        raise RuntimeError(f"source has no {family} file for {year}")
    return input_dir / entry["file"]


def read_rows(path: Path):
    """Yield dict rows from a gzipped CSV.

    Read with Python's ``csv`` module: the narrative columns contain newlines
    inside quoted fields, which desynchronises readers that split on raw
    newlines. The files themselves are well-formed RFC 4180.
    """
    with gzip.open(
        path, "rt", encoding="utf-8", errors="replace", newline=""
    ) as fh:
        yield from csv.DictReader(fh)


# --------------------------------------------------------------------------
# Field-level transforms
# --------------------------------------------------------------------------


def parse_damage(raw: str) -> str:
    """Decode a damage figure to a plain dollar amount, as a string.

    ``"2.5K"`` -> ``"2500"``, ``".5K"`` -> ``"500"``, ``"10.00B"`` ->
    ``"10000000000"``, ``"0"`` -> ``"0"``. Returns ``""`` when the value is
    blank or cannot be decoded — the 49 rows carrying an ``H`` or ``T`` suffix,
    or a suffix with no number, are left null rather than guessed at, with the
    published text preserved in the companion ``*_source`` column.
    """
    v = (raw or "").strip()
    if not v:
        return ""
    m = DAMAGE_RE.match(v)
    if not m:
        return ""
    suffix = m.group(2).upper()
    mult = constants.DAMAGE_MULTIPLIER.value.get(suffix)
    if mult is None:
        return ""
    value = float(m.group(1)) * mult
    # Every decoded amount is a whole number of dollars in this source; format as
    # an integer so BigQuery does not read "2500.0" for a value the source wrote
    # as "2.5K".
    return f"{value:.0f}" if value == int(value) else f"{value:g}"


def build_datetime(yearmonth: str, day: str, time: str) -> str:
    """Assemble ``YYYY-MM-DD HH:MM:SS`` from the source's numeric date parts.

    Returns ``""`` when year, month or day is missing or out of range. ``time``
    is ``hhmm`` with no zero padding (``"0"`` means midnight, ``"145"`` 01:45).
    """
    ym = (yearmonth or "").strip()
    d = (day or "").strip()
    if len(ym) != 6 or not ym.isdigit() or not d.isdigit():
        return ""
    year, month, dd = int(ym[:4]), int(ym[4:]), int(d)
    if not (1 <= month <= 12 and 1 <= dd <= 31):
        return ""
    t = (time or "").strip()
    hh = mm = 0
    if t.isdigit():
        t = t.zfill(4)
        hh, mm = int(t[:2]), int(t[2:])
        # 2400 occurs for midnight at the end of a day; clamp rather than drop.
        if hh == 24:
            hh, mm = 23, 59
        if hh > 23 or mm > 59:
            hh = mm = 0
    return f"{year:04d}-{month:02d}-{dd:02d} {hh:02d}:{mm:02d}:00"


def state_id_for(state_fips: str) -> str:
    """Map the published NWS state code to a real two-digit FIPS code.

    Returns ``""`` for the marine and Great Lakes zones (codes 81-95), which are
    not states, and for a blank input. Territory codes are remapped per
    ``constants.STATE_FIPS_REMAP``.
    """
    v = (state_fips or "").strip()
    if not v or not v.isdigit():
        return ""
    n = int(v)
    if constants.MARINE_FIPS_MIN.value <= n <= constants.MARINE_FIPS_MAX.value:
        return ""
    code = f"{n:02d}"
    return constants.STATE_FIPS_REMAP.value.get(code, code)


def county_id_for(state_fips: str, cz_type: str, cz_fips: str) -> str:
    """Build a five-digit county FIPS, or ``""`` where there is no county.

    Only ``cz_type == "C"`` rows carry a county code in ``cz_fips``; on ``Z``
    (forecast zone) and ``M`` (marine zone) rows the same field holds a zone
    number that must not be read as a county. County code ``000`` marks a
    state-wide occurrence and likewise yields no county.
    """
    if (cz_type or "").strip() != "C":
        return ""
    sid = state_id_for(state_fips)
    cz = (cz_fips or "").strip()
    if not sid or not cz.isdigit() or int(cz) == 0:
        return ""
    return f"{sid}{int(cz):03d}"


def _num(raw: str) -> str:
    """Pass a numeric field through, blanking anything not parseable as a number.

    Keeps the published text (so ``43.2`` stays ``43.2``) rather than
    round-tripping through float, which would rewrite precision.
    """
    v = (raw or "").strip()
    if not v:
        return ""
    try:
        float(v)
    except ValueError:
        return ""
    return v


def _int(raw: str) -> str:
    v = (raw or "").strip()
    if not v:
        return ""
    try:
        return str(int(float(v)))
    except ValueError:
        return ""


# --------------------------------------------------------------------------
# Row-level transforms, one per table
# --------------------------------------------------------------------------


def clean_event(row: dict) -> dict:
    return {
        "year": row["YEAR"].strip(),
        "month": _int(row["BEGIN_YEARMONTH"].strip()[4:]),
        "event_id": row["EVENT_ID"].strip(),
        "episode_id": row["EPISODE_ID"].strip(),
        "state_id": state_id_for(row["STATE_FIPS"]),
        "county_id": county_id_for(
            row["STATE_FIPS"], row["CZ_TYPE"], row["CZ_FIPS"]
        ),
        "state_name": row["STATE"].strip(),
        "state_fips_nws": row["STATE_FIPS"].strip(),
        "cz_type": row["CZ_TYPE"].strip(),
        "cz_fips": row["CZ_FIPS"].strip(),
        "cz_name": row["CZ_NAME"].strip(),
        "wfo": row["WFO"].strip(),
        "event_type": row["EVENT_TYPE"].strip(),
        "begin_datetime": build_datetime(
            row["BEGIN_YEARMONTH"], row["BEGIN_DAY"], row["BEGIN_TIME"]
        ),
        "end_datetime": build_datetime(
            row["END_YEARMONTH"], row["END_DAY"], row["END_TIME"]
        ),
        "timezone": row["CZ_TIMEZONE"].strip(),
        "injuries_direct": _int(row["INJURIES_DIRECT"]),
        "injuries_indirect": _int(row["INJURIES_INDIRECT"]),
        "deaths_direct": _int(row["DEATHS_DIRECT"]),
        "deaths_indirect": _int(row["DEATHS_INDIRECT"]),
        "damage_property": parse_damage(row["DAMAGE_PROPERTY"]),
        "damage_property_source": row["DAMAGE_PROPERTY"].strip(),
        "damage_crops": parse_damage(row["DAMAGE_CROPS"]),
        "damage_crops_source": row["DAMAGE_CROPS"].strip(),
        "source": row["SOURCE"].strip(),
        "magnitude": _num(row["MAGNITUDE"]),
        "magnitude_type": row["MAGNITUDE_TYPE"].strip(),
        "flood_cause": row["FLOOD_CAUSE"].strip(),
        "hurricane_category": row["CATEGORY"].strip(),
        "tornado_scale": row["TOR_F_SCALE"].strip(),
        "tornado_length": _num(row["TOR_LENGTH"]),
        "tornado_width": _num(row["TOR_WIDTH"]),
        "tornado_other_wfo": row["TOR_OTHER_WFO"].strip(),
        "tornado_other_state_abbreviation": row["TOR_OTHER_CZ_STATE"].strip(),
        "tornado_other_cz_fips": row["TOR_OTHER_CZ_FIPS"].strip(),
        "tornado_other_cz_name": row["TOR_OTHER_CZ_NAME"].strip(),
        "begin_range": _num(row["BEGIN_RANGE"]),
        "begin_azimuth": row["BEGIN_AZIMUTH"].strip(),
        "begin_location": row["BEGIN_LOCATION"].strip(),
        "end_range": _num(row["END_RANGE"]),
        "end_azimuth": row["END_AZIMUTH"].strip(),
        "end_location": row["END_LOCATION"].strip(),
        "begin_latitude": _num(row["BEGIN_LAT"]),
        "begin_longitude": _num(row["BEGIN_LON"]),
        "end_latitude": _num(row["END_LAT"]),
        "end_longitude": _num(row["END_LON"]),
        "episode_narrative": row["EPISODE_NARRATIVE"].strip(),
        "event_narrative": row["EVENT_NARRATIVE"].strip(),
        "data_source": row["DATA_SOURCE"].strip(),
    }


def clean_fatality(row: dict, year: int) -> dict:
    return {
        "year": str(year),
        "event_id": row["EVENT_ID"].strip(),
        "fatality_id": row["FATALITY_ID"].strip(),
        "fatality_datetime": build_datetime(
            row["FAT_YEARMONTH"], row["FAT_DAY"], row["FAT_TIME"]
        ),
        "fatality_type": row["FATALITY_TYPE"].strip(),
        "age": _int(row["FATALITY_AGE"]),
        "sex": row["FATALITY_SEX"].strip(),
        "location": row["FATALITY_LOCATION"].strip(),
    }


def clean_event_location(row: dict, year: int) -> dict:
    return {
        "year": str(year),
        "event_id": row["EVENT_ID"].strip(),
        "episode_id": row["EPISODE_ID"].strip(),
        "location_index": row["LOCATION_INDEX"].strip(),
        "location_range": _num(row["RANGE"]),
        "location_azimuth": row["AZIMUTH"].strip(),
        "location_name": row["LOCATION"].strip(),
        "latitude": _num(row["LATITUDE"]),
        "longitude": _num(row["LONGITUDE"]),
    }


# --------------------------------------------------------------------------
# Parquet output
# --------------------------------------------------------------------------


def _string_schema(cols: list[Col]) -> pa.Schema:
    """An all-STRING arrow schema carrying the architecture's column order.

    Staging is all-STRING by house convention and ``dump_header`` stringifies
    the header regardless, so the schema fixes order, not types.
    """
    return pa.schema([(c.name, pa.string()) for c in cols])


def write_partition(
    rows: list[dict], cols: list[Col], out_dir: Path, year: int
) -> int:
    """Write one ``year=<Y>/data.parquet`` partition. Returns the row count.

    An empty year writes **no file at all**. ``event_location`` has no rows for
    1950-1971 and 1973-1995 — 46 empty years — and ``gcs.dump_header`` builds the
    staging table's schema from the first file it finds. Given a zero-row parquet
    it has no values to go on and BigQuery autodetect types the columns INTEGER,
    after which every real partition fails to read:

        Parquet column 'episode_id' has type BYTE_ARRAY which does not match the
        target cpp_type INT64

    Skipping empty partitions removes the hazard and loses nothing: a zero-row
    file carries no data, and the years are recoverable from the source anyway.
    """
    if not rows:
        return 0
    part = out_dir / f"year={year}"
    part.mkdir(parents=True, exist_ok=True)
    schema = _string_schema(cols)
    arrays = [
        pa.array([r.get(c.name) or None for r in rows], type=pa.string())
        for c in cols
    ]
    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, part / "data.parquet", compression="snappy")
    return table.num_rows


def clean_year(
    year: int, input_dir: Path, output_dir: Path, listing: dict
) -> dict:
    """Clean all three families for one year into partitioned parquet."""
    counts = {}

    ev_cols = load_cols("event")
    rows = [
        clean_event(r)
        for r in read_rows(_source_path(input_dir, "details", year, listing))
    ]
    counts["event"] = write_partition(
        rows, ev_cols, output_dir / "event", year
    )

    fa_cols = load_cols("fatality")
    rows = [
        clean_fatality(r, year)
        for r in read_rows(
            _source_path(input_dir, "fatalities", year, listing)
        )
    ]
    counts["fatality"] = write_partition(
        rows, fa_cols, output_dir / "fatality", year
    )

    lo_cols = load_cols("event_location")
    rows = [
        clean_event_location(r, year)
        for r in read_rows(_source_path(input_dir, "locations", year, listing))
    ]
    counts["event_location"] = write_partition(
        rows, lo_cols, output_dir / "event_location", year
    )
    return counts


def clean_all(
    input_dir: Path, output_dir: Path, years: list[int], listing: dict
) -> dict:
    """Clean the given years. Returns ``{table: total_rows}``."""
    totals: dict[str, int] = defaultdict(int)
    for year in years:
        for table, n in clean_year(
            year, input_dir, output_dir, listing
        ).items():
            totals[table] += n
    return dict(totals)


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------


def build_dicionario(output_dir: Path) -> int:
    """Build the dicionario table from the cleaned parquet partitions.

    The source publishes readable labels for most coded columns rather than
    codes, so ``valor`` repeats ``chave`` for those; the register earns its keep
    through ``cobertura_temporal``, which is what exposes that event_type
    coverage is not uniform across eras.
    """
    import pyarrow.dataset as ds

    rows = []
    for table, columns in constants.DICT_COLUMNS.value.items():
        part = output_dir / table
        if not part.exists():
            continue
        # No hive partitioning: the year is already a column inside every file,
        # and letting pyarrow also derive it from the directory name yields an
        # int32 partition field that will not merge with the string column.
        data = ds.dataset(part, format="parquet")
        for column in columns:
            seen: dict[str, list[int]] = defaultdict(list)
            for batch in data.to_batches(columns=[column, "year"]):
                vals = batch.column(0).to_pylist()
                years = batch.column(1).to_pylist()
                for v, y in zip(vals, years, strict=True):
                    if v is None or v == "":
                        continue
                    seen[v].append(int(y))
            for value, years in sorted(seen.items()):
                lo, hi = min(years), max(years)
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": value,
                        "cobertura_temporal": f"{lo}(1){hi}",
                        "valor": DICT_LABELS.get((column, value), value),
                    }
                )
    out = output_dir / "dicionario"
    out.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            (c, pa.string())
            for c in (
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            )
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array([r[c] for r in rows], type=pa.string())
            for c in schema.names
        ],
        schema=schema,
    )
    pq.write_table(table, out / "data.parquet", compression="snappy")
    return table.num_rows


# Expansions for the codes the source does NOT publish as readable labels. Taken
# from the NCEI Storm Data Bulk CSV Format document; every other dictionary-
# covered column already carries its own label, for which valor repeats chave.
DICT_LABELS = {
    ("cz_type", "C"): "County/Parish",
    ("cz_type", "Z"): "NWS Public Forecast Zone",
    ("cz_type", "M"): "Marine",
    ("magnitude_type", "EG"): "Estimated gust",
    ("magnitude_type", "ES"): "Estimated sustained wind",
    ("magnitude_type", "MG"): "Measured gust",
    ("magnitude_type", "MS"): "Measured sustained wind",
    ("magnitude_type", "E"): "Estimated",
    ("magnitude_type", "M"): "Measured",
    ("data_source", "CSV"): "Comma-separated values load",
    ("data_source", "PDS"): "Storm Data publication, scanned",
    ("data_source", "PDC"): "Storm Data publication, corrected",
    ("data_source", "PUB"): "Storm Data publication",
    ("fatality_type", "D"): "Direct fatality",
    ("fatality_type", "I"): "Indirect fatality",
    ("sex", "M"): "Male",
    ("sex", "F"): "Female",
}


# --------------------------------------------------------------------------
# Coverage
# --------------------------------------------------------------------------


def source_max_date(input_dir: Path, listing: dict, year: int) -> str:
    """Return the latest ``YYYY-MM-01`` present in the given year's details file."""
    latest = ""
    for row in read_rows(_source_path(input_dir, "details", year, listing)):
        ym = row["BEGIN_YEARMONTH"].strip()
        if len(ym) == 6 and ym > latest:
            latest = ym
    if not latest:
        return ""
    return f"{latest[:4]}-{latest[4:]}-01"


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
