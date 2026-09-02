"""Pure download and cleaning helpers for us_dot_bts_ontime.

No Prefect imports live here: the one-shot onboarding bootstrap under
``models/us_dot_bts_ontime/code/`` imports these same functions, so the
transform exists in exactly one place.

The source offers two acquisition routes for the same 109 fields:

* **PREZIP** — a static monthly archive. Fast, but only covers 1987-10..1989-12
  and 2000-01..present. The 1990s are simply not published there.
* **The TranStats form** — an ASP.NET WebForms page that generates any
  year-month on demand. Slower (the server builds the extract per request) but
  complete. It returns the *internal* column names (``FL_DATE``,
  ``OP_UNIQUE_CARRIER``, ...) rather than the published ones, and dates as
  ``M/D/YYYY 12:00:00 AM``. Both quirks are normalised in :func:`read_month_csv`.

The two routes emit the same 109 fields in the same order, so the rename is
positional and needs no per-name mapping.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import requests

from pipelines.datasets.us_dot_bts_ontime.constants import constants

GT = ">"
_TIMEOUT = (30, 1800)


def month_iter(
    start: tuple[int, int] = (
        constants.FIRST_YEAR.value,
        constants.FIRST_MONTH.value,
    ),
    end: tuple[int, int] | None = None,
) -> list[tuple[int, int]]:
    """Every (year, month) from `start` through `end`, inclusive."""
    if end is None:
        raise ValueError("end is required")
    out = []
    y, m = start
    while (y, m) <= end:
        out.append((y, m))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def uses_prezip(year: int) -> bool:
    """Whether this year is published as a prezipped monthly archive."""
    return year not in constants.PREZIP_GAP_YEARS.value


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": constants.USER_AGENT.value})
    return s


# --------------------------------------------------------------------------
# download
# --------------------------------------------------------------------------
def download_month_prezip(
    year: int, month: int, dest: Path, session=None
) -> Path:
    """Fetch one prezipped monthly archive. The month is *not* zero-padded."""
    url = constants.PREZIP_URL.value.format(year=year, month=month)
    s = session or _session()
    r = s.get(url, timeout=_TIMEOUT, stream=True)
    r.raise_for_status()
    if r.headers.get("content-type", "").startswith("text/html"):
        raise RuntimeError(
            f"{year}-{month:02d}: PREZIP returned HTML, not a zip ({url})"
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    with open(tmp, "wb") as fh:
        for chunk in r.iter_content(1 << 20):
            fh.write(chunk)
    tmp.replace(dest)
    return dest


def download_month_form(
    year: int, month: int, dest: Path, session=None
) -> Path:
    """Generate one month through the TranStats download form.

    Three details are load-bearing and were each established by failing without
    them:

    1. The ``chkAllVars`` postback must run **first**. It is what ticks the 109
       field checkboxes server-side; posting them straight away is ignored.
    2. ``chkDownloadZip`` must be **left off**. With it on, the form merely
       redirects to the PREZIP URL — which 404s for exactly the years this
       function exists to fetch.
    3. ``cboPeriod`` takes the month *number*, not its name.
    """
    s = session or _session()
    url = constants.FORM_URL.value
    s.headers.update(
        {"Referer": url, "Origin": "https://www.transtats.bts.gov"}
    )

    def _hidden(html: str, name: str) -> str:
        m = re.search(rf'id="{name}"[^{GT}]*value="([^"]*)"', html)
        return m.group(1) if m else ""

    def _state(html: str) -> dict:
        return {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": _hidden(html, "__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": _hidden(html, "__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": _hidden(html, "__EVENTVALIDATION"),
        }

    picked = {
        "cboGeography": "All",
        "cboYear": str(year),
        "cboPeriod": str(month),
    }

    html = s.get(url, timeout=_TIMEOUT).text
    step1 = (
        _state(html)
        | picked
        | {"__EVENTTARGET": "chkAllVars", "chkAllVars": "on"}
    )
    html = s.post(url, data=step1, timeout=_TIMEOUT).text

    fields = [
        n
        for _, n in re.findall(
            r'<input id="([^"]+)" type="checkbox" name="([^"]+)"', html
        )
        if not n.startswith("chk")
    ]
    if len(fields) != 109:
        raise RuntimeError(
            f"{year}-{month:02d}: expected 109 field checkboxes, got {len(fields)}"
        )

    step2 = (
        _state(html) | picked | {"chkAllVars": "on", "btnDownload": "Download"}
    )
    step2.update({n: "on" for n in fields})

    r = s.post(url, data=step2, timeout=_TIMEOUT, stream=True)
    r.raise_for_status()
    if "zip" not in r.headers.get("content-type", ""):
        raise RuntimeError(
            f"{year}-{month:02d}: form returned {r.headers.get('content-type')}, not a zip"
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    with open(tmp, "wb") as fh:
        for chunk in r.iter_content(1 << 20):
            fh.write(chunk)
    tmp.replace(dest)
    return dest


def download_month(year: int, month: int, dest: Path, session=None) -> Path:
    """Fetch one month by whichever route publishes it."""
    if uses_prezip(year):
        return download_month_prezip(year, month, dest, session=session)
    return download_month_form(year, month, dest, session=session)


def download_lookups(dest_dir: Path, session=None) -> dict[str, Path]:
    """Fetch every BTS lookup table as `Code,Description` CSV."""
    s = session or _session()
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = {}
    for name, key in constants.LOOKUPS.value.items():
        r = s.get(constants.LOOKUP_URL.value.format(key=key), timeout=_TIMEOUT)
        r.raise_for_status()
        p = dest_dir / f"{name}.csv"
        p.write_bytes(r.content)
        out[name] = p
    return out


def open_month_zip(path: Path) -> bytes:
    """Return the single data CSV inside a monthly archive, whatever its name."""
    with zipfile.ZipFile(path) as z:
        members = [m for m in z.namelist() if m.lower().endswith(".csv")]
        if len(members) != 1:
            raise RuntimeError(f"{path.name}: expected 1 csv, found {members}")
        return z.read(members[0])


def read_readme(path: Path) -> bytes | None:
    """Return the readme.html bundled with a prezipped archive, if present."""
    with zipfile.ZipFile(path) as z:
        for m in z.namelist():
            if m.lower().endswith("readme.html"):
                return z.read(m)
    return None


# --------------------------------------------------------------------------
# architecture
# --------------------------------------------------------------------------
import csv as _csv  # noqa: E402

import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.compute as pc  # noqa: E402
import pyarrow.csv as pacsv  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

PA = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
    "TIME": pa.time64("us"),
    "DATETIME": pa.timestamp("us"),
}

# STRING columns published as ``0.00``/``1.00`` floats. They are booleans, so the
# dictionary keys are ``0``/``1`` and the trailing ``.00`` has to come off or the
# dicionario join silently matches nothing.
FLAG_COLUMNS = (
    "cancelled",
    "diverted",
    "departure_delay_15min",
    "arrival_delay_15min",
    "diverted_reached_destination",
)

# HHMM source column -> derived TIME column.
DERIVED_TIMES = {
    "scheduled_departure_time": "scheduled_departure_time_local",
    "departure_time": "departure_time_local",
    "scheduled_arrival_time": "scheduled_arrival_time_local",
    "arrival_time": "arrival_time_local",
}


def read_arch(table: str) -> list[dict]:
    """The architecture CSV for one table — the source of truth for name/order/type."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"{table}.csv"
    with open(path, encoding="utf-8") as fh:
        return list(_csv.DictReader(fh))


def _source_columns() -> list[str]:
    """The 109 published columns, in order, excluding the five derived ones."""
    return [
        a["name"]
        for a in read_arch("flight")
        if "+" not in a["original_name"] and not a["name"].endswith("_local")
    ]


# --------------------------------------------------------------------------
# clean
# --------------------------------------------------------------------------
def _hhmm_to_micros(arr: pa.Array) -> pa.Array:
    """HHMM clock label -> microseconds since midnight, as a TIME array.

    Done with arrow compute rather than ``pandas.to_datetime`` because the latter
    materialises 600k Python ``time`` objects per column per month, which turns a
    sub-second operation into minutes across 465 months.

    ``2400`` means midnight at the end of the day and is normalised to
    ``00:00:00``; the calendar day is not advanced, because the source carries no
    timezone and the arrival day is not knowable from this field alone.
    """
    padded = pc.utf8_lpad(pc.utf8_trim_whitespace(arr), 4, "0")  # pyrefly: ignore
    valid = pc.match_substring_regex(padded, r"^\d{4}$")  # pyrefly: ignore
    padded = pc.if_else(valid, padded, pa.nulls(len(arr), pa.string()))  # pyrefly: ignore
    hh = pc.cast(pc.utf8_slice_codeunits(padded, 0, 2), pa.int64())  # pyrefly: ignore
    mm = pc.cast(pc.utf8_slice_codeunits(padded, 2, 4), pa.int64())  # pyrefly: ignore
    micros = pc.multiply(  # pyrefly: ignore
        pc.add(pc.multiply(hh, 3600), pc.multiply(mm, 60)),  # pyrefly: ignore
        1_000_000,  # pyrefly: ignore
    )
    # 2400 -> 00:00; anything else out of range (bad minutes) becomes null.
    micros = pc.if_else(  # pyrefly: ignore
        pc.equal(micros, 24 * 3600 * 1_000_000),  # pyrefly: ignore
        pa.scalar(0, pa.int64()),
        micros,
    )
    in_range = pc.and_(  # pyrefly: ignore
        pc.greater_equal(micros, 0),  # pyrefly: ignore
        pc.less(micros, 24 * 3600 * 1_000_000),  # pyrefly: ignore
    )
    micros = pc.if_else(in_range, micros, pa.nulls(len(arr), pa.int64()))  # pyrefly: ignore
    return micros.cast(pa.time64("us"))


def _parse_flight_date(arr: pa.Array) -> pa.Array:
    """Parse the flight date from whichever route produced the file.

    PREZIP publishes ISO ``YYYY-MM-DD``; the form publishes
    ``M/D/YYYY 12:00:00 AM``. The format is detected from the first non-null
    value rather than attempted blindly, so a genuinely malformed date still
    surfaces as null instead of being silently reinterpreted.
    """
    trimmed = pc.utf8_trim_whitespace(arr)  # pyrefly: ignore
    sample = None
    for v in trimmed:
        if v.is_valid:
            sample = v.as_py()
            break
    if sample is None:
        return pa.nulls(len(arr), pa.date32())
    if "/" in sample:
        day = pc.utf8_split_whitespace(trimmed)  # pyrefly: ignore
        day = pc.list_element(day, 0)  # pyrefly: ignore
        return pc.strptime(  # pyrefly: ignore
            day, format="%m/%d/%Y", unit="s", error_is_null=True
        ).cast(pa.date32())
    return pc.strptime(  # pyrefly: ignore
        trimmed, format="%Y-%m-%d", unit="s", error_is_null=True
    ).cast(pa.date32())


def clean_month(raw: bytes) -> pa.Table:
    """Turn one monthly CSV into an architecture-shaped, typed arrow table.

    Accepts either acquisition route. The two differ in three ways and in no
    others: the prezipped file uses the published column names and carries a
    trailing empty column from a stray delimiter, the form file uses the internal
    names, and the form renders the flight date as ``M/D/YYYY 12:00:00 AM``. The
    109 fields themselves are the same, in the same order, so the rename is
    positional and no per-name mapping is needed.

    Every column is read as STRING so that nothing is silently reinterpreted --
    type inference would turn the HHMM label ``0937`` into the number ``937`` --
    and each is then converted to its architecture type explicitly.
    """
    names = _source_columns()
    header = raw[: raw.index(b"\n")].decode("latin-1")
    raw_names = next(_csv.reader([header]))
    all_string = {n: pa.string() for n in raw_names if n.strip()}

    tbl = pacsv.read_csv(
        io.BytesIO(raw),
        convert_options=pacsv.ConvertOptions(
            column_types=all_string, strings_can_be_null=True, null_values=[""]
        ),
        read_options=pacsv.ReadOptions(encoding="latin-1"),
    )
    # Drop the trailing empty column the prezipped files carry from a stray delimiter.
    tbl = tbl.select([n for n in tbl.column_names if n.strip()])
    if tbl.num_columns != len(names):
        raise RuntimeError(
            f"expected {len(names)} source columns, got {tbl.num_columns}"
        )
    tbl = tbl.rename_columns(names)

    cols = {n: tbl.column(n).combine_chunks() for n in names}
    cols["flight_date"] = _parse_flight_date(cols["flight_date"])

    for c in FLAG_COLUMNS:
        cols[c] = pc.replace_substring_regex(cols[c], r"\.0+$", "")  # pyrefly: ignore

    for src, dst in DERIVED_TIMES.items():
        cols[dst] = _hhmm_to_micros(cols[src])

    seconds = pc.cast(
        pc.cast(cols["scheduled_departure_time_local"], pa.int64()), pa.int64()
    )
    base = pc.cast(cols["flight_date"], pa.timestamp("us"))
    cols["scheduled_departure_datetime_local"] = pc.if_else(  # pyrefly: ignore
        pc.is_valid(seconds),  # pyrefly: ignore
        pc.cast(
            pc.add(pc.cast(base, pa.int64()), seconds),  # pyrefly: ignore
            pa.timestamp("us"),  # pyrefly: ignore
        ),
        pa.nulls(len(base), pa.timestamp("us")),
    )

    arch = read_arch("flight")
    out = []
    for a in arch:
        t = PA[a["bigquery_type"]]
        arr = cols[a["name"]]
        if arr.type != t:
            arr = (
                pc.cast(arr, pa.float64())
                if t == pa.int64() and arr.type == pa.string()
                else arr
            )
            arr = pc.cast(arr, t)
        out.append(arr)
    return pa.Table.from_arrays(out, names=[a["name"] for a in arch])


def write_month_parquet(
    tbl: pa.Table, output_dir: Path, year: int, month: int
) -> Path:
    """Write one month as all-STRING Snappy Parquet under ``flight/year=YYYY/``.

    Staging is all-STRING by house convention: ``gcs.dump_header`` stringifies the
    one-row header BigQuery infers the staging schema from, so typed parquet is
    rejected on read. Values pass through the architecture's real types first --
    which is what :func:`clean_month` returns -- so ``year`` serializes as
    ``"1987"`` rather than ``"1987.0"`` and a NULL stays NULL rather than becoming
    the literal ``"nan"`` that ``astype(str)`` would produce and ``safe_cast``
    would not undo.

    One file per month rather than per year keeps peak memory at a single month.
    """
    arch = read_arch("flight")
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    pdir = output_dir / "flight" / f"year={year}"
    pdir.mkdir(parents=True, exist_ok=True)
    out = pdir / f"data_{year}_{month:02d}.parquet"
    pq.write_table(tbl.cast(strings), out, compression="snappy")
    return out


# --------------------------------------------------------------------------
# reference tables
# --------------------------------------------------------------------------
_AIRPORT_DESC = re.compile(r"^(?P<loc>.+?): (?P<name>.+)$")


def build_airport(lookup_dir: Path) -> pd.DataFrame:
    """Parse ``L_AIRPORT_ID`` into the ``airport`` reference table.

    The description packs city, region and airport name as ``City, XX: Name``,
    where ``XX`` is a two-letter US state or territory code for domestic airports
    and a country name for foreign ones. It is split into columns and also kept
    verbatim so the parse can be audited.

    ``L_AIRPORT`` (the airport *code* lookup) is deliberately not joined in here.
    Its only shared key would be the description text, and 74 descriptions are
    duplicated across the two lookups, so the join is not sound. Airport codes
    are covered by the dicionario instead.
    """
    with open(lookup_dir / "L_AIRPORT_ID.csv", encoding="latin-1") as fh:
        rows = list(_csv.DictReader(fh))
    out = []
    for r in rows:
        desc = (r["Description"] or "").strip()
        m = _AIRPORT_DESC.match(desc)
        city = region = name = None
        if m:
            loc, name = m.group("loc"), m.group("name")
            city, region = loc.rsplit(", ", 1) if ", " in loc else (loc, None)
        is_us = bool(region) and len(region) == 2 and region.isupper()
        out.append(
            {
                "airport_id": r["Code"],
                "city_name": city,
                "state_abbreviation": region if is_us else None,
                "country_name": None if is_us else region,
                "airport_name": name,
                "airport_description": desc,
            }
        )
    return pd.DataFrame(out, columns=[a["name"] for a in read_arch("airport")])


# Dictionary-covered flight columns and the BTS lookup that decodes each.
DICIONARIO_SOURCES = {
    "day_of_week": "L_WEEKDAYS",
    "reporting_carrier": "L_UNIQUE_CARRIERS",
    "reporting_carrier_airline_id": "L_AIRLINE_ID",
    "origin": "L_AIRPORT",
    "destination": "L_AIRPORT",
    "origin_state_abbreviation": "L_STATE_ABR_AVIATION",
    "destination_state_abbreviation": "L_STATE_ABR_AVIATION",
    "origin_state_fips": "L_STATE_FIPS",
    "destination_state_fips": "L_STATE_FIPS",
    "origin_world_area_code": "L_WORLD_AREA_CODES",
    "destination_world_area_code": "L_WORLD_AREA_CODES",
    "departure_delay_group": "L_ONTIME_DELAY_GROUPS",
    "arrival_delay_group": "L_ONTIME_DELAY_GROUPS",
    "scheduled_departure_time_block": "L_DEPARRBLK",
    "scheduled_arrival_time_block": "L_DEPARRBLK",
    "cancellation_code": "L_CANCELLATION",
    "distance_group": "L_DISTANCE_GROUP_250",
    "diversion_1_airport": "L_AIRPORT",
    "diversion_2_airport": "L_AIRPORT",
    "diversion_3_airport": "L_AIRPORT",
    "diversion_4_airport": "L_AIRPORT",
    "diversion_5_airport": "L_AIRPORT",
}

# Booleans the source publishes as 0/1 with no BTS lookup of their own.
_YESNO = {"0": "No", "1": "Yes"}
DICIONARIO_INLINE = {
    "cancelled": {
        "0": "Flight was not cancelled",
        "1": "Flight was cancelled",
    },
    "diverted": {"0": "Flight was not diverted", "1": "Flight was diverted"},
    "departure_delay_15min": {
        "0": "Departure delayed less than 15 minutes",
        "1": "Departure delayed 15 minutes or more",
    },
    "arrival_delay_15min": {
        "0": "Arrival delayed less than 15 minutes",
        "1": "Arrival delayed 15 minutes or more",
    },
    "diverted_reached_destination": {
        "0": "Diverted flight did not reach its scheduled destination",
        "1": "Diverted flight reached its scheduled destination",
    },
}


def build_dicionario(lookup_dir: Path) -> pd.DataFrame:
    """Assemble the dicionario from the BTS lookup tables.

    Airport codes and carrier codes land here rather than in a directory because
    no US airport or airline directory exists yet; the note is recorded on the
    columns themselves.
    """
    cache: dict[str, list[tuple[str, str]]] = {}

    def load(name: str) -> list[tuple[str, str]]:
        if name not in cache:
            with open(lookup_dir / f"{name}.csv", encoding="latin-1") as fh:
                rows = list(_csv.DictReader(fh))
            cache[name] = [
                (r["Code"], (r["Description"] or "").strip()) for r in rows
            ]
        return cache[name]

    out = []
    for column, lookup in DICIONARIO_SOURCES.items():
        for key, value in load(lookup):
            out.append(
                {
                    "id_tabela": "flight",
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": "",
                    "valor": value,
                }
            )
    for column, mapping in DICIONARIO_INLINE.items():
        for key, value in mapping.items():
            out.append(
                {
                    "id_tabela": "flight",
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": "",
                    "valor": value,
                }
            )
    for key, value in load("L_STATE_ABR_AVIATION"):
        out.append(
            {
                "id_tabela": "airport",
                "nome_coluna": "state_abbreviation",
                "chave": key,
                "cobertura_temporal": "",
                "valor": value,
            }
        )
    return pd.DataFrame(
        out, columns=[a["name"] for a in read_arch("dicionario")]
    )


def write_reference_parquet(
    df: pd.DataFrame, table: str, output_dir: Path
) -> Path:
    """Write an unpartitioned reference table as all-STRING Snappy Parquet."""
    arch = read_arch(table)
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    tdir = output_dir / table
    tdir.mkdir(parents=True, exist_ok=True)
    at = pa.Table.from_pandas(
        df[[a["name"] for a in arch]], schema=strings, preserve_index=False
    )
    out = tdir / "data.parquet"
    pq.write_table(at, out, compression="snappy")
    return out


# --------------------------------------------------------------------------
# recurring pipeline helpers
# --------------------------------------------------------------------------
def month_exists_prezip(year: int, month: int, session=None) -> bool:
    """Whether the prezipped archive for this month is published yet."""
    s = session or _session()
    url = constants.PREZIP_URL.value.format(year=year, month=month)
    r = s.head(url, timeout=(30, 120), allow_redirects=True)
    return r.status_code == 200 and "zip" in r.headers.get("content-type", "")


def latest_available_month(
    today: tuple[int, int], back: int = 8, session=None
):
    """The most recent month BTS has published, searching backwards from `today`.

    BTS runs roughly two months behind, so this walks back from the current month
    until a prezipped archive answers. Only PREZIP is probed: every month the
    recurring pipeline will ever want is prezipped, and the on-demand form route
    exists solely to backfill the 1990s during onboarding.
    """
    s = session or _session()
    y, m = today
    for _ in range(back):
        if month_exists_prezip(y, m, session=s):
            return y, m
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    raise RuntimeError(
        f"no published month found in the {back} months before {today}"
    )


def write_header_stub(output_dir: Path) -> Path | None:
    """Prepend a 0-row parquet to the earliest partition of ``flight``.

    The GitHub table-approve action's ``save_header_files`` runs ``pd.read_parquet``
    on the *first* parquet file of a staging table; on a large file that OOMs CI and
    prod materialization never runs at all. ``00_header.parquet`` sorts ahead of
    ``data_YYYY_MM.parquet``, so it is read instead.

    It goes *inside* the earliest ``year=`` partition rather than at the table root,
    so hive partition discovery still sees every file under a partition directory.
    """
    partitions = sorted((output_dir / "flight").glob("year=*"))
    if not partitions:
        return None
    first = partitions[0]
    data = sorted(first.glob("data_*.parquet"))
    if not data:
        return None
    stub = first / "00_header.parquet"
    schema = pq.ParquetFile(data[0]).schema_arrow
    pq.write_table(
        pa.Table.from_pylist([], schema=schema), stub, compression="snappy"
    )
    return stub
