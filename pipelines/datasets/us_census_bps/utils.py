"""Download and cleaning transform for us_census_bps.

Shared by the recurring pipeline and the one-shot bootstrap in
``models/us_census_bps/code/``. Pure functions, no Prefect, so they are
importable and unit-testable.

The Census Building Permits Survey publishes comma-delimited ASCII files whose
column layout changed six times between 1980 and 2026 — fields were added,
moved and redefined. Parsing is therefore driven by the two header rows each
file carries, never by the file name or the year, so a further change is a loud
failure rather than silently shifted data.
"""

from __future__ import annotations

import concurrent.futures as cf
import csv
import logging
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.us_census_bps.constants import constants

log = logging.getLogger("us_census_bps")

PA = {"STRING": pa.string(), "INT64": pa.int64(), "FLOAT64": pa.float64()}
_ARCH = constants.ARCHITECTURE_DIR.value
STRUCTURE_TYPES = constants.STRUCTURE_TYPES.value

# ── header resolution ───────────────────────────────────────────────────────

# (header row 1, header row 2) -> canonical field name.
_PAIR: dict[tuple[str, str], str] = {
    ("survey", "date"): "survey_date",
    ("fips", "state"): "state",
    ("fips", "county"): "county",
    ("state", "code"): "state",
    ("county", "code"): "county",
    ("place", "code"): "census_place",
    ("census place", "code"): "census_place",
    ("fips place", "code"): "fips_place",
    ("fips mcd", "code"): "fips_mcd",
    ("csa", "code"): "csa",
    ("cbsa", "code"): "cbsa",
    ("msa/", "cmsa"): "msa_cmsa",
    ("pmsa", "code"): "pmsa",
    ("footnote", "code"): "footnote",
    ("central", "city"): "central_city",
    ("ccity", "code"): "central_city",
    ("zip", "code"): "zip",
    ("region", "code"): "region",
    ("division", "code"): "division",
    ("source", "code"): "source",
    ("6-digit", "id"): "office",
    ("number of", "months rep"): "months_reported",
}

# Header row 1 alone, for the layouts where row 2 is blank or shifted.
_SINGLE: dict[str, str] = {
    "survey": "survey_date",
    "state": "state",
    "county": "county",
    "place": "census_place",
    "census place": "census_place",
    "fips place": "fips_place",
    "fips mcd": "fips_mcd",
    "pop": "pop",
    "csa": "csa",
    "cbsa": "cbsa",
    "msa/": "msa_cmsa",
    "pmsa": "pmsa",
    "footnote": "footnote",
    "central": "central_city",
    "ccity": "central_city",
    "zip": "zip",
    "region": "region",
    "division": "division",
    "source": "source",
    "6-digit": "office",
    "number of": "months_reported",
    "moncov": "moncov",
    "hheader": "hheader",
}


def _norm(value: str) -> str:
    """Lowercase, strip and collapse whitespace in a header cell."""
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip().lower()


def resolve_fields(head1: list[str], head2: list[str], n_id: int) -> list[str]:
    """Map the identifier columns of a BPS file to canonical field names.

    The last identifier column is the geography name in every published
    layout, so it is assigned directly rather than looked up.

    Args:
        head1: First header row, split on commas.
        head2: Second header row, split on commas.
        n_id: Number of identifier columns, i.e. the index of the first
            measure column.

    Returns:
        ``n_id`` field names; ``""`` marks a column the file leaves blank.

    Raises:
        ValueError: If a labelled column cannot be resolved, which means the
            Census Bureau changed the layout again.
    """
    fields: list[str] = []
    for i in range(n_id):
        if i == n_id - 1:
            fields.append("name")
            continue
        a = _norm(head1[i]) if i < len(head1) else ""
        b = _norm(head2[i]) if i < len(head2) else ""
        if not a and not b:
            fields.append("")
            continue
        name = _PAIR.get((a, b)) or _SINGLE.get(a) or _SINGLE.get(b)
        if name is None:
            raise ValueError(
                f"unknown BPS header column {i}: row1={a!r} row2={b!r}"
            )
        fields.append(name)
    return fields


# ── file identity ───────────────────────────────────────────────────────────

_FILE_RE = re.compile(
    r"^(?P<prefix>st|co|cbsa|ma|ne|mw|so|we)"
    r"(?:(?P<yy>\d{2})(?P<mm>\d{2})c|(?P<yyyy>\d{4})a)\.txt$",
    re.IGNORECASE,
)

_PREFIX_LEVEL = {
    "st": "state",
    "co": "county",
    "cbsa": "cbsa",
    "ma": "metro",
    "ne": "place",
    "mw": "place",
    "so": "place",
    "we": "place",
}


def parse_filename(name: str) -> tuple[str, str, int, int | None]:
    """Read the geography level, periodicity and period from a file name.

    Args:
        name: Bare file name, e.g. ``"mw2507c.txt"``.

    Returns:
        ``(level, periodicity, year, month)``; ``month`` is None for annual
        files.

    Raises:
        ValueError: If the name does not match a published BPS file name.
    """
    m = _FILE_RE.match(name)
    if not m:
        raise ValueError(f"unrecognised BPS file name: {name}")
    level = _PREFIX_LEVEL[m.group("prefix").lower()]
    if m.group("yyyy"):
        return level, "annual", int(m.group("yyyy")), None
    yy = int(m.group("yy"))
    year = 1900 + yy if yy >= 80 else 2000 + yy
    return level, "monthly", year, int(m.group("mm"))


def check_survey_date(raw: str, year: int, month: int | None) -> None:
    """Assert a row's survey-date field agrees with the file name's period.

    The Census Bureau writes this field five different ways (``YYMM``,
    ``YYYYMM``, ``YYYY``, ``YYYY99`` and ``YY99``), so it is validated against
    the file name rather than parsed as the authority.

    Args:
        raw: Survey date exactly as published.
        year: Year taken from the file name.
        month: Month taken from the file name, or None for annual files.

    Raises:
        ValueError: If the field cannot be reconciled with the file name.
    """
    digits = raw.strip()
    if not digits.isdigit():
        raise ValueError(f"non-numeric survey date {raw!r}")
    if month is None:
        ok = digits in {
            str(year),
            f"{year}99",
            f"{year % 100:02d}99",
            f"{year % 100:02d}{99}",
        }
    else:
        ok = digits in {f"{year}{month:02d}", f"{year % 100:02d}{month:02d}"}
    if not ok:
        raise ValueError(
            f"survey date {raw!r} does not match file period "
            f"{year}-{month if month else 'annual'}"
        )


# ── value cleaning ──────────────────────────────────────────────────────────

# Values the source uses to mean "not applicable" for a given code column.
_SENTINEL = {
    "csa": {"999", "0999"},
    "cbsa": {"99999", "0"},
    "pmsa": {"9999"},
    "msa_cmsa": {"9999"},
    "region": {"0"},
    "division": {"0"},
}


def _clean_code(field: str, raw: str) -> str | None:
    """Strip a code field and map the source's not-applicable sentinels to None."""
    value = raw.strip().lstrip("-").strip()
    if not value:
        return None
    if value in _SENTINEL.get(field, frozenset()):
        return None
    return value


def _clean_int(raw: str) -> int | None:
    """Parse an integer measure, returning None when the field is blank."""
    value = raw.strip().replace(",", "")
    if not value or value in {"-", "."}:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


_NAME_DOTS = re.compile(r"[.\s]+$")


def _clean_name(raw: str) -> str | None:
    """Strip the dot leaders the pre-1996 files pad geography names with."""
    value = _NAME_DOTS.sub("", raw.strip())
    return value or None


def _pad(value: str | None, width: int) -> str | None:
    """Zero-pad a numeric code, leaving a non-numeric code untouched."""
    if value is None:
        return None
    return value.zfill(width) if value.isdigit() else value


# ── parsing ─────────────────────────────────────────────────────────────────


def read_file(path: Path) -> tuple[list[str], list[list[str]]]:
    """Read one BPS ASCII file into its identifier field names and data rows.

    Args:
        path: Path to the downloaded ``.txt`` file.

    Returns:
        ``(fields, rows)`` where ``fields`` names the identifier columns and
        ``rows`` holds every non-blank data row, already split on commas.

    Raises:
        ValueError: If the header cannot be located or the measure block is
            neither 12 nor 24 columns wide.
    """
    # 977 of the pre-2000 files end with a DOS end-of-file byte. In two of
    # them it lands in a full-width row of empty fields, which would otherwise
    # be parsed as data.
    text = path.read_bytes().decode("latin-1").replace("\x1a", "")
    lines = text.splitlines()
    if len(lines) < 3:
        raise ValueError(f"{path.name}: fewer than 3 lines")
    head1 = next(csv.reader([lines[0]]))
    head2 = next(csv.reader([lines[1]]))
    rows = [
        r for r in csv.reader(lines[2:]) if r and any(c.strip() for c in r)
    ]
    if not rows:
        return [], []
    n_id_candidates = [i for i, c in enumerate(head2) if _norm(c) == "bldgs"]
    if not n_id_candidates:
        raise ValueError(f"{path.name}: no 'Bldgs' column in header row 2")
    n_id = n_id_candidates[0]
    width = len(rows[0])
    n_measure = width - n_id
    if n_measure not in (12, 24):
        raise ValueError(
            f"{path.name}: {n_measure} measure columns (expected 12 or 24)"
        )
    return resolve_fields(head1, head2, n_id), rows


# Fields that describe a geography rather than identify it. They are excluded
# from the duplicate key because the source sometimes contradicts itself on
# them: St. Clair County, Illinois is listed twice in eight 2000 county files,
# once with its correct Midwest region code and once with a wrong one, and the
# figures on both rows are identical.
_NOT_IDENTITY = frozenset(
    {
        "survey_date",
        "name",
        "region",
        "division",
        "source",
        "months_reported",
        "pop",
        "zip",
        "footnote",
        "central_city",
        "moncov",
        "hheader",
        "csa",
        "census_place",
        "fips_place",
        "fips_mcd",
    }
)


def melt_rows(
    fields: list[str],
    rows: list[list[str]],
    *,
    level: str,
    year: int,
    month: int | None,
) -> Iterator[dict]:
    """Turn one file's rows into long records, one per structure type.

    Valuation is normalised to US dollars: the state and metropolitan files
    publish it in thousands, the county and place files in dollars.

    Args:
        fields: Identifier field names from :func:`read_file`.
        rows: Data rows from :func:`read_file`.
        level: Geography level of the file.
        year: Survey year from the file name.
        month: Survey month, or None for annual files.

    Yields:
        One record per source row and structure type.

    Raises:
        ValueError: If a row's survey date contradicts the file name.
    """
    n_id = len(fields)
    scale = 1000 if level in constants.THOUSANDS_LEVELS.value else 1
    # Some county files list the same county twice, usually under two
    # spellings of its name ("Anchorage Borough" and "Anchorage
    # Municipality"), with identical figures. Keeping both would double the
    # county. The measures are compared before the repeat is discarded, so a
    # genuine split would raise instead of being silently collapsed.
    seen: dict[tuple[str, ...], tuple[str, ...]] = {}
    identity = [f for f in fields if f and f not in _NOT_IDENTITY]
    for row in rows:
        if len(row) < n_id + 12:
            continue
        ident = {f: row[i] for i, f in enumerate(fields) if f}
        check_survey_date(ident.get("survey_date", ""), year, month)
        measures = row[n_id:]
        key = tuple(ident[f].strip() for f in identity)
        stripped = tuple(v.strip() for v in measures)
        if key in seen:
            if seen[key] != stripped:
                raise ValueError(
                    f"repeated key {key} with different figures in "
                    f"{year}-{month}; the source split a record"
                )
            continue
        seen[key] = stripped
        has_reported = len(measures) >= 24
        base: dict[str, object] = {
            f: _clean_code(f, v)
            for f, v in ident.items()
            if f not in {"survey_date", "name"}
        }
        base["name"] = _clean_name(ident.get("name", ""))
        base["year"] = year
        base["month"] = month
        for k, code in enumerate(STRUCTURE_TYPES):
            rec: dict[str, object] = dict(base)
            rec["structure_type"] = code
            rec["buildings"] = _clean_int(measures[3 * k])
            rec["units"] = _clean_int(measures[3 * k + 1])
            value = _clean_int(measures[3 * k + 2])
            rec["valuation"] = None if value is None else value * scale
            if has_reported:
                rec["buildings_reported"] = _clean_int(measures[12 + 3 * k])
                rec["units_reported"] = _clean_int(measures[12 + 3 * k + 1])
                value_rep = _clean_int(measures[12 + 3 * k + 2])
                rec["valuation_reported"] = (
                    None if value_rep is None else value_rep * scale
                )
            yield rec


def target_table(level: str, periodicity: str, fields: list[str]) -> str:
    """Choose the destination table for a parsed file.

    The metropolitan files switched from the MSA/PMSA code system to CBSA
    codes mid-series — annual 2003 and monthly January 2004 — so the split is
    decided by which code columns the header declares, never by the year.

    Args:
        level: Geography level from the file name.
        periodicity: ``"monthly"`` or ``"annual"``.
        fields: Identifier field names from :func:`read_file`.

    Returns:
        The destination table slug.
    """
    if level in {"metro", "cbsa"}:
        geo = "cbsa" if "cbsa" in fields else "msa"
    else:
        geo = level
    return f"permit_{geo}_{periodicity}"


# ── field mapping to the architecture columns ───────────────────────────────

_PLACE_NULL_CODES = {"00000", "99990", "99999", "0", "000"}

# Until 2021 the survey identified the territories by their old Census codes
# rather than by FIPS, so Puerto Rico appears as 43 through 2021 and as 72
# from 2022. Normalising state_id keeps a territory on one identifier across
# the whole panel; geography_id still carries the code exactly as published.
_LEGACY_STATE_CODES = {"43": "72", "52": "78"}


def _geography_level(code: str | None) -> str | None:
    """Classify a state-file geography code as nation, region, division or state."""
    if not code:
        return None
    if code == "US":
        return "nation"
    if code.startswith("R"):
        return "region"
    if code.startswith("D"):
        return "division"
    return "state"


def _compose(prefix: str | None, suffix: str | None, width: int) -> str | None:
    """Join a state code to a sub-state code, or return None if either is absent."""
    if not prefix or not suffix or suffix in _PLACE_NULL_CODES:
        return None
    return f"{prefix}{suffix.zfill(width)}"


def to_columns(rec: dict, table: str) -> dict:
    """Rename one long record's fields to the architecture column names.

    Args:
        rec: A record from :func:`melt_rows`.
        table: Destination table slug.

    Returns:
        The record keyed by architecture column name.
    """
    out: dict = {
        "year": rec["year"],
        "structure_type": rec["structure_type"],
        "buildings": rec.get("buildings"),
        "units": rec.get("units"),
        "valuation": rec.get("valuation"),
    }
    if table.endswith("_monthly"):
        out["month"] = rec["month"]
    if "buildings_reported" in rec:
        out["buildings_reported"] = rec.get("buildings_reported")
        out["units_reported"] = rec.get("units_reported")
        out["valuation_reported"] = rec.get("valuation_reported")

    state = _pad(rec.get("state"), 2)
    state = _LEGACY_STATE_CODES.get(state or "", state)
    region = rec.get("region")
    division = rec.get("division")

    if "_state_" in table:
        code = rec.get("state")
        out["geography_level"] = _geography_level(code)
        out["geography_id"] = _pad(code, 2)
        out["state_id"] = state if code and code.isdigit() else None
        out["region_id"] = region
        out["division_id"] = division
        out["geography_name"] = rec.get("name")
    elif "_county_" in table:
        out["county_id"] = _compose(state, rec.get("county"), 3)
        out["state_id"] = state
        out["region_id"] = region
        out["division_id"] = division
        out["county_name"] = rec.get("name")
    elif "_cbsa_" in table:
        out["cbsa_id"] = _pad(rec.get("cbsa"), 5)
        out["csa_id"] = _pad(rec.get("csa"), 3)
        out["cbsa_type"] = rec.get("hheader")
        out["full_monthly_coverage"] = rec.get("moncov")
        out["cbsa_name"] = rec.get("name")
    elif "_msa_" in table:
        out["msa_cmsa_id"] = _pad(rec.get("msa_cmsa"), 4)
        out["pmsa_id"] = _pad(rec.get("pmsa"), 4)
        out["msa_name"] = rec.get("name")
    else:  # place
        out["state_id"] = state
        out["county_id"] = _compose(state, rec.get("county"), 3)
        out["place_id"] = _compose(state, rec.get("fips_place"), 5)
        out["mcd_id"] = _compose(state, rec.get("fips_mcd"), 5)
        out["cbsa_id"] = _pad(rec.get("cbsa"), 5)
        out["csa_id"] = _pad(rec.get("csa"), 3)
        out["msa_cmsa_id"] = _pad(rec.get("msa_cmsa"), 4)
        out["pmsa_id"] = _pad(rec.get("pmsa"), 4)
        out["permit_office_id"] = _pad(rec.get("office"), 6)
        out["census_place_id"] = _pad(rec.get("census_place"), 4)
        out["region_id"] = region
        out["division_id"] = division
        out["place_name"] = rec.get("name")
        out["central_city"] = rec.get("central_city")
        out["footnote_code"] = rec.get("footnote")
        out["zip_code"] = rec.get("zip")
        out["population"] = _clean_int(rec.get("pop") or "")
        if table.endswith("_monthly"):
            out["source_code"] = rec.get("source")
        else:
            out["months_reported"] = _clean_int(
                rec.get("months_reported") or ""
            )
    return out


def keep_row(row: dict, table: str) -> bool:
    """Reject rows that do not belong in a metropolitan-area table.

    The January and March 1998 metropolitan files append 58 state and
    balance-of-state records coded with the not-applicable metro sentinel
    9999. They are not metropolitan areas and their figures do not match the
    state file, so they are dropped rather than left to corrupt any total
    taken over the table.

    Args:
        row: A record from :func:`to_columns`.
        table: Destination table slug.

    Returns:
        Whether the row should be written.
    """
    if "_msa_" in table:
        return row.get("msa_cmsa_id") is not None
    if "_cbsa_" in table:
        return row.get("cbsa_id") is not None
    return True


# ── schema ──────────────────────────────────────────────────────────────────


def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV — the schema source of truth.

    Args:
        table: Table slug, matching the CSV file name.

    Returns:
        One dict per column, in architecture order.
    """
    with (_ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _schemas(table: str) -> tuple[list[str], pa.Schema, pa.Schema]:
    """Return the column order and the typed and all-string arrow schemas."""
    arch = read_arch(table)
    order = [a["name"] for a in arch]
    typed = pa.schema(
        [pa.field(a["name"], PA[a["bigquery_type"]]) for a in arch]
    )
    strings = pa.schema([pa.field(a["name"], pa.string()) for a in arch])
    return order, typed, strings


def write_parquet(
    records: list[dict], table: str, out_dir: Path, part: int
) -> int:
    """Write one batch of records as all-STRING Snappy Parquet.

    Staging is all-STRING by Data Basis convention: ``gcs.dump_header``
    stringifies the header BigQuery infers the staging schema from, so typed
    parquet is rejected on read. Values pass through the architecture's real
    types first, then cast to string via arrow — never ``astype(str)``, which
    would write a NULL as the literal ``"nan"`` and defeat the dbt
    ``safe_cast``.

    Args:
        records: Records for one table and one year.
        table: Table slug.
        out_dir: Root output directory.
        part: Sequence number for this batch within the year partition.

    Returns:
        Number of rows written.
    """
    if not records:
        return 0
    order, typed, strings = _schemas(table)
    frame = pd.DataFrame.from_records(records)
    for name in order:
        if name not in frame.columns:
            frame[name] = None
    year = int(frame["year"].iloc[0])
    pdir = out_dir / table / f"year={year}"
    pdir.mkdir(parents=True, exist_ok=True)
    at = pa.Table.from_pandas(frame[order], schema=typed, preserve_index=False)
    at = at.cast(strings)
    pq.write_table(at, pdir / f"part_{part:04d}.parquet", compression="snappy")
    return len(records)


# ── dictionary ──────────────────────────────────────────────────────────────

_STRUCTURE_LABELS = {
    "101": "Single-family building (1 housing unit)",
    "103": "Two-family building (2 housing units)",
    "104": "Three- and four-family building (3 to 4 housing units)",
    "105": "Multifamily building (5 or more housing units)",
}
_GEOGRAPHY_LEVEL_LABELS = {
    "nation": "United States total",
    "region": "Census region total",
    "division": "Census division total",
    "state": "State, District of Columbia or territory",
}
_REGION_LABELS = {
    "1": "Northeast",
    "2": "Midwest",
    "3": "South",
    "4": "West",
}
_DIVISION_LABELS = {
    "1": "New England",
    "2": "Middle Atlantic",
    "3": "East North Central",
    "4": "West North Central",
    "5": "South Atlantic",
    "6": "East South Central",
    "7": "West South Central",
    "8": "Mountain",
    "9": "Pacific",
}
_SOURCE_LABELS = {
    "1": "Building Permits C-404 survey form",
    "2": "Received data in electronic format",
    "3": "Received residential data via online reporting",
    "4": "Received residential data from another source, equivalent to "
    "reported data",
    "5": "No report received, imputed data",
    "9": "No report received and no imputed data",
}
_CBSA_TYPE_LABELS = {
    "2": "Metropolitan area that is part of a Combined Statistical Area",
    "4": "Metropolitan area that is not part of a Combined Statistical Area",
    "5": "Micropolitan area",
}
_COVERAGE_LABELS = {
    "C": "Area completely covered by monthly reporting permit-issuing places"
}
_FOOTNOTE_LABELS = {
    "2": "Place carries an explanatory footnote in the source release"
}
_CENTRAL_CITY_LABELS = {"1": "Place is a central city of a metropolitan area"}

# column name -> (label map, temporal coverage) for every coded column.
_DICTIONARY: dict[str, tuple[dict[str, str], str]] = {
    "structure_type": (_STRUCTURE_LABELS, ""),
    "geography_level": (_GEOGRAPHY_LEVEL_LABELS, ""),
    "region_id": (_REGION_LABELS, ""),
    "division_id": (_DIVISION_LABELS, ""),
    "source_code": (_SOURCE_LABELS, ""),
    "cbsa_type": (_CBSA_TYPE_LABELS, "2024(1)2025"),
    "full_monthly_coverage": (_COVERAGE_LABELS, "2003(1)2023"),
    "footnote_code": (_FOOTNOTE_LABELS, "2005(1)2026"),
    "central_city": (_CENTRAL_CITY_LABELS, "2000(1)2026"),
}


def build_dicionario() -> list[dict]:
    """Build the dictionary rows from the columns each table actually carries.

    Derived from the architecture CSVs rather than from a hand-kept list, so a
    column flagged ``covered_by_dictionary`` can never end up without labels.

    Returns:
        Dictionary rows, one per table, column and coded value.

    Raises:
        KeyError: If a column is flagged as dictionary-covered but has no
            label map above.
    """
    rows: list[dict] = []
    for table in constants.TABLES.value:
        if table == "dicionario":
            continue
        for column in read_arch(table):
            if column["covered_by_dictionary"] != "yes":
                continue
            labels, coverage = _DICTIONARY[column["name"]]
            for key, value in labels.items():
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column["name"],
                        "chave": key,
                        "cobertura_temporal": coverage
                        or column["temporal_coverage"],
                        "valor": value,
                    }
                )
    return rows


# ── driver ──────────────────────────────────────────────────────────────────

FLUSH_ROWS = 400_000


def clean_all(
    input_dir: Path, output_dir: Path, *, only: set[str] | None = None
) -> dict[str, int]:
    """Parse every downloaded BPS file into the partitioned Parquet tables.

    Files are grouped by destination table and year so each year partition is
    written in a few flushes, keeping peak memory to one batch rather than one
    table.

    Args:
        input_dir: Directory holding the downloaded ``.txt`` files.
        output_dir: Root directory to write ``<table>/year=<YYYY>/`` under.
        only: Optional set of table slugs to restrict the run to.

    Returns:
        Rows written per table.
    """
    grouped: dict[tuple[str, int], list[Path]] = {}
    for path in sorted(input_dir.rglob("*.txt")):
        level, periodicity, year, _ = parse_filename(path.name)
        with path.open("rb") as fh:
            header = fh.readline().decode("latin-1")
        # Only the metropolitan files need the full header to be routed, and
        # only to tell the MSA era from the CBSA era.
        fields = ["cbsa"] if "CBSA" in header else []
        table = target_table(level, periodicity, fields)
        if only and table not in only:
            continue
        grouped.setdefault((table, year), []).append(path)

    # A rerun must not leave part files behind from a previous run that
    # produced more of them, or the stale rows are read back as real data.
    for table in {t for t, _ in grouped}:
        shutil.rmtree(output_dir / table, ignore_errors=True)

    totals: dict[str, int] = {}
    dropped: dict[str, int] = {}
    for (table, year), paths in sorted(grouped.items()):
        buffer: list[dict] = []
        part = 0
        written = 0
        for path in paths:
            level, periodicity, file_year, month = parse_filename(path.name)
            fields, rows = read_file(path)
            for rec in melt_rows(
                fields, rows, level=level, year=file_year, month=month
            ):
                row = to_columns(rec, table)
                if keep_row(row, table):
                    buffer.append(row)
                else:
                    dropped[table] = dropped.get(table, 0) + 1
            if len(buffer) >= FLUSH_ROWS:
                written += write_parquet(buffer, table, output_dir, part)
                part += 1
                buffer = []
        written += write_parquet(buffer, table, output_dir, part)
        totals[table] = totals.get(table, 0) + written
        log.info(f"{table} {year}: {written:,} rows")

    if not only or "dicionario" in (only or set()):
        rows = build_dicionario()
        shutil.rmtree(output_dir / "dicionario", ignore_errors=True)
        order, _, strings = _schemas("dicionario")
        frame = pd.DataFrame.from_records(rows)[order]
        target = output_dir / "dicionario"
        target.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(frame, schema=strings, preserve_index=False)
        pq.write_table(at, target / "data.parquet", compression="snappy")
        totals["dicionario"] = len(rows)
    for table, count in sorted(dropped.items()):
        log.info(f"{table}: dropped {count:,} rows with no geography code")
    return totals


# ── download ────────────────────────────────────────────────────────────────

BASE_URL = constants.BASE_URL.value
HEADERS = constants.HEADERS.value


def download_targets(through_year: int) -> list[tuple[str, Path]]:
    """Build the (url, destination) list for every published BPS file.

    Args:
        through_year: Last survey year to ask for. Years the Census Bureau has
            not published yet simply answer 404 and are skipped.

    Returns:
        One (url, destination) pair per candidate file.
    """
    out: list[tuple[str, Path]] = []

    def add(directory: str, prefix: str, level: str, kind: str, first: int):
        for year in range(first, through_year + 1):
            names = (
                [f"{prefix}{year % 100:02d}{m:02d}c.txt" for m in range(1, 13)]
                if kind == "monthly"
                else [f"{prefix}{year}a.txt"]
            )
            for name in names:
                url = BASE_URL + urllib.parse.quote(f"{directory}/{name}")
                out.append((url, Path(level) / name))

    for level, directory in constants.GEO_DIRS.value.items():
        prefix = constants.GEO_PREFIXES.value[level]
        for kind in ("monthly", "annual"):
            add(
                directory,
                prefix,
                level,
                kind,
                constants.FIRST_YEAR.value[(level, kind)],
            )
    for directory, prefix in constants.PLACE_REGIONS.value.items():
        for kind in ("monthly", "annual"):
            add(
                f"Place/{directory}",
                prefix,
                "place",
                kind,
                constants.FIRST_YEAR.value[("place", kind)],
            )
    return out


# Statuses that mean "slow down", not "broken". They need a far longer pause
# than a dropped connection: www2.census.gov returned 429 on three files
# partway through a full download from a datacentre, where requests leave
# faster than they do from a laptop.
THROTTLE_CODES = frozenset({408, 429, 503})


def _throttle_delay(exc: urllib.error.HTTPError, attempt: int) -> float:
    """Seconds to wait after a throttling response, honouring Retry-After."""
    header = exc.headers.get("Retry-After") if exc.headers else None
    if header and header.strip().isdigit():
        return min(120.0, float(header.strip()))
    return min(120.0, 5.0 * (2**attempt))


def fetch_file(url: str, dest: Path, tries: int = 6) -> tuple[str, str]:
    """Download one BPS file, distinguishing a missing file from a rejection.

    Three Census server behaviours would otherwise lose data or fail the run.
    A file that does not exist answers HTTP 404 with an HTML page, so the body
    is checked before anything is written. The site firewall rejects a few
    perfectly valid URLs with **HTTP 200** and a "Request Rejected" HTML page;
    treating that as a missing file dropped four place months and one
    metropolitan month on the first full run, and since the rejection is cached
    against the exact URL, every retry carries a cache-busting parameter. And
    the server throttles a sustained download with 429, which needs a much
    longer pause than a network flake.

    Args:
        url: Absolute URL of the file.
        dest: Destination path; parent directories are created.
        tries: Attempts before giving up.

    Returns:
        ``(status, detail)``; status is "ok", "cached", "missing" or "failed".
        Only an HTTP 404 yields "missing".
    """
    if dest.exists() and dest.stat().st_size > 0:
        return "cached", ""
    detail = ""
    for attempt in range(tries):
        target = url if attempt == 0 else f"{url}?attempt={attempt}"
        request = urllib.request.Request(target, headers=HEADERS)
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return "missing", "HTTP 404"
            detail = f"HTTP {exc.code}"
            if exc.code in THROTTLE_CODES:
                time.sleep(_throttle_delay(exc, attempt))
            else:
                time.sleep(2 * (attempt + 1))
            continue
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            time.sleep(2 * (attempt + 1))
            continue
        head = body[:1000].lower()
        if b"<html" in head or b"<!doctype" in head:
            detail = "firewall rejection (HTTP 200 with an HTML body)"
            time.sleep(1 + attempt)
            continue
        if not body.strip():
            detail = "empty body"
            time.sleep(1 + attempt)
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        return "ok", ""
    return "failed", detail


def download_all(
    input_dir: Path, through_year: int, workers: int = 4
) -> dict[str, int]:
    """Download every published BPS file into ``input_dir``.

    Args:
        input_dir: Directory to download into.
        through_year: Last survey year to ask for.
        workers: Concurrent downloads. Kept low: six was enough to draw
            HTTP 429 from the server partway through a full run.

    Returns:
        Counts by status.

    Raises:
        RuntimeError: If any file failed for a reason other than a 404. A
            partial download must stop the run rather than quietly produce a
            table with months missing.
    """
    jobs = [
        (url, input_dir / rel) for url, rel in download_targets(through_year)
    ]
    counts = {"ok": 0, "cached": 0, "missing": 0, "failed": 0}
    failures: list[str] = []
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_file, u, d): u for u, d in jobs}
        for future in cf.as_completed(futures):
            status, detail = future.result()
            counts[status] += 1
            if status == "failed":
                failures.append(f"{futures[future]} -- {detail}")
    log.info(f"download: {counts}")
    if failures:
        raise RuntimeError(
            f"{len(failures)} files failed to download (not 404s): "
            + "; ".join(failures[:10])
        )
    return counts


def latest_monthly_period(input_dir: Path) -> str:
    """Return the most recent monthly survey period present, as ``YYYY-MM``.

    Read from the state files, the smallest monthly series and the one that
    goes back furthest, so it is the cheapest reliable signal of what the
    Census Bureau has published.

    Args:
        input_dir: Directory holding the downloaded files.

    Returns:
        The latest period, e.g. ``"2026-07"``.

    Raises:
        ValueError: If no monthly state file was downloaded.
    """
    periods = []
    for path in (input_dir / "state").glob("st*c.txt"):
        _level, periodicity, year, month = parse_filename(path.name)
        if periodicity == "monthly" and month is not None:
            periods.append((year, month))
    if not periods:
        raise ValueError(f"no monthly state files under {input_dir}")
    year, month = max(periods)
    return f"{year:04d}-{month:02d}"
