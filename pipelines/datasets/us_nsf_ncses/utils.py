"""Pure download and transform functions for us_nsf_ncses.

This module is the single home of the NCSES transform. The one-shot onboarding
scripts under ``models/us_nsf_ncses/code/`` import from here rather than
duplicating it, so the recurring flow and the bootstrap can never drift apart.

Nothing here imports Prefect: ``tasks.py`` wraps these functions.

Two surveys, both annual:

* **HERD** — institution-level public use files, one ZIP per fiscal year from
  1972, plus a short-form ZIP per year from 2012. Already long: one row per
  (institution, year, questionnaire item, row, column).
* **SED** — the published data tables of one survey cycle, ~96 Excel workbooks
  in a single ZIP.

Every release restates its own history — HERD retro-imputes prior years, and an
SED cycle republishes its whole series — so a refresh rebuilds all partitions
rather than appending the newest one.

The NCSES server is slow and drops connections part way through a file, and a
truncated download still exits 0 with HTTP 200, so :func:`fetch` resumes and
checks the finished size against ``Content-Length``.
"""

from __future__ import annotations

import csv
import io
import os
import re
import zipfile
from collections import defaultdict
from pathlib import Path

# pyrefly: ignore [untyped-import]
import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# pyrefly: ignore [untyped-import]
from openpyxl.cell.rich_text import CellRichText

# NCSES serves documents only to something that looks like a browser.
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

HERD_MICRODATA_PAGE = (
    "https://ncses.nsf.gov/explore-data/microdata/"
    "higher-education-research-development"
)
HERD_FILE_URL = (
    "https://ncses.nsf.gov/821/assets/0/files/"
    "higher_education_r_and_d_{year}{suffix}.zip"
)
SED_SURVEY_PAGE = "https://ncses.nsf.gov/surveys/earned-doctorates"
SED_TABLES_URL = (
    "https://ncses.nsf.gov/pubs/{publication_id}/assets/data-tables/"
    "{publication_id}-data-tables-tables-excels.zip"
)

# The first HERD fiscal year NCSES publishes a public use file for.
FIRST_HERD_YEAR = 1972
# The first year a short-form file exists.
FIRST_SHORT_FORM_YEAR = 2012

# The cycle this dataset was onboarded from. These are fallbacks for the
# one-shot bootstrap scripts, which rebuild the onboarded vintage exactly; the
# recurring pipeline discovers the real years from NCSES and passes them in, so
# nothing downstream should read these literals to decide what "current" is.
LAST_ONBOARDED_HERD_YEAR = 2024
FIRST_ONBOARDED_SED_CYCLE = 2024
LAST_ONBOARDED_SED_CYCLE = 2024


def fetch(url: str, destination: Path, attempts: int = 10) -> Path:
    """Download a URL to a path, resuming until the size matches the server's.

    A plain download of an NCSES file returns HTTP 200 on a truncated body, so
    the only reliable completion test is the byte count.

    Args:
        url: What to download.
        destination: Where to write it.
        attempts: How many resume attempts before giving up.

    Returns:
        The destination path.

    Raises:
        RuntimeError: The file never reached the advertised length.
    """
    headers = {"User-Agent": USER_AGENT}
    head = requests.head(
        url, headers=headers, timeout=60, allow_redirects=True
    )
    head.raise_for_status()
    expected = int(head.headers.get("Content-Length", 0))
    destination.parent.mkdir(parents=True, exist_ok=True)

    for _ in range(attempts):
        have = destination.stat().st_size if destination.exists() else 0
        if expected and have == expected:
            return destination
        request_headers = dict(headers)
        if have:
            request_headers["Range"] = f"bytes={have}-"
        with requests.get(
            url, headers=request_headers, stream=True, timeout=600
        ) as response:
            response.raise_for_status()
            mode = "ab" if have and response.status_code == 206 else "wb"
            with open(destination, mode) as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    handle.write(chunk)

    have = destination.stat().st_size if destination.exists() else 0
    if expected and have != expected:
        raise RuntimeError(
            f"{url}: got {have} bytes, server advertised {expected}"
        )
    return destination


def latest_herd_year(page: str | None = None) -> int:
    """Return the most recent fiscal year with a HERD public use file."""
    if page is None:
        page = requests.get(
            HERD_MICRODATA_PAGE,
            headers={"User-Agent": USER_AGENT},
            timeout=120,
        ).text
    years = {
        int(match)
        for match in re.findall(r"higher_education_r_and_d_(\d{4})\.zip", page)
    }
    if not years:
        raise RuntimeError("no HERD public use files found on the NCSES page")
    return max(years)


def latest_sed_publication(page: str | None = None) -> tuple[int, str]:
    """Return the newest SED cycle year and its NCSES publication id."""
    if page is None:
        page = requests.get(
            SED_SURVEY_PAGE, headers={"User-Agent": USER_AGENT}, timeout=120
        ).text
    publications = set(
        re.findall(r"/pubs/(nsf\d{5})/assets/data-tables/", page)
    )
    if not publications:
        raise RuntimeError("no SED data-table publication found on the page")
    cycles = {int(y) for y in re.findall(r"earned-doctorates/(\d{4})", page)}
    if not cycles:
        raise RuntimeError("no SED cycle year found on the page")
    return max(cycles), sorted(publications)[-1]


def download_herd(input_dir: Path, latest_year: int) -> Path:
    """Download every HERD public use ZIP up to and including ``latest_year``."""
    for year in range(FIRST_HERD_YEAR, latest_year + 1):
        fetch(
            HERD_FILE_URL.format(year=year, suffix=""),
            input_dir / f"herd_{year}.zip",
        )
        if year >= FIRST_SHORT_FORM_YEAR:
            fetch(
                HERD_FILE_URL.format(year=year, suffix="_short"),
                input_dir / f"herd_{year}_short.zip",
            )
    return input_dir


def download_sed(
    input_dir: Path, reference_year: int, publication_id: str
) -> Path:
    """Download one SED cycle's data-table workbooks."""
    fetch(
        SED_TABLES_URL.format(publication_id=publication_id),
        input_dir / f"sed{reference_year}_xlsx.zip",
    )
    return input_dir


# NCSES reports every expenditure item in thousands of current dollars.
THOUSANDS = 1000

FILE_RE = re.compile(r"^herd_(\d{4})(_short)?\.zip$")

# Questionnaire items that are not expenditures. Everything else is monetary.
PERSONNEL_ITEMS = {
    "15": "headcount",  # Headcount of personnel (FY2022+)
    "16": "full_time_equivalent",  # FTEs (FY2022+)
    "NA_02": "headcount",  # Personnel (FY2010-FY2019)
    "NA_03": "headcount",  # Postdocs (FY2010-FY2015)
}
SURVEY_ITEMS = {
    "01.1",  # Inclusion of institution funds (response code)
    "05.1",  # Inclusion of clinical trials in the FY2009 report (FY2010 only)
    "13",  # Capitalisation thresholds (a dollar amount, not an expenditure)
}
# Items in SURVEY_ITEMS whose value is a dollar amount rather than a code.
SURVEY_ITEM_AMOUNTS = {"13"}


def read_rows(zip_path: Path):
    """Yield dict rows from the single CSV inside a HERD public use ZIP.

    Five of the files are cp1252 rather than UTF-8; both are tried in turn.
    """
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError(
                f"{zip_path.name}: expected one CSV, got {names}"
            )
        raw = zf.read(names[0])
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise RuntimeError(f"{zip_path.name}: undecodable")
    yield from csv.DictReader(io.StringIO(text))


def clean(value) -> str:
    """Strip a source field, mapping the missing markers to the empty string."""
    if value is None:
        return ""
    value = value.strip()
    # '??' state and '?????' ZIP mark aggregations of institutions, not values.
    if value in {"??", "?????"}:
        return ""
    return value


def normalise_status(value) -> str:
    """Lowercase the status code; FY1972-FY2009 mixes 'I'/'i' and 'E'/'e'."""
    return clean(value).lower()


def to_usd(value) -> str:
    """Convert a reported value in thousands of dollars to dollars."""
    value = clean(value)
    if value == "":
        return ""
    return repr(float(value) * THOUSANDS)


def source_files(input_dir: Path) -> list[tuple[Path, int, str]]:
    """Return (path, year, survey_form) for every HERD public use ZIP."""
    out = []
    for name in sorted(os.listdir(input_dir)):
        m = FILE_RE.match(name)
        if not m:
            continue
        year = int(m.group(1))
        form = "short" if m.group(2) else "standard"
        out.append((input_dir / name, year, form))
    return out


def build_unitid_map(files) -> dict[str, str]:
    """Map NCSES institution id -> IPEDS UNITID, learned from FY2010 onwards.

    The pre-FY2010 files carry no UNITID. ``inst_id`` is stable across the two
    survey eras (Charles R. Drew is ``000166`` in both FY1990 and FY2024), so
    the mapping observed from FY2010 is carried back. An institution that left
    the survey before FY2010 keeps a null UNITID.

    Raises if an institution id ever maps to two different UNITIDs, which would
    make the carry-back unsafe.
    """
    seen: dict[str, set[str]] = defaultdict(set)
    for path, year, _form in files:
        if year < 2010:
            continue
        for row in read_rows(path):
            inst = clean(row.get("inst_id"))
            unitid = clean(row.get("ipeds_unitid"))
            if inst and unitid:
                seen[inst].add(unitid)
    conflicts = {k: v for k, v in seen.items() if len(v) > 1}
    if conflicts:
        raise RuntimeError(
            f"institution id maps to several UNITIDs: {conflicts}"
        )
    return {k: next(iter(v)) for k, v in seen.items()}


def institution_fields(
    row: dict, year: int, form: str, unitid_map: dict
) -> dict:
    """Normalise the institution half of a source row across the three layouts."""
    if year >= 2010:
        inst_id = clean(row["inst_id"])
        return {
            "year": str(year),
            "institution_id": inst_id,
            "ncses_institution_id": clean(row.get("ncses_inst_id")),
            "unitid": clean(row.get("ipeds_unitid"))
            or unitid_map.get(inst_id, ""),
            "combined_institution_id": "",
            "survey_form": form,
            "institution_name": clean(row.get("inst_name_long")),
            "institution_city": clean(row.get("inst_city")),
            "state_abbreviation": clean(row.get("inst_state_code")),
            "zip_code": clean(row.get("inst_zip")),
            "hbcu_indicator": clean(row.get("hbcu_flag")),
            "medical_school_indicator": clean(row.get("med_sch_flag")),
            "high_hispanic_enrollment_indicator": clean(row.get("hhe_flag")),
            "institution_type_code": clean(row.get("toi_code")),
            "highest_degree_code": clean(row.get("hdg_code")),
            "control_type_code": clean(row.get("toc_code")),
            "fy09_pilot_indicator": "",
        }
    inst_id = clean(row["fice"])
    combined = clean(row.get("fice_combined"))
    return {
        "year": str(year),
        "institution_id": inst_id,
        "ncses_institution_id": "",
        "unitid": unitid_map.get(inst_id, ""),
        # "000000" means "not to be combined" — treat it as absent.
        "combined_institution_id": "" if combined == "000000" else combined,
        "survey_form": form,
        "institution_name": clean(row.get("inst_name_long")),
        "institution_city": clean(row.get("inst_city")),
        "state_abbreviation": clean(row.get("inst_state")),
        "zip_code": clean(row.get("inst_zip")),
        "hbcu_indicator": clean(row.get("hbcu_flag")),
        "medical_school_indicator": clean(row.get("has_med_sch_flag")),
        "high_hispanic_enrollment_indicator": clean(row.get("hhe_flag")),
        "institution_type_code": clean(row.get("toi_code")),
        "highest_degree_code": clean(row.get("hdg_code")),
        "control_type_code": clean(row.get("toc_code")),
        "fy09_pilot_indicator": clean(row.get("pilot_fy09_flag")),
    }


INSTITUTION_COLUMNS = [
    "year",
    "institution_id",
    "ncses_institution_id",
    "unitid",
    "combined_institution_id",
    "survey_form",
    "institution_name",
    "institution_city",
    "state_abbreviation",
    "zip_code",
    "hbcu_indicator",
    "medical_school_indicator",
    "high_hispanic_enrollment_indicator",
    "institution_type_code",
    "highest_degree_code",
    "control_type_code",
    "fy09_pilot_indicator",
]

EXPENDITURE_COLUMNS = [
    "year",
    "institution_id",
    "unitid",
    "survey_form",
    "question_code",
    "question",
    "row_label",
    "column_label",
    "expenditure",
    "status_code",
    "other_information",
    "standardized_agency_name",
]

PERSONNEL_COLUMNS = [
    "year",
    "institution_id",
    "unitid",
    "survey_form",
    "personnel_group",
    "personnel_function",
    "headcount",
    "headcount_status_code",
    "full_time_equivalent",
    "full_time_equivalent_status_code",
]

SURVEY_ITEM_COLUMNS = [
    "year",
    "institution_id",
    "unitid",
    "survey_form",
    "question_code",
    "question",
    "row_label",
    "column_label",
    "response_code",
    "amount",
    "other_information",
]

DICIONARIO_COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]


def write_partition(
    output_dir: Path,
    table: str,
    partition: str,
    value: int,
    columns: list[str],
    rows: list[dict],
) -> int:
    """Write one partition of one table as all-STRING snappy parquet.

    Args:
        output_dir: Root the table directories are written under.
        table: Table slug.
        partition: Partition column name, ``year`` or ``reference_year``.
        value: Partition value.
        columns: Column order, from the architecture.
        rows: The rows, every value already a string.

    Returns:
        How many rows were written.
    """
    if not rows:
        return 0
    schema = pa.schema([(c, pa.string()) for c in columns])
    arrays = [
        pa.array(
            [r.get(c) if r.get(c) != "" else None for r in rows],
            type=pa.string(),
        )
        for c in columns
    ]
    out = output_dir / table / f"{partition}={value}"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_arrays(arrays, schema=schema),
        out / "data.parquet",
        compression="snappy",
    )
    return len(rows)


def build_dicionario(
    herd_end_year: int = LAST_ONBOARDED_HERD_YEAR,
    sed_end_year: int = LAST_ONBOARDED_SED_CYCLE,
) -> list[dict]:
    """Value -> label for every coded column, per survey era.

    Codes are taken from the NCSES "Guide for Public Use Data Files" (FY2024).
    The two eras use different code sets for the same concepts, so each entry
    carries its own temporal coverage.

    The open era's coverage ends at the newest year actually processed, not at a
    literal: a refresh that ingests FY2025 must not ship a dictionary that still
    claims to stop at FY2024, or every coded value in the new partition would
    sit outside its own dictionary coverage.

    Args:
        herd_end_year: Newest HERD fiscal year in this build. Closes the open
            HERD era's temporal coverage.
        sed_end_year: Newest SED cycle in this build. Closes the coverage of the
            ``sed_estimate.unit`` codes.

    Returns:
        One dict per dictionary row, keyed by :data:`DICIONARIO_COLUMNS`.
    """
    herd_all = f"1972(1){herd_end_year}"
    new = f"2010(1){herd_end_year}"
    old = "1972(1)2009"
    entries: list[tuple[str, str, str, str, str]] = []

    def add(tables, column, coverage, mapping):
        for table in tables:
            for key, label in mapping.items():
                entries.append((table, column, key, coverage, label))

    facts = ["herd_expenditure", "herd_personnel", "herd_survey_item"]
    all_tables = ["herd_institution", *facts]

    add(
        all_tables,
        "survey_form",
        herd_all,
        {
            "standard": "Standard form questionnaire",
            "short": "Short form questionnaire (institutions under $1 million "
            "in total R&D; FY2012 onwards)",
        },
    )
    add(
        ["herd_institution"],
        "hbcu_indicator",
        new,
        {
            "0": "Not a historically black college or university",
            "1": "Historically black college or university",
        },
    )
    add(
        ["herd_institution"],
        "hbcu_indicator",
        old,
        {
            "F": "Not a historically black college or university",
            "T": "Historically black college or university",
        },
    )
    add(
        ["herd_institution"],
        "medical_school_indicator",
        herd_all,
        {
            "F": "Does not have a medical school",
            "N": "Null; information was not included",
            "T": "Has a medical school",
        },
    )
    add(
        ["herd_institution"],
        "high_hispanic_enrollment_indicator",
        new,
        {
            "0": "Not a high Hispanic enrollment institution",
            "1": "High Hispanic enrollment institution",
        },
    )
    add(
        ["herd_institution"],
        "high_hispanic_enrollment_indicator",
        old,
        {
            "F": "Not a high Hispanic enrollment institution",
            "N": "Null; information was not included",
            "T": "High Hispanic enrollment institution",
        },
    )
    add(
        ["herd_institution"],
        "institution_type_code",
        herd_all,
        {"1": "Academic"},
    )
    add(
        ["herd_institution"],
        "highest_degree_code",
        new,
        {
            "1": "Doctorate",
            "2": "Master's",
            "3": "Bachelor's",
            "4": "Associate's",
            "5": "No degree",
            "6": "Professional degree",
        },
    )
    add(
        ["herd_institution"],
        "highest_degree_code",
        old,
        {
            "1": "Doctorate",
            "2": "Master's",
            "3": "Bachelor's",
            "4": "No science and engineering degree (may grant a bachelor's or "
            "higher degree in a non-science program)",
            "8": "Two-year program",
            "9": "No degree assigned; aggregation of institutions",
        },
    )
    add(
        ["herd_institution"],
        "control_type_code",
        herd_all,
        {
            "1": "Public",
            "2": "Private",
            "?": "No institutional control assigned; aggregation of institutions",
        },
    )
    add(
        ["herd_institution"],
        "fy09_pilot_indicator",
        old,
        {
            "T": "Institution took part in the FY2009 HERD pilot survey",
            "F": "Institution did not take part in the FY2009 HERD pilot survey",
        },
    )
    status = {
        "e": "Estimated by NCSES",
        "i": "Imputed by computer for nonresponse",
        "n": "Data not available",
        "c": "Undocumented code present in the source files",
        "u": "Undocumented code present in the source files",
        "0": "Undocumented code present in the source files",
    }
    for table, column in [
        ("herd_expenditure", "status_code"),
        ("herd_personnel", "headcount_status_code"),
        ("herd_personnel", "full_time_equivalent_status_code"),
    ]:
        add([table], column, herd_all, status)
    add(
        ["herd_survey_item"],
        "response_code",
        new,
        {"-1": "Don't know", "0": "No", "1": "Yes"},
    )
    # SED published tables mix units within one table, so sed_estimate carries
    # the unit per cell. Codes are assigned by models/us_nsf_ncses/code/sed_clean.py.
    add(
        ["sed_estimate"],
        "unit",
        f"{FIRST_ONBOARDED_SED_CYCLE}(1){sed_end_year}",
        {
            "number": "Count of doctorate recipients or institutions",
            "percent": "Percentage, on a 0 to 100 scale",
            "dollars": "Current U.S. dollars",
            "median_years": "Median number of years",
            "median": "Median of the measure named in the column label",
            "mean": "Mean of the measure named in the column label",
        },
    )
    return [dict(zip(DICIONARIO_COLUMNS, e, strict=True)) for e in entries]


# Survey cycle -> NCSES publication id, for the onboarded vintage only. The
# recurring pipeline does NOT read this: it passes `clean_sed` the cycle that
# `latest_sed_publication` discovered and `download_sed` actually fetched.
# Iterating this map in the pipeline would make a refresh look for
# `sed2024_xlsx.zip` in a pod that only ever downloaded the new cycle's ZIP.
CYCLES = {LAST_ONBOARDED_SED_CYCLE: "nsf25349"}

TABLE_ID_RE = re.compile(r"^Table\s+(\d+)[\u2013-](\d+)")
FOOTNOTE_START = re.compile(
    r"^(note\(s\)|source\(s\)|notes:|source:|n/a\b)", re.I
)

UNIT_PATTERNS = [
    (re.compile(r"\(number\)|\bnumber\b", re.I), "number"),
    (re.compile(r"%|\(percent\)|\bpercent\b", re.I), "percent"),
    (re.compile(r"\bdollars\b|\bsalary\b|\bdebt\b", re.I), "dollars"),
    (re.compile(r"median years|years to degree", re.I), "median_years"),
    (re.compile(r"\bmean\b", re.I), "mean"),
    (re.compile(r"\bmedian\b", re.I), "median"),
]

TABLE_COLUMNS = [
    "reference_year",
    "table_id",
    "table_group",
    "table_title",
    "unit_statement",
    "publication_id",
    "source_file",
    "estimate_count",
]

ESTIMATE_COLUMNS = [
    "reference_year",
    "table_id",
    "year",
    "row_number",
    "column_number",
    "row_label",
    "row_path",
    "row_level",
    "column_label",
    "column_path",
    "unit",
    "value",
]


def cell_text(value) -> str:
    """Render a header or stub cell as text, dropping NCSES footnote markers.

    The marker on ``"All doctorate recipients<sup>a</sup>"`` is a superscript
    run inside the cell's rich text, so the workbook is read with
    ``rich_text=True`` and superscript runs are discarded. Guessing from the
    characters instead would truncate real labels — ``"Male"`` would lose its
    ``e`` and ``"Science and engineering"`` its ``g``.
    """
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, CellRichText):
        parts = []
        for block in value:
            if isinstance(block, str):
                parts.append(block)
            elif getattr(block.font, "vertAlign", None) != "superscript":
                parts.append(block.text)
        return "".join(parts).strip()
    return str(value).strip()


def parse_number(value):
    """Return a float for a data cell, or None for a suppression marker."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("$", "")
    if text in {
        "",
        "-",
        "\u2013",
        "\u2014",
        "na",
        "NA",
        "n/a",
        "D",
        "S",
        "(X)",
        "(S)",
        "(D)",
    }:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def infer_unit(*candidates: str) -> str:
    """Pick a unit from the most specific label available.

    Candidates are tried in order, so a column header that says ``"(%)"`` beats
    the table's ``"(Number and percent)"`` statement.
    """
    for text in candidates:
        if not text:
            continue
        for pattern, unit in UNIT_PATTERNS:
            if pattern.search(text):
                return unit
    return ""


def year_or_none(text: str):
    """Return a four-digit year from a header or stub label, else None."""
    text = text.strip()
    if re.fullmatch(r"(19|20)\d{2}", text):
        return int(text)
    return None


MAX_HEADER_ROW = 8


def header_chains(ws, merged) -> tuple[list[list[str]], int]:
    """Return (one label chain per column, first data row index, 0-based).

    The header starts at row 4 and is as deep as its merged ranges reach: a
    table with a plain header has none and is one row deep, while tables such
    as 3-3 and 6-1 nest three rows (citizenship, then ethnicity, then race).
    Reading only two rows collapses every race column onto the same pair of
    labels, which is what made 1,804 key collisions in the 2024 cycle.

    Horizontal merges spread their label across the span. Vertical merges are
    left alone, so a column spanning the whole header contributes one label
    rather than repeating it at every level.
    """
    grid = [[c.value for c in row] for row in ws.iter_rows()]
    ncol = max(len(r) for r in grid[:MAX_HEADER_ROW]) if grid else 0

    # Grow the header block while a merged range that starts inside it reaches
    # further down; the title merge on row 2 is outside the block and ignored.
    last = 4
    while True:
        reach = max(
            (r.max_row for r in merged if 4 <= r.min_row <= last),
            default=last,
        )
        reach = min(reach, MAX_HEADER_ROW)
        if reach == last:
            break
        last = reach

    rows = []
    for source in grid[3:last]:
        labels = [cell_text(v) for v in source]
        rows.append(labels + [""] * (ncol - len(labels)))
    for r in merged:
        if not (4 <= r.min_row <= last and r.max_col > r.min_col):
            continue
        label = rows[r.min_row - 4][r.min_col - 1]
        for c in range(r.min_col, min(r.max_col, ncol)):
            rows[r.min_row - 4][c] = label

    chains = [[row[c] for row in rows if row[c]] for c in range(ncol)]
    return chains, last


def parse_workbook(path: Path, reference_year: int, publication_id: str):
    """Parse one NCSES data table workbook into table + estimate rows."""
    wb = openpyxl.load_workbook(path, data_only=True, rich_text=True)
    ws = wb[wb.sheetnames[0]]
    merged = list(ws.merged_cells.ranges)
    grid = list(ws.iter_rows())

    tid_match = TABLE_ID_RE.match(cell_text(grid[0][0].value))
    if not tid_match:
        wb.close()
        raise RuntimeError(f"{path.name}: no table id in cell A1")
    table_id = f"{tid_match.group(1)}-{tid_match.group(2)}"
    title = cell_text(grid[1][0].value)
    unit_statement = cell_text(grid[2][0].value).strip("()")

    chains, first_data = header_chains(ws, merged)

    estimates: list[dict] = []
    ancestors: dict[int, str] = {}
    section_label = ""
    # Some tables restate the column labels part way down the stub — table 4-4
    # switches from "Median debt (dollars)" to a Number/Percent pair under each
    # debt concept. Such a row has an empty stub and text in the value columns,
    # and it re-labels every column below it until the next one.
    overrides: dict[int, str] = {}

    for row_number, row in enumerate(grid[first_data:], start=first_data + 1):
        stub_cell = row[0]
        stub = cell_text(stub_cell.value)
        if FOOTNOTE_START.match(stub):
            break
        if not stub:
            restated = {
                i + 1: cell_text(c.value)
                for i, c in enumerate(row[1:])
                if parse_number(c.value) is None and cell_text(c.value)
            }
            if restated:
                overrides = restated
            continue
        level = int(stub_cell.alignment.indent or 0)
        label = stub
        ancestors = {k: v for k, v in ancestors.items() if k < level}
        ancestors[level] = label
        path_labels = [ancestors[k] for k in sorted(ancestors)]

        stub_year = year_or_none(label)
        values = [parse_number(c.value) for c in row[1:]]
        if all(v is None for v in values):
            # A stub row with no numbers is a section heading; it also carries
            # the unit for the rows beneath it (e.g. "All recipients (number)").
            section_label = label
            continue

        for offset, value in enumerate(values):
            if value is None:
                continue
            col = offset + 1
            chain = chains[col] if col < len(chains) else []
            # A header level that is just a year is the table's time axis, not
            # a column label: it becomes `year` and drops out of the chain.
            header_year = next(
                (year_or_none(part) for part in chain if year_or_none(part)),
                None,
            )
            labels = [part for part in chain if not year_or_none(part)]
            if overrides.get(col):
                labels = [*labels, overrides[col]]
            year = stub_year or header_year or reference_year
            column_label = labels[-1] if labels else ""
            estimates.append(
                {
                    "reference_year": str(reference_year),
                    "table_id": table_id,
                    "year": str(year),
                    "row_number": str(row_number),
                    "column_number": str(col + 1),
                    "row_label": label,
                    "row_path": " > ".join(path_labels),
                    "row_level": str(level),
                    "column_label": column_label,
                    "column_path": " > ".join(labels),
                    "unit": infer_unit(
                        *reversed(labels),
                        section_label,
                        unit_statement,
                    ),
                    "value": repr(value),
                }
            )
    wb.close()

    table_row = {
        "reference_year": str(reference_year),
        "table_id": table_id,
        "table_group": tid_match.group(1),
        "table_title": title,
        "unit_statement": unit_statement,
        "publication_id": publication_id,
        "source_file": path.name,
        "estimate_count": str(len(estimates)),
    }
    return table_row, estimates


def extract_cycle(
    input_dir: Path, work_dir: Path, reference_year: int, publication_id: str
) -> Path:
    """Unpack a cycle's data-table ZIP into the working directory."""
    zip_path = input_dir / f"sed{reference_year}_xlsx.zip"
    out = work_dir / publication_id
    out.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out)
    return out


def clean_herd(
    input_dir: Path,
    output_dir: Path,
    sed_end_year: int = LAST_ONBOARDED_SED_CYCLE,
) -> dict[str, Path]:
    """Clean every HERD public use file into partitioned all-STRING parquet.

    Also writes the shared ``dicionario``, which covers both surveys' coded
    columns — which is why the SED cycle has to reach this far.

    Args:
        input_dir: Directory holding the downloaded ``herd_*.zip`` files.
        output_dir: Root the partitioned parquet is written under.
        sed_end_year: Newest SED cycle in this build, for the dictionary's
            ``sed_estimate.unit`` coverage. Defaults to the onboarded vintage.

    Returns:
        Table slug -> the table's output directory, for ``upload_to_gcs``.
    """
    files = source_files(input_dir)
    if not files:
        raise RuntimeError(f"no HERD ZIPs under {input_dir}")
    print(f"{len(files)} source files", flush=True)

    print("building institution id -> UNITID map from FY2010+ ...", flush=True)
    unitid_map = build_unitid_map(files)
    print(f"  {len(unitid_map)} institutions with a UNITID", flush=True)

    files_by_year: dict[int, list[tuple[Path, str]]] = defaultdict(list)
    for path, year, form in files:
        files_by_year[year].append((path, form))

    totals: dict[str, int] = defaultdict(int)

    for year in sorted(files_by_year):
        institutions: dict[str, dict] = {}
        expenditure: list[dict] = []
        personnel: dict[tuple, dict] = {}
        items: list[dict] = []

        for path, form in files_by_year[year]:
            n = 0
            for row in read_rows(path):
                n += 1
                inst = institution_fields(row, year, form, unitid_map)
                key = inst["institution_id"]
                prev = institutions.get(key)
                if prev is None:
                    institutions[key] = inst
                elif prev != inst:
                    raise RuntimeError(
                        f"{path.name}: institution {key} described two ways in {year}"
                    )

                qcode = clean(row.get("questionnaire_no"))
                common = {
                    "year": str(year),
                    "institution_id": key,
                    "unitid": inst["unitid"],
                    "survey_form": form,
                }
                status = normalise_status(row.get("status"))

                if qcode in PERSONNEL_ITEMS:
                    measure = PERSONNEL_ITEMS[qcode]
                    group = (
                        "Postdocs"
                        if qcode == "NA_03"
                        else clean(row.get("row"))
                    )
                    function = clean(row.get("column"))
                    rec = personnel.setdefault(
                        (key, group, function),
                        {
                            **common,
                            "personnel_group": group,
                            "personnel_function": function,
                            "headcount": "",
                            "headcount_status_code": "",
                            "full_time_equivalent": "",
                            "full_time_equivalent_status_code": "",
                        },
                    )
                    rec[measure] = clean(row.get("data"))
                    rec[f"{measure}_status_code"] = status
                elif qcode in SURVEY_ITEMS:
                    value = clean(row.get("data"))
                    is_amount = qcode in SURVEY_ITEM_AMOUNTS
                    items.append(
                        {
                            **common,
                            "question_code": qcode,
                            "question": clean(row.get("question")),
                            "row_label": clean(row.get("row")),
                            "column_label": clean(row.get("column")),
                            "response_code": "" if is_amount else value,
                            "amount": to_usd(value) if is_amount else "",
                            "other_information": clean(row.get("othinfo")),
                        }
                    )
                else:
                    expenditure.append(
                        {
                            **common,
                            "question_code": qcode,
                            "question": clean(row.get("question")),
                            "row_label": clean(row.get("row")),
                            "column_label": clean(row.get("column")),
                            "expenditure": to_usd(row.get("data")),
                            "status_code": status,
                            "other_information": clean(row.get("othinfo")),
                            "standardized_agency_name": clean(
                                row.get("standardized_agency_names")
                            ),
                        }
                    )
            totals["source"] += n

        inst_rows = sorted(
            institutions.values(), key=lambda r: r["institution_id"]
        )
        pers_rows = sorted(
            personnel.values(),
            key=lambda r: (
                r["institution_id"],
                r["personnel_group"],
                r["personnel_function"],
            ),
        )
        totals["herd_institution"] += write_partition(
            output_dir,
            "herd_institution",
            "year",
            year,
            INSTITUTION_COLUMNS,
            inst_rows,
        )
        totals["herd_expenditure"] += write_partition(
            output_dir,
            "herd_expenditure",
            "year",
            year,
            EXPENDITURE_COLUMNS,
            expenditure,
        )
        totals["herd_personnel"] += write_partition(
            output_dir,
            "herd_personnel",
            "year",
            year,
            PERSONNEL_COLUMNS,
            pers_rows,
        )
        totals["herd_survey_item"] += write_partition(
            output_dir,
            "herd_survey_item",
            "year",
            year,
            SURVEY_ITEM_COLUMNS,
            items,
        )
        print(
            f"  {year}: inst={len(inst_rows):<5} exp={len(expenditure):<7} "
            f"pers={len(pers_rows):<5} item={len(items)}",
            flush=True,
        )

    # Coverage follows what this build actually produced, not a literal.
    dicionario = build_dicionario(
        herd_end_year=max(files_by_year), sed_end_year=sed_end_year
    )
    out = output_dir / "dicionario"
    out.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in DICIONARIO_COLUMNS])
    pq.write_table(
        pa.Table.from_arrays(
            [
                pa.array([r[c] for r in dicionario], type=pa.string())
                for c in DICIONARIO_COLUMNS
            ],
            schema=schema,
        ),
        out / "data.parquet",
        compression="snappy",
    )
    totals["dicionario"] = len(dicionario)

    print("\n== row counts ==")
    for name in (
        "source",
        "herd_institution",
        "herd_expenditure",
        "herd_personnel",
        "herd_survey_item",
        "dicionario",
    ):
        print(f"  {name:<20} {totals[name]:>9,}")
    facts = (
        totals["herd_expenditure"]
        + totals["herd_personnel"]
        + totals["herd_survey_item"]
    )
    # Question 15 (headcount) and Question 16 (FTE) share a personnel row, so
    # the fact tables hold fewer rows than the source; nothing else is dropped.
    print(f"  fact rows {facts:,} vs source {totals['source']:,}")
    return {
        table: output_dir / table
        for table in (
            "herd_institution",
            "herd_expenditure",
            "herd_personnel",
            "herd_survey_item",
            "dicionario",
        )
    }


def clean_sed(
    input_dir: Path,
    output_dir: Path,
    work_dir: Path,
    cycles: dict[int, str] | None = None,
) -> dict[str, Path]:
    """Parse every published SED data table of the given cycles.

    Args:
        input_dir: Directory holding the downloaded ``sed<year>_xlsx.zip``.
        output_dir: Root the partitioned parquet is written under.
        work_dir: Scratch directory the workbooks are unpacked into.
        cycles: Reference year -> NCSES publication id, for the cycles actually
            downloaded into ``input_dir``. Defaults to :data:`CYCLES`, the
            onboarded vintage, which is what the bootstrap script wants. The
            recurring pipeline must pass the cycle it discovered — a fresh pod
            holds only that cycle's ZIP, so defaulting here would send a 2025
            refresh looking for ``sed2024_xlsx.zip``.

    Returns:
        Table slug -> the table's output directory, for ``upload_to_gcs``.
    """
    for reference_year, publication_id in sorted((cycles or CYCLES).items()):
        folder = extract_cycle(
            input_dir, work_dir, reference_year, publication_id
        )
        books = sorted(folder.glob(f"{publication_id}-tab*.xlsx"))
        if not books:
            raise RuntimeError(f"no workbooks under {folder}")
        tables, estimates = [], []
        for book in books:
            table_row, rows = parse_workbook(
                book, reference_year, publication_id
            )
            tables.append(table_row)
            estimates.extend(rows)
        tables.sort(
            key=lambda r: (
                int(r["table_group"]),
                int(r["table_id"].split("-")[1]),
            )
        )
        n_tables = write_partition(
            output_dir,
            "sed_data_table",
            "reference_year",
            reference_year,
            TABLE_COLUMNS,
            tables,
        )
        n_est = write_partition(
            output_dir,
            "sed_estimate",
            "reference_year",
            reference_year,
            ESTIMATE_COLUMNS,
            estimates,
        )
        unresolved = sum(1 for e in estimates if not e["unit"])
        with_year = sum(
            1 for e in estimates if int(e["year"]) != reference_year
        )
        print(
            f"SED {reference_year} ({publication_id}): {n_tables} tables, "
            f"{n_est:,} estimates; {with_year:,} carry a data year other than "
            f"the cycle year; unit unresolved on {unresolved:,} "
            f"({unresolved / max(n_est, 1):.1%})"
        )
        empty = [t["table_id"] for t in tables if t["estimate_count"] == "0"]
        if empty:
            raise RuntimeError(f"tables parsed to zero rows: {empty}")
    return {
        table: output_dir / table
        for table in ("sed_estimate", "sed_data_table")
    }
