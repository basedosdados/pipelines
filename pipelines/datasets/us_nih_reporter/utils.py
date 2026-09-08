"""Pure download and transform helpers for us_nih_reporter — no Prefect imports.

This module is the single home of the cleaning transform. The one-shot onboarding
scripts under ``models/us_nih_reporter/code/`` import these functions rather than
duplicating them, so the recurring pipeline and the bootstrap cannot drift.

Seven source traps the transform exists to neutralise, each measured over the
full FY1985-FY2025 corpus (2,951,523 project rows):

* **The project file's header changes four times and its column order changes
  with it.** ``SUBPROJECT_ID`` is the 14th column in FY1985-1989 and FY1991 and
  the 40th in FY1990 and FY1992-2005; FY2006 adds ``FUNDING_MECHANISM``,
  ``ORG_IPF_CODE`` and the two cost-component columns and starts quoting every
  field; FY2025 renames ``CFDA_CODE`` to ``ASSISTANCE_LISTING_NUMBER``. Reading
  by position would silently transpose two columns for six fiscal years. Every
  file is read by header name, with ``constants.HEADER_RENAME`` folding the two
  renames onto one name.

* **The component columns are not a decomposition of the project number.**
  ``CORE_PROJECT_NUM`` is a substring of ``FULL_PROJECT_NUM`` on all 2.95M rows,
  but it disagrees with ``ACTIVITY + ADMINISTERING_IC + SERIAL_NUMBER`` on 91,147
  of them (3.09%) — the administering IC moves when an award is transferred while
  the number keeps the original letters. Both are shipped and neither is derived
  from the other: ``core_project_num`` is the join key to the publication, patent
  and clinical-study tables; the component columns describe the award as it now
  stands.

* **Dates arrive in three formats.** ``PROJECT_START`` and ``BUDGET_START`` are
  ``YYYY-MM-DD`` on 1.55M rows and ``M/D/YYYY`` on 1.08M; ``AWARD_NOTICE_DATE``
  adds a third with a time component. A plain ``safe_cast(... as date)`` returns
  null for the ``M/D/YYYY`` era, which is most of 1985-2005. All three are
  normalised to ``YYYY-MM-DD`` here.

* **Costs are absent from the FY1985-FY1999 project files** and published in a
  separate accessory file, which NIH documents as joining on ``APPLICATION_ID``.
  The transform merges ``TOTAL_COST``, ``TOTAL_COST_SUB_PROJECT``, ``FUNDING_ICS``
  and ``ORG_DUNS`` from it, filling only where the main file is blank.

* **``ORG_STATE`` is not a US state field.** Its 70 values include eleven
  Canadian provinces (ON, PQ, QC, BC, AB, MB, NS, SK, NL, NB, PE) and eight
  territories and freely-associated states (PR, GU, VI, AS, MP, PW, MH, FM). It
  is kept as published and carries no directory link, because a link to a US
  state directory would be wrong for those rows.

* **The download endpoint intermittently answers 404 for a file that exists.**
  One of 176 downloads failed this way and succeeded on the next attempt, so the
  fetch retries rather than treating 404 as absence.

* **Some publication files carry a UTF-8 byte order mark.** Decoded as plain
  utf-8, the mark stays glued to the first header name and stops ``csv``
  recognising it as a quoted field, so it reads as ``﻿"AFFILIATION"``
  rather than ``AFFILIATION`` and the affiliation column comes out empty for
  every affected year. Files are decoded ``utf-8-sig`` first, and header names
  are stripped of any surviving quote.

* **Files are not all UTF-8.** Older years are Windows-1252; decoding is
  attempted in order and falls back.

Output parquet is **all-STRING**. ``pipelines.utils.gcs.dump_header`` stringifies
the header BigQuery infers the staging schema from, so typed parquet is rejected;
the dbt model ``safe_cast``s every column to its architecture type.
"""

import csv
import io
import re
import sys
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_nih_reporter.constants import constants

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

DOWNLOAD_CHUNK = 8 * 1024 * 1024

# utf-8-sig first: some publication files carry a UTF-8 byte order mark, and
# plain utf-8 leaves it glued to the first header name, which then reads as
# '﻿"AFFILIATION"' rather than AFFILIATION and empties that column for the
# affected years.
_ENCODINGS = ("utf-8-sig", "cp1252", "latin-1")

_DATE_ISO = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})")
_DATE_US = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


@dataclass
class Col:
    """One column of an architecture table.

    The transform only needs ``name`` / ``bq_type`` / ``original``; the remaining
    fields are carried so the metadata scripts read the same CSV through the same
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


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------


def source_url(family: str, year: int | None) -> str:
    """URL of one ExPORTER bulk file."""
    path = constants.FAMILIES.value[family][0]
    base = f"{constants.BASE_URL.value}/{path}/download"
    return base if year is None else f"{base}/{year}"


def source_filename(family: str, year: int | None) -> str:
    """Local name for one ExPORTER bulk file.

    Projects, abstracts, publications and link tables arrive zipped; patents and
    clinical studies arrive as bare CSV, so the extension follows the family.
    """
    stem = {
        "projects": "PRJ_FY",
        "abstracts": "PRJABS_FY",
        "publications": "PUB_",
        "linktables": "PUBLNK_",
    }
    if year is None:
        return {
            "patents": "PATENTS_ALL.csv",
            "clinicalstudies": "CLINICAL_ALL.csv",
        }[family]
    return f"{stem[family]}{year}.zip"


def download_file(
    url: str,
    dest: Path,
    session: requests.Session | None = None,
    skip_existing: bool = True,
) -> Path:
    """Download one file, retrying a 404.

    The exporter endpoint answers 404 intermittently for files that exist — one
    of 176 downloads while building this dataset, which succeeded on the next
    attempt — so a 404 is retried rather than read as absence.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if skip_existing and dest.exists() and dest.stat().st_size > 1000:
        return dest
    sess = session or requests.Session()
    tmp = dest.with_suffix(dest.suffix + ".part")
    last = ""
    for attempt in range(1, constants.DOWNLOAD_ATTEMPTS.value + 1):
        try:
            with sess.get(
                url, stream=True, timeout=constants.REQUEST_TIMEOUT.value
            ) as resp:
                if resp.status_code != 200:
                    last = f"HTTP {resp.status_code}"
                    raise OSError(last)
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(DOWNLOAD_CHUNK):
                        fh.write(chunk)
            if tmp.stat().st_size <= 1000:
                last = f"suspiciously small ({tmp.stat().st_size} bytes)"
                raise OSError(last)
            tmp.replace(dest)
            return dest
        except Exception as exc:
            last = str(exc) or last
            tmp.unlink(missing_ok=True)
            if attempt == constants.DOWNLOAD_ATTEMPTS.value:
                raise RuntimeError(f"{url}: {last}") from exc
            time.sleep(constants.DOWNLOAD_RETRY_SLEEP.value * attempt)
    raise RuntimeError(f"{url}: {last}")


def download_family(
    family: str,
    years: list[int] | None,
    input_dir: Path,
    session: requests.Session | None = None,
    skip_existing: bool = True,
) -> list[Path]:
    """Download every file of one family. ``years`` is ignored for all-year families."""
    sess = session or requests.Session()
    out = []
    if constants.FAMILIES.value[family][2] == "none":
        dest = input_dir / source_filename(family, None)
        out.append(
            download_file(source_url(family, None), dest, sess, skip_existing)
        )
        return out
    for year in years or []:
        dest = input_dir / source_filename(family, year)
        out.append(
            download_file(source_url(family, year), dest, sess, skip_existing)
        )
    return out


def download_funding_supplement(
    input_dir: Path,
    session: requests.Session | None = None,
    skip_existing: bool = True,
) -> Path:
    """Download the FY1985-FY1999 cost and DUNS accessory file."""
    dest = input_dir / "PRJFUNDING_FY1985_FY1999.zip"
    return download_file(
        constants.FUNDING_SUPPLEMENT_URL.value, dest, session, skip_existing
    )


# --------------------------------------------------------------------------
# Reading
# --------------------------------------------------------------------------


def _decode(raw: bytes) -> str:
    for enc in _ENCODINGS:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")


def read_rows(path: Path, member: str | None = None):
    """Yield dict rows from a CSV, whether bare or inside a zip.

    Header names are stripped and passed through ``constants.HEADER_RENAME`` so
    the two columns ExPORTER renamed mid-history read under one name. Rows are
    matched to the header by name, never by position — six fiscal years order
    their columns differently from their neighbours.
    """
    if path.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(path)
        members = [member] if member else zf.namelist()
        for name in members:
            yield from _rows_from_text(_decode(zf.open(name).read()))
        return
    yield from _rows_from_text(_decode(path.read_bytes()))


def _rows_from_text(text: str):
    rename = constants.HEADER_RENAME.value
    reader = csv.reader(io.StringIO(text))
    header = [_header_name(h, rename) for h in next(reader)]
    for row in reader:
        yield dict(zip(header, row, strict=False))


def _header_name(raw: str, rename: dict) -> str:
    """Normalise one header cell.

    Quotes are stripped defensively: a byte order mark ahead of an opening quote
    stops ``csv`` recognising the field as quoted, so the name arrives with its
    quotes still attached. ``_decode`` removes the mark, and this removes any
    quote that survives it.
    """
    name = raw.strip().strip('"').strip()
    return rename.get(name, name)


# --------------------------------------------------------------------------
# Field normalisation
# --------------------------------------------------------------------------


def _s(row: dict, key: str) -> str:
    """A stripped string field, empty when absent or blank."""
    v = row.get(key)
    return v.strip() if isinstance(v, str) else ""


def norm_date(raw: str) -> str:
    """Normalise the three published date shapes to ``YYYY-MM-DD``.

    ``YYYY-MM-DD``, ``YYYY-MM-DD HH:MM:SS`` and ``M/D/YYYY`` all occur; the last
    is the only shape used for PROJECT_START and BUDGET_START through most of
    1985-2005, so dropping it would empty those columns for two decades.
    """
    v = (raw or "").strip()
    if not v:
        return ""
    m = _DATE_ISO.match(v)
    if m:
        y, mo, d = m.groups()
    else:
        m = _DATE_US.match(v)
        if not m:
            return ""
        mo, d, y = m.groups()
    try:
        yi, mi, di = int(y), int(mo), int(d)
    except ValueError:
        return ""
    if not (1 <= mi <= 12 and 1 <= di <= 31) or yi < 1900 or yi > 2100:
        return ""
    return f"{yi:04d}-{mi:02d}-{di:02d}"


def norm_int(raw: str) -> str:
    """An integer rendered as text, or empty. Never ``'nan'``."""
    v = (raw or "").strip().replace(",", "")
    if not v:
        return ""
    try:
        return str(int(float(v)))
    except ValueError:
        return ""


def norm_float(raw: str) -> str:
    """A number rendered as text, or empty."""
    v = (raw or "").strip().replace(",", "").replace("$", "")
    if not v:
        return ""
    try:
        f = float(v)
    except ValueError:
        return ""
    return str(int(f)) if f == int(f) else repr(f)


# --------------------------------------------------------------------------
# Cleaning
# --------------------------------------------------------------------------


def load_funding_supplement(input_dir: Path) -> dict[str, dict[str, str]]:
    """Read the FY1985-FY1999 accessory file, keyed by APPLICATION_ID.

    The main project files for those fiscal years carry no cost at all; NIH
    publishes ``TOTAL_COST``, ``TOTAL_COST_SUB_PROJECT``, ``FUNDING_ICS`` and
    ``ORG_DUNS`` separately and documents the join on ``APPLICATION_ID``.
    """
    path = input_dir / "PRJFUNDING_FY1985_FY1999.zip"
    if not path.exists():
        return {}
    out: dict[str, dict[str, str]] = {}
    for row in read_rows(path):
        aid = _s(row, "APPLICATION_ID")
        if aid:
            out[aid] = row
    return out


def clean_project(row: dict, supplement: dict | None = None) -> dict:
    """One cleaned row of the ``project`` table."""
    sup = supplement or {}
    total_cost = _s(row, "TOTAL_COST") or _s(sup, "TOTAL_COST")
    total_sub = _s(row, "TOTAL_COST_SUB_PROJECT") or _s(
        sup, "TOTAL_COST_SUB_PROJECT"
    )
    funding_ics = _s(row, "FUNDING_ICs") or _s(sup, "FUNDING_ICS")
    org_duns = _s(row, "ORG_DUNS") or _s(sup, "ORG_DUNS")
    return {
        "year": norm_int(_s(row, "FY")),
        "application_id": _s(row, "APPLICATION_ID"),
        "full_project_num": _s(row, "FULL_PROJECT_NUM"),
        "core_project_num": _s(row, "CORE_PROJECT_NUM"),
        "subproject_id": _s(row, "SUBPROJECT_ID"),
        "application_type": _s(row, "APPLICATION_TYPE"),
        "activity": _s(row, "ACTIVITY"),
        "administering_ic": _s(row, "ADMINISTERING_IC"),
        "serial_number": _s(row, "SERIAL_NUMBER"),
        "suffix": _s(row, "SUFFIX"),
        "support_year": norm_int(_s(row, "SUPPORT_YEAR")),
        "assistance_listing_number": _s(row, "ASSISTANCE_LISTING_NUMBER"),
        "opportunity_number": _s(row, "OPPORTUNITY NUMBER"),
        "study_section": _s(row, "STUDY_SECTION"),
        "org_ipf_id": _s(row, "ORG_IPF_CODE"),
        "org_duns": org_duns,
        "pi_ids": _s(row, "PI_IDS"),
        "project_title": _s(row, "PROJECT_TITLE"),
        "ic_name": _s(row, "IC_NAME"),
        "funding_mechanism": _s(row, "FUNDING_MECHANISM"),
        "funding_ics": funding_ics,
        "arra_funded": _s(row, "ARRA_FUNDED"),
        "study_section_name": _s(row, "STUDY_SECTION_NAME"),
        "program_officer_name": _s(row, "PROGRAM_OFFICER_NAME"),
        "pi_names": _s(row, "PI_NAMEs"),
        "org_name": _s(row, "ORG_NAME"),
        "org_dept": _s(row, "ORG_DEPT"),
        "ed_inst_type": _s(row, "ED_INST_TYPE"),
        "org_city": _s(row, "ORG_CITY"),
        "org_state": _s(row, "ORG_STATE"),
        "org_zipcode": _s(row, "ORG_ZIPCODE"),
        "org_district": _s(row, "ORG_DISTRICT"),
        "org_country": _s(row, "ORG_COUNTRY"),
        "org_fips": _s(row, "ORG_FIPS"),
        "project_start": norm_date(_s(row, "PROJECT_START")),
        "project_end": norm_date(_s(row, "PROJECT_END")),
        "budget_start": norm_date(_s(row, "BUDGET_START")),
        "budget_end": norm_date(_s(row, "BUDGET_END")),
        "award_notice_date": norm_date(_s(row, "AWARD_NOTICE_DATE")),
        "nih_spending_cats": _s(row, "NIH_SPENDING_CATS"),
        "project_terms": _s(row, "PROJECT_TERMS"),
        "public_health_relevance": _s(row, "PHR"),
        "direct_cost": norm_float(_s(row, "DIRECT_COST_AMT")),
        "indirect_cost": norm_float(_s(row, "INDIRECT_COST_AMT")),
        "total_cost": norm_float(total_cost),
        "total_cost_subproject": norm_float(total_sub),
    }


def clean_project_abstract(row: dict, year: int) -> dict:
    """One cleaned row of ``project_abstract``.

    The abstract file carries no fiscal year of its own; it takes the year of the
    file it came from, which is the same fiscal year as the project record with
    the same application id.
    """
    return {
        "year": str(year),
        "application_id": _s(row, "APPLICATION_ID"),
        "abstract_text": _s(row, "ABSTRACT_TEXT"),
    }


def clean_publication(row: dict, year: int) -> dict:
    """One cleaned row of ``publication``."""
    return {
        "year": str(year),
        "pmid": _s(row, "PMID"),
        "pmc_id": _s(row, "PMC_ID"),
        "issn": _s(row, "ISSN"),
        "publication_title": _s(row, "PUB_TITLE"),
        "journal_title": _s(row, "JOURNAL_TITLE"),
        "journal_title_abbreviation": _s(row, "JOURNAL_TITLE_ABBR"),
        "journal_volume": _s(row, "JOURNAL_VOLUME"),
        "journal_issue": _s(row, "JOURNAL_ISSUE"),
        "page_number": _s(row, "PAGE_NUMBER"),
        "publication_date": _s(row, "PUB_DATE"),
        "publication_year": norm_int(_s(row, "PUB_YEAR")),
        "author_list": _s(row, "AUTHOR_LIST"),
        "affiliation": _s(row, "AFFILIATION"),
        "country": _s(row, "COUNTRY"),
        "language": _s(row, "LANG"),
    }


def clean_publication_link(row: dict, year: int) -> dict:
    """One cleaned row of ``publication_link``."""
    return {
        "year": str(year),
        "pmid": _s(row, "PMID"),
        "core_project_num": _s(row, "PROJECT_NUMBER"),
    }


def clean_patent_link(row: dict) -> dict:
    """One cleaned row of ``patent_link``."""
    return {
        "patent_id": _s(row, "PATENT_ID"),
        "core_project_num": _s(row, "PROJECT_ID"),
        "patent_title": _s(row, "PATENT_TITLE"),
        "patent_org_name": _s(row, "PATENT_ORG_NAME"),
    }


def clean_clinical_study_link(row: dict) -> dict:
    """One cleaned row of ``clinical_study_link``."""
    return {
        "nct_id": _s(row, "ClinicalTrials.gov ID"),
        "core_project_num": _s(row, "Core Project Number"),
        "study_title": _s(row, "Study"),
        "study_status": _s(row, "Study Status"),
    }


# --------------------------------------------------------------------------
# Parquet output
# --------------------------------------------------------------------------


def _string_schema(cols: list[Col]) -> pa.Schema:
    """An all-STRING arrow schema carrying the architecture's column order.

    Staging is all-STRING by house convention and ``dump_header`` stringifies the
    header regardless, so the schema fixes order, not types.
    """
    return pa.schema([(c.name, pa.string()) for c in cols])


def _write_parquet(rows: list[dict], cols: list[Col], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    arrays = [
        pa.array([r.get(c.name) or None for r in rows], type=pa.string())
        for c in cols
    ]
    table = pa.Table.from_arrays(arrays, schema=_string_schema(cols))
    pq.write_table(table, path, compression="snappy")
    return table.num_rows


def write_partition(
    rows: list[dict], cols: list[Col], out_dir: Path, year: int
) -> int:
    """Write one ``year=<Y>/data.parquet`` partition. Returns the row count.

    An empty year writes **no file at all**: ``gcs.dump_header`` builds the
    staging table's schema from the first file it finds, and a zero-row parquet
    gives BigQuery nothing to go on, after which every real partition fails to
    read.
    """
    if not rows:
        return 0
    return _write_parquet(
        rows, cols, out_dir / f"year={year}" / "data.parquet"
    )


def write_flat(rows: list[dict], cols: list[Col], out_dir: Path) -> int:
    """Write an unpartitioned ``data.parquet``. Returns the row count."""
    if not rows:
        return 0
    return _write_parquet(rows, cols, out_dir / "data.parquet")


# --------------------------------------------------------------------------
# Per-year cleaning
# --------------------------------------------------------------------------


def clean_project_year(
    year: int,
    input_dir: Path,
    output_dir: Path,
    supplement: dict | None = None,
) -> int:
    """Clean one fiscal year of the project file into a parquet partition."""
    src = input_dir / source_filename("projects", year)
    sup = supplement if supplement is not None else {}
    rows = []
    for row in read_rows(src):
        aid = _s(row, "APPLICATION_ID")
        rows.append(clean_project(row, sup.get(aid)))
    return write_partition(
        rows, load_cols("project"), output_dir / "project", year
    )


def clean_abstract_year(year: int, input_dir: Path, output_dir: Path) -> int:
    """Clean one fiscal year of the abstract file into a parquet partition."""
    src = input_dir / source_filename("abstracts", year)
    rows = [clean_project_abstract(r, year) for r in read_rows(src)]
    return write_partition(
        rows,
        load_cols("project_abstract"),
        output_dir / "project_abstract",
        year,
    )


def clean_publication_year(
    year: int, input_dir: Path, output_dir: Path
) -> int:
    """Clean one calendar year of the publication file into a parquet partition."""
    src = input_dir / source_filename("publications", year)
    rows = [clean_publication(r, year) for r in read_rows(src)]
    return write_partition(
        rows, load_cols("publication"), output_dir / "publication", year
    )


def clean_publication_link_year(
    year: int, input_dir: Path, output_dir: Path
) -> int:
    """Clean one calendar year of the link table into a parquet partition."""
    src = input_dir / source_filename("linktables", year)
    rows = [clean_publication_link(r, year) for r in read_rows(src)]
    return write_partition(
        rows,
        load_cols("publication_link"),
        output_dir / "publication_link",
        year,
    )


def clean_patents(input_dir: Path, output_dir: Path) -> int:
    """Clean the all-years patent file.

    36 of the 92,936 published rows repeat a ``(patent_id, core_project_num)``
    pair, and every one of them carries the same patent title under two
    different owners — the same patent, supported by the same project, reported
    by two institutions. That is real published data, so the pair is not the
    table's key: the owner belongs in it, and
    ``(patent_id, core_project_num, patent_org_name)`` is unique on all 92,936
    rows. Only rows identical in every field are collapsed here.
    """
    src = input_dir / source_filename("patents", None)
    seen: set[tuple] = set()
    rows = []
    for r in read_rows(src):
        row = clean_patent_link(r)
        key = tuple(sorted(row.items()))
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return write_flat(
        rows, load_cols("patent_link"), output_dir / "patent_link"
    )


def clean_clinical_studies(input_dir: Path, output_dir: Path) -> int:
    """Clean the all-years clinical studies file."""
    src = input_dir / source_filename("clinicalstudies", None)
    seen: set[tuple] = set()
    rows = []
    for r in read_rows(src):
        row = clean_clinical_study_link(r)
        key = tuple(sorted(row.items()))
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return write_flat(
        rows,
        load_cols("clinical_study_link"),
        output_dir / "clinical_study_link",
    )


def clean_all(
    input_dir: Path,
    output_dir: Path,
    fiscal_years: list[int],
    calendar_years: list[int],
    include_all_year_tables: bool = True,
) -> dict[str, int]:
    """Clean the given years. Returns ``{table: total_rows}``."""
    totals: dict[str, int] = defaultdict(int)
    supplement = load_funding_supplement(input_dir)
    for year in fiscal_years:
        totals["project"] += clean_project_year(
            year, input_dir, output_dir, supplement
        )
        totals["project_abstract"] += clean_abstract_year(
            year, input_dir, output_dir
        )
    for year in calendar_years:
        totals["publication"] += clean_publication_year(
            year, input_dir, output_dir
        )
        totals["publication_link"] += clean_publication_link_year(
            year, input_dir, output_dir
        )
    if include_all_year_tables:
        totals["patent_link"] += clean_patents(input_dir, output_dir)
        totals["clinical_study_link"] += clean_clinical_studies(
            input_dir, output_dir
        )
    return dict(totals)


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------


def load_activity_code_labels() -> dict[str, str]:
    """Activity code -> official NIH title, from the committed reference CSV.

    ``grants.nih.gov/grants/funding/ac_search_results.htm`` answers 403 to every
    scripted request — plain requests, a browser user agent and a TLS-impersonating
    client alike — while serving the same page to a browser session. The list is
    therefore committed rather than fetched at run time.
    """
    path = Path(constants.REFERENCE_DIR.value) / "activity_codes.csv"
    out = {}
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            code = r["activity_code"].strip()
            title = r["title"].strip()
            category = r["funding_category"].strip()
            if code:
                out[code] = f"{title} ({category})" if category else title
    return out


def build_dicionario(output_dir: Path) -> int:
    """Build the dicionario from the cleaned project partitions plus references.

    Four columns of ``project`` store codes a reader cannot interpret unaided.
    Two have published label sets (``application_type`` and ``arra_funded``, from
    the ExPORTER data dictionary), one has an official register that is not
    machine-fetchable (``activity``), and one carries its label in a sibling
    column of the data itself (``administering_ic`` -> ``ic_name``), from which
    the most frequent spelling is taken because 13 of the 114 institute codes
    appear under more than one.
    """
    part_dir = output_dir / "project"
    files = sorted(part_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no project partitions under {part_dir}")

    years: dict[tuple[str, str], set[int]] = defaultdict(set)
    ic_names: dict[str, Counter] = defaultdict(Counter)
    for f in files:
        year = int(f.parent.name.split("=")[1])
        # pq.ParquetFile, not pq.read_table: the latter reads the enclosing
        # year=<Y>/ directory as a hive partition and adds its own
        # dictionary-typed `year` column, which then collides with the string
        # `year` stored inside the file.
        tbl = (
            pq.ParquetFile(f)
            .read(
                columns=[
                    "application_type",
                    "arra_funded",
                    "activity",
                    "administering_ic",
                    "ic_name",
                ]
            )
            .to_pydict()
        )
        for col in (
            "application_type",
            "arra_funded",
            "activity",
            "administering_ic",
        ):
            for v in tbl[col]:
                if v:
                    years[(col, v)].add(year)
        for ic, name in zip(
            tbl["administering_ic"], tbl["ic_name"], strict=True
        ):
            if ic and name:
                ic_names[ic][name] += 1

    activity_labels = load_activity_code_labels()
    labels = {
        "application_type": constants.APPLICATION_TYPE_LABELS.value,
        "arra_funded": constants.ARRA_FUNDED_LABELS.value,
        "activity": activity_labels,
        "administering_ic": {
            k: v.most_common(1)[0][0] for k, v in ic_names.items()
        },
    }

    rows = []
    for (col, key), ys in sorted(years.items()):
        rows.append(
            {
                "id_tabela": "project",
                "nome_coluna": col,
                "chave": key,
                "cobertura_temporal": _coverage(ys),
                "valor": labels[col].get(key, ""),
            }
        )
    return write_flat(rows, load_cols("dicionario"), output_dir / "dicionario")


def _coverage(years: set[int]) -> str:
    """``START(1)END`` for a set of years, or ``YEAR(1)YEAR`` for a single one."""
    if not years:
        return ""
    return f"{min(years)}(1){max(years)}"


# --------------------------------------------------------------------------
# Coverage and verification
# --------------------------------------------------------------------------


def probe_file(
    family: str, year: int | None, session: requests.Session | None = None
) -> dict | None:
    """Publication metadata for one ExPORTER file, without downloading it.

    ``/exporter/<family>/download/<year>`` answers 302 to a tokenised URL on the
    NIH document service when the file exists and 400 when it does not, and the
    document service answers HEAD with ``Last-Modified`` and ``Content-Length``.
    Two cheap requests therefore establish both existence and publication date.

    Neither shortcut works on the ``reporter.nih.gov`` route itself: HEAD there
    answers 405 for every year, valid or not, and a ranged GET is ignored and
    streams the whole file. The ExPORTER listing page is a JavaScript
    application with no server-rendered file list, so there is nothing to
    scrape either.

    Returns ``None`` when the file does not exist.
    """
    sess = session or requests.Session()
    resp = sess.get(
        source_url(family, year), timeout=120, allow_redirects=False
    )
    if resp.status_code != 302:
        return None
    location = resp.headers.get("Location")
    if not location:
        return None
    head = sess.head(location, timeout=120)
    if head.status_code != 200:
        return None
    modified = head.headers.get("Last-Modified", "")
    try:
        stamp = time.strftime(
            "%Y-%m-%d", time.strptime(modified, "%a, %d %b %Y %H:%M:%S %Z")
        )
    except ValueError:
        stamp = ""
    return {
        "family": family,
        "year": year,
        "last_modified": stamp,
        "size": int(head.headers.get("Content-Length") or 0),
    }


def list_source_files(
    families: list[str] | None = None, session: requests.Session | None = None
) -> dict:
    """Probe the published ExPORTER files. Keys are ``"<family>|<year>"``.

    ``year`` is the empty string for the two families that ship a single
    all-fiscal-years file. String keys, because Prefect serializes task results
    and tuple keys do not survive the round trip.
    """
    sess = session or requests.Session()
    wanted = families or list(constants.FAMILIES.value)
    out: dict[str, dict] = {}
    this_year = time.gmtime().tm_year + 1
    for family in wanted:
        kind = constants.FAMILIES.value[family][2]
        if kind == "none":
            info = probe_file(family, None, sess)
            if info:
                out[f"{family}|"] = info
            continue
        first = (
            constants.FIRST_FISCAL_YEAR.value
            if kind == "fiscal"
            else constants.FIRST_CALENDAR_YEAR.value
        )
        for year in range(first, this_year + 1):
            info = probe_file(family, year, sess)
            if info:
                out[f"{family}|{year}"] = info
    return out


def years_in(listing: dict, family: str) -> list[int]:
    """Sorted years present in a listing for one family."""
    return sorted(
        int(k.split("|")[1])
        for k in listing
        if k.startswith(f"{family}|") and k.split("|")[1]
    )


def source_max_date(listing: dict) -> str:
    """The most recent publication date across every probed file, ``YYYY-MM-DD``.

    This is a **publication timestamp**, not a coverage date: it is the
    ``Last-Modified`` of the newest file NIH has rewritten. The poll therefore
    compares it against ``Table.Update.latest`` — when we last materialised —
    rather than against the table's coverage, which measures fiscal years and
    would never move for a release that only restates earlier years.
    """
    stamps = [
        v["last_modified"] for v in listing.values() if v.get("last_modified")
    ]
    return max(stamps) if stamps else ""


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
