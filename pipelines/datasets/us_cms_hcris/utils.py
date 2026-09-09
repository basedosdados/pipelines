"""Pure download and cleaning functions for us_cms_hcris. No Prefect imports.

The one-shot onboarding under ``models/us_cms_hcris/code/`` imports these, so the
bootstrap and the recurring flow can never drift.

The transform is deliberately thin. HCRIS is already normalised: a report index
plus two long cell tables keyed on (report, worksheet, line, column). This module
reshapes it as little as possible —

* ``report`` — the RPT record, one row per report, with the dates parsed to ISO
  and a ``year`` partition taken from the **fiscal year end date**.
* ``report_value`` — NMRC and ALPHA unioned into one long table, the numeric and
  the text value in separate columns, carrying the same ``year`` partition as
  the report it belongs to.

Everything else — the named financial measures — is derived in dbt from
``report_value``, so the worksheet/line/column provenance of each measure is
literal rather than buried in Python.

Both tables are written **all-STRING**, per the house staging convention: the
dbt models ``safe_cast`` every column to its architecture type. See
``.claude/rules/bigquery-conventions.md``.
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

import duckdb
import pyarrow as pa
import requests

from pipelines.datasets.us_cms_hcris.constants import constants

FORMS: dict = constants.FORMS.value
DOWNLOAD_BASE: str = constants.DOWNLOAD_BASE.value
RPT_COLUMNS: list[str] = constants.RPT_COLUMNS.value

# CMS serves these without any bot check, but a bare urllib UA occasionally
# draws a 403 from the CDN.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

SSA_STATE_CODES = Path(__file__).with_name("ssa_state_codes.csv")

REPORT_COLUMNS = [
    "year",
    "report_id",
    "provider_ccn",
    "state_id",
    "state_abbreviation",
    "npi",
    "form_version",
    "source_extract_year",
    "provider_control_type_code",
    "report_status_code",
    "fiscal_year_begin_date",
    "fiscal_year_end_date",
    "fiscal_year_days",
    "process_date",
    "initial_report_indicator",
    "last_report_indicator",
    "transmittal_number",
    "fiscal_intermediary_number",
    "adr_vendor_code",
    "fiscal_intermediary_create_date",
    "utilization_code",
    "notice_of_program_reimbursement_date",
    "special_indicator",
    "fiscal_intermediary_receipt_date",
]

REPORT_VALUE_COLUMNS = [
    "year",
    "report_id",
    "provider_ccn",
    "form_version",
    "worksheet_code",
    "line_number",
    "column_number",
    "numeric_value",
    "alpha_value",
]


def zip_url(form: str, year: int) -> str:
    """Return the CMS download URL of one fiscal-year extract.

    Args:
        form: Form version, ``"2552-96"`` or ``"2552-10"``.
        year: Federal fiscal year of the extract.

    Returns:
        The absolute URL of the archive.
    """
    return f"{DOWNLOAD_BASE}/{FORMS[form]['zip'].format(year=year)}"


def extract_exists(
    form: str, year: int, session: requests.Session | None = None
) -> bool:
    """Report whether CMS publishes an extract for this form and year.

    Args:
        form: Form version.
        year: Federal fiscal year of the extract.
        session: Optional session to reuse.

    Returns:
        True when the archive responds 200 to a HEAD request.
    """
    get = (session or requests).head
    try:
        return (
            get(
                zip_url(form, year),
                headers=HEADERS,
                allow_redirects=True,
                timeout=60,
            ).status_code
            == 200
        )
    except requests.RequestException:
        return False


def list_extracts(
    last_year: int, session: requests.Session | None = None
) -> list[tuple[str, int]]:
    """Enumerate every published extract up to ``last_year``.

    Probed rather than hardcoded: CMS adds a new federal fiscal year every
    October, and the 2552-96 series ended at a year the code should discover
    rather than assert.

    Args:
        last_year: Highest federal fiscal year to probe.
        session: Optional session to reuse.

    Returns:
        ``(form, year)`` pairs in ascending order, 2552-96 before 2552-10.
    """
    session = session or requests.Session()
    out: list[tuple[str, int]] = []
    for form, spec in FORMS.items():
        for year in range(spec["first_year"], last_year + 1):
            if extract_exists(form, year, session):
                out.append((form, year))
    return out


def download_extract(form: str, year: int, dest_dir: Path) -> Path:
    """Download one fiscal-year archive, skipping it when already present.

    Written to a ``.part`` file and renamed, so an interrupted run never leaves
    a truncated archive that a later run would treat as complete.

    Args:
        form: Form version.
        year: Federal fiscal year of the extract.
        dest_dir: Directory to write into.

    Returns:
        Path of the downloaded archive.

    Raises:
        RuntimeError: If the download is short of the advertised length.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / FORMS[form]["zip"].format(year=year)
    if target.exists() and target.stat().st_size > 0:
        return target

    part = target.with_suffix(target.suffix + ".part")
    with requests.get(
        zip_url(form, year), headers=HEADERS, stream=True, timeout=(30, 600)
    ) as response:
        response.raise_for_status()
        expected = int(response.headers.get("Content-Length", 0))
        response.raw.decode_content = True
        with part.open("wb") as handle:
            shutil.copyfileobj(response.raw, handle)
    if expected and part.stat().st_size != expected:
        part.unlink(missing_ok=True)
        raise RuntimeError(
            f"{target.name}: downloaded {part.stat().st_size} bytes, expected {expected}"
        )
    part.rename(target)
    return target


def unpack_extract(
    archive: Path, form: str, year: int, work_dir: Path
) -> dict[str, Path]:
    """Unpack the rpt, nmrc and alpha members of one archive.

    The v1996 archives also carry a ``rollup`` member, which is not unpacked.

    Args:
        archive: Path of the downloaded ZIP.
        form: Form version.
        year: Federal fiscal year of the extract.
        work_dir: Directory to unpack into.

    Returns:
        Mapping of part name to the unpacked CSV path.

    Raises:
        RuntimeError: If a required member is missing from the archive.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    with zipfile.ZipFile(archive) as zf:
        members = {
            name.rsplit("/", 1)[-1].lower(): name for name in zf.namelist()
        }
        for part in ("rpt", "nmrc", "alpha"):
            wanted = FORMS[form]["csv"].format(year=year, part=part).lower()
            name = members.get(wanted)
            if name is None:
                # A few archives ship the members upper-cased.
                name = next(
                    (
                        v
                        for k, v in members.items()
                        if k.endswith(f"_{part}.csv")
                    ),
                    None,
                )
            if name is None:
                raise RuntimeError(
                    f"{archive.name}: no {part} member (has {sorted(members)})"
                )
            target = work_dir / f"{part}.csv"
            with zf.open(name) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            out[part] = target
    return out


# --------------------------------------------------------------------------
# cleaning
# --------------------------------------------------------------------------

FORM_TAG = {"2552-96": "v1996", "2552-10": "v2010"}


# CMS writes every date as MM/DD/YYYY, and leaves the optional ones empty.
def _iso(col: str) -> str:
    """SQL rewriting one MM/DD/YYYY column to an ISO date string."""
    return (
        f"case when nullif(trim({col}), '') is null then null "
        f"else strftime(try_strptime(trim({col}), '%m/%d/%Y'), '%Y-%m-%d') end"
    )


def _read_csv_sql(path: Path, columns: list[str]) -> str:
    """SQL reading one headerless HCRIS CSV with every column as VARCHAR.

    Args:
        path: CSV to read.
        columns: Column names, in source order.

    Returns:
        A ``read_csv`` expression usable in a FROM clause.
    """
    spec = ", ".join(f"'{c}': 'VARCHAR'" for c in columns)
    return (
        f"read_csv('{path.as_posix()}', header = false, columns = {{{spec}}}, "
        "quote = '\"', escape = '\"', all_varchar = true, ignore_errors = false)"
    )


def connect(scratch_dir: Path) -> duckdb.DuckDBPyConnection:
    """Open a duckdb connection that spills to ``scratch_dir``, not the CWD.

    duckdb writes its out-of-core spill files to ``.tmp/`` **relative to the
    working directory**. These scripts run from inside the repository, so a
    query large enough to spill drops gigabytes of `.tmp` into the checkout --
    22 GB on one interrupted 538-million-row group-by here, which git then
    tried to hash on the next `git add`.

    Args:
        scratch_dir: Directory to spill into. Created if absent.

    Returns:
        A connection with ``temp_directory`` pinned.
    """
    scratch_dir = Path(scratch_dir)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        f"set temp_directory = '{(scratch_dir / '.duckdb_tmp').as_posix()}'"
    )
    return con


def clean_extract(
    parts: dict[str, Path],
    form: str,
    extract_year: int,
    output_dir: Path,
    con: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, int]:
    """Clean one fiscal-year extract into the two partitioned parquet tables.

    ``year`` is the calendar year the report's **fiscal year ends** in, not the
    federal fiscal year of the extract that carries the report. The two differ
    for most reports — a hospital whose fiscal year ends in June 2023 is
    published in the FY2024 extract — so an extract writes into several
    partitions, and a partition is fed by several extracts. File names encode
    the extract, so re-cleaning an extract replaces exactly its own files.

    Args:
        parts: ``{"rpt": path, "nmrc": path, "alpha": path}`` from
            :func:`unpack_extract`.
        form: Form version.
        extract_year: Federal fiscal year of the extract.
        output_dir: Root output directory; tables are written beneath it.
        con: Optional duckdb connection to reuse.

    Returns:
        Row counts written, keyed by table.

    Raises:
        RuntimeError: If any report lacks a usable fiscal year end date, or if
            a report id is not unique within the extract.
    """
    con = con or connect(Path(output_dir).parent)
    tag = f"{FORM_TAG[form]}_{extract_year}"
    output_dir = Path(output_dir)

    # The CCN is documented as ``xxyyyy``: an SSA state code and an assigned
    # provider range (HCRIS_DataDictionary.csv, PRVDR_NUM). Resolving the state
    # from those two digits works for every report in both form versions, and is
    # what links this dataset to the US geographic observation level. The address
    # on Worksheet S-2 is hospital-entered free text and is not used for it.
    con.execute(
        "create or replace temp table ssa_state as select * from "
        f"read_csv('{SSA_STATE_CODES.as_posix()}', header = true, all_varchar = true)"
    )

    con.execute(f"""
        create or replace temp table rpt as
        select
            cast(coalesce(
                year(try_strptime(trim(fy_end_dt), '%m/%d/%Y')),
                year(try_strptime(trim(fy_bgn_dt), '%m/%d/%Y'))
            ) as varchar)                                     as year,
            trim(rpt_rec_num)                                 as report_id,
            nullif(trim(prvdr_num), '')                       as provider_ccn,
            s.state_id                                        as state_id,
            s.state_abbreviation                              as state_abbreviation,
            nullif(trim(npi), '')                             as npi,
            '{form}'                                          as form_version,
            '{extract_year}'                                  as source_extract_year,
            nullif(trim(prvdr_ctrl_type_cd), '')              as provider_control_type_code,
            nullif(trim(rpt_stus_cd), '')                     as report_status_code,
            {_iso("fy_bgn_dt")}                               as fiscal_year_begin_date,
            {_iso("fy_end_dt")}                               as fiscal_year_end_date,
            cast(date_diff('day',
                try_strptime(trim(fy_bgn_dt), '%m/%d/%Y'),
                try_strptime(trim(fy_end_dt), '%m/%d/%Y')) + 1 as varchar)
                                                              as fiscal_year_days,
            {_iso("proc_dt")}                                 as process_date,
            nullif(trim(initl_rpt_sw), '')                    as initial_report_indicator,
            nullif(trim(last_rpt_sw), '')                     as last_report_indicator,
            nullif(trim(trnsmtl_num), '')                     as transmittal_number,
            nullif(trim(fi_num), '')                          as fiscal_intermediary_number,
            nullif(trim(adr_vndr_cd), '')                     as adr_vendor_code,
            {_iso("fi_creat_dt")}                             as fiscal_intermediary_create_date,
            nullif(trim(util_cd), '')                         as utilization_code,
            {_iso("npr_dt")}                                  as notice_of_program_reimbursement_date,
            nullif(trim(spec_ind), '')                        as special_indicator,
            {_iso("fi_rcpt_dt")}                              as fiscal_intermediary_receipt_date
        from {_read_csv_sql(parts["rpt"], RPT_COLUMNS)} r
        left join ssa_state s on s.ssa_state_code = substr(trim(r.prvdr_num), 1, 2)
    """)

    total, undated, distinct_ids = con.execute(
        "select count(*), count(*) filter (where year is null), count(distinct report_id) from rpt"
    ).fetchone() or (0, 0, 0)
    if undated:
        raise RuntimeError(
            f"{tag}: {undated} of {total} reports have no parseable fiscal year date"
        )
    if distinct_ids != total:
        raise RuntimeError(
            f"{tag}: report_id is not unique ({distinct_ids} of {total})"
        )

    # NMRC and ALPHA address the same cell space, so they are joined on the cell
    # key rather than unioned. That keeps (report, worksheet, line, column) a
    # genuine key, which the dbt uniqueness test depends on, and puts the two
    # value types of one cell on one row.
    con.execute(f"""
        create or replace temp table cells as
        select
            coalesce(n.rpt_rec_num, a.rpt_rec_num)  as report_id,
            coalesce(n.wksht_cd,    a.wksht_cd)     as worksheet_code,
            coalesce(n.line_num,    a.line_num)     as line_number,
            coalesce(n.clmn_num,    a.clmn_num)     as column_number,
            n.item_value                            as numeric_value,
            a.item_value                            as alpha_value
        from (select trim(rpt_rec_num) rpt_rec_num, trim(wksht_cd) wksht_cd,
                     trim(line_num) line_num, trim(clmn_num) clmn_num,
                     nullif(trim(item_value), '') item_value
              from {_read_csv_sql(parts["nmrc"], constants.CELL_COLUMNS.value)}) n
        full outer join
             (select trim(rpt_rec_num) rpt_rec_num, trim(wksht_cd) wksht_cd,
                     trim(line_num) line_num, trim(clmn_num) clmn_num,
                     nullif(trim(item_value), '') item_value
              from {_read_csv_sql(parts["alpha"], constants.CELL_COLUMNS.value)}) a
        using (rpt_rec_num, wksht_cd, line_num, clmn_num)
    """)

    written: dict[str, int] = {}
    for table, select in (
        ("report", "select " + ", ".join(REPORT_COLUMNS) + " from rpt"),
        (
            "report_value",
            """
            select r.year, c.report_id, r.provider_ccn, r.form_version,
                   c.worksheet_code, c.line_number, c.column_number,
                   c.numeric_value, c.alpha_value
            from cells c join rpt r on r.report_id = c.report_id
            """,
        ),
    ):
        (output_dir / table).mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            copy ({select}) to '{(output_dir / table).as_posix()}'
            (format parquet, partition_by (year), write_partition_columns true,
             compression snappy, filename_pattern '{table}_{tag}_{{i}}',
             overwrite_or_ignore true)
        """)
        written[table] = (
            con.execute(f"select count(*) from ({select})").fetchone() or (0,)
        )[0]
    return written


def clean_all(
    input_dir: Path,
    output_dir: Path,
    extracts: list[tuple[str, int]],
    work_dir: Path | None = None,
) -> dict[str, int]:
    """Clean every extract in turn, unpacking and deleting one at a time.

    Peak disk stays near one unpacked extract (about 1.5 GB) rather than the
    16 GB the whole series would take unpacked at once.

    Args:
        input_dir: Directory holding the downloaded archives.
        output_dir: Root output directory.
        extracts: ``(form, year)`` pairs to clean.
        work_dir: Scratch directory for unpacked CSVs; defaults to
            ``<input_dir>/_work``.

    Returns:
        Total rows written per table.
    """
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    work_dir = Path(work_dir or input_dir / "_work")
    con = connect(output_dir.parent)
    totals: dict[str, int] = {}
    for form, year in extracts:
        archive = input_dir / FORMS[form]["zip"].format(year=year)
        stage = work_dir / f"{FORM_TAG[form]}_{year}"
        try:
            parts = unpack_extract(archive, form, year, stage)
            counts = clean_extract(parts, form, year, output_dir, con=con)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
        for table, rows in counts.items():
            totals[table] = totals.get(table, 0) + rows
    return totals


def assert_all_string(path: Path) -> None:
    """Fail loudly if any parquet under ``path`` is non-string or empty.

    Staging is all-STRING by house convention, and a zero-row first partition
    poisons the schema BigQuery infers for the whole staging table.

    Args:
        path: Directory to walk.

    Raises:
        AssertionError: On a non-string column or an empty file.
    """
    import pyarrow.parquet as pq

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
                f"{file}: zero rows poisons the staging schema"
            )


# --------------------------------------------------------------------------
# freshness
# --------------------------------------------------------------------------


def source_last_modified(
    extracts: list[tuple[str, int]], session: requests.Session | None = None
) -> str:
    """Return the newest ``Last-Modified`` across the published archives, as ISO.

    HCRIS is a **revising** source, not an appending one: CMS reissues every
    fiscal-year archive each quarter as reports are received, settled, reopened
    and amended, and reports for a fiscal year keep arriving for years after it
    ends. Measured 2026-09-09, all 33 archives — 1996's included — carried the
    same ``Last-Modified`` of 2026-07-14.

    That is why the poll uses this rather than the source's maximum coverage
    date. The maximum coverage date advances roughly once a year, when the
    newest fiscal years first appear, so polling on it would report "nothing
    new" for three quarters out of four while CMS was in fact adding thousands
    of reports to years already published — a flow that completes green and
    ingests nothing, which is the failure mode ``br_ibge_ipca`` sat in for 56 of
    60 runs. The archives' publication timestamp is the honest answer to "has
    the source changed since we last refreshed", and it compares correctly
    against ``Table.Update.latest``, which is itself a wall clock.

    Args:
        extracts: ``(form, year)`` pairs to probe.
        session: Optional session to reuse.

    Returns:
        The newest publication date, ``YYYY-MM-DD``.

    Raises:
        RuntimeError: If no archive returns a usable ``Last-Modified``.
    """
    from email.utils import parsedate_to_datetime

    session = session or requests.Session()
    seen: list[str] = []
    for form, year in extracts:
        try:
            head = session.head(
                zip_url(form, year),
                headers=HEADERS,
                allow_redirects=True,
                timeout=60,
            )
        except requests.RequestException:
            continue
        stamp = head.headers.get("Last-Modified")
        if head.status_code == 200 and stamp:
            # parsedate_to_datetime, not time.strptime: %a and %b resolve
            # through LC_TIME, so a worker with a non-English locale would fail
            # to parse every header.
            seen.append(parsedate_to_datetime(stamp).date().isoformat())
    if not seen:
        raise RuntimeError("no archive returned a Last-Modified header")
    return max(seen)


def clear_staging_prefix(
    bucket_name: str, dataset_id: str, table_id: str
) -> int:
    """Delete every blob under a table's staging prefix.

    Each run rebuilds the whole history, because CMS republishes the whole
    history. Clearing the prefix first is what makes that a replacement rather
    than an overlay: a partition that needed two part files last quarter and
    one this quarter would otherwise keep the stale second file and double
    count those rows, since ``upload_to_gcs`` replaces blobs by name and never
    removes extras.

    This is deliberately **not** ``dump_mode="overwrite"``. That path calls
    ``tb.delete(mode="all")``, which drops the *production* table — from the dev
    half of the flow too, because ``bd.Table`` resolves its projects from the
    pod's config rather than from ``bucket_name``.

    Args:
        bucket_name: GCS bucket, which is also the billing project.
        dataset_id: GCP dataset id.
        table_id: Table slug.

    Returns:
        The number of blobs deleted.
    """
    from google.cloud import storage

    client = storage.Client(project=bucket_name)
    bucket = client.bucket(bucket_name, user_project=bucket_name)
    prefix = f"staging/{dataset_id}/{table_id}/"
    deleted = 0
    for blob in list(client.list_blobs(bucket, prefix=prefix)):
        blob.delete()
        deleted += 1
    return deleted
