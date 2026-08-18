"""Pure download and cleaning transform for us_fec_campaign_finance.

No Prefect imports: the recurring pipeline in pipelines/datasets/us_fec_campaign_finance
imports these functions rather than duplicating them
(.claude/rules/prefect-pipeline-conventions.md, "DRY with the onboarding code").

The FEC publishes one ZIP per file type per two-year election cycle, each holding a
single pipe-delimited text file with no header row. Layouts are documented and stable;
the only per-file quirks are:

* ``oppexp`` carries a trailing delimiter, so every line has 26 fields for 25 documented
  columns. The 26th is dropped.
* ``oppexp`` dates are ``MM/DD/YYYY``; every other transaction file uses ``MMDDYYYY``.
* Files are not quoted and contain stray double quotes inside names, so they must be
  parsed with QUOTE_NONE or lines get swallowed.
* Encoding is latin-1, not UTF-8.

Output is hive-partitioned all-STRING parquet under ``output/<table>/cycle=<YYYY>/``.
All-STRING is deliberate: staging is all-STRING by house convention, the dbt model
safe_casts every column, and ``gcs.py::dump_header`` stringifies the pipeline's staging
header anyway, so typed parquet would be rejected there
(.claude/rules/bigquery-conventions.md).
"""

from __future__ import annotations

import csv
import os
import shutil
import time
import zipfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_fec_campaign_finance.constants import constants

BULK_BASE = constants.BULK_BASE.value
USER_AGENT = constants.USER_AGENT.value
ARCHITECTURE_DIR = constants.ARCHITECTURE_DIR.value

DATA_DIR = Path(
    os.environ.get(
        "FEC_DATA_DIR",
        Path.home() / "Downloads" / "us_fec_campaign_finance_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

CHUNK_ROWS = 1_000_000
FIRST_CYCLE = 1980
LAST_CYCLE = 2026


def cycles(first: int = FIRST_CYCLE, last: int = LAST_CYCLE) -> list[int]:
    """Every even year from `first` to `last` — one FEC election cycle each."""
    return list(range(first, last + 1, 2))


# --------------------------------------------------------------------------- #
# File specifications
# --------------------------------------------------------------------------- #

TXN_SOURCE = [
    "CMTE_ID",
    "AMNDT_IND",
    "RPT_TP",
    "TRANSACTION_PGI",
    "IMAGE_NUM",
    "TRANSACTION_TP",
    "ENTITY_TP",
    "NAME",
    "CITY",
    "STATE",
    "ZIP_CODE",
    "EMPLOYER",
    "OCCUPATION",
    "TRANSACTION_DT",
    "TRANSACTION_AMT",
    "OTHER_ID",
    "TRAN_ID",
    "FILE_NUM",
    "MEMO_CD",
    "MEMO_TEXT",
    "SUB_ID",
]


def txn_rename(prefix: str) -> dict[str, str]:
    return {
        "CMTE_ID": "committee_id",
        "AMNDT_IND": "amendment_indicator",
        "RPT_TP": "report_type",
        "TRANSACTION_PGI": "election_type_year",
        "IMAGE_NUM": "image_number",
        "TRANSACTION_TP": "transaction_type",
        "ENTITY_TP": "entity_type",
        "NAME": f"{prefix}_name",
        "CITY": f"{prefix}_city",
        "STATE": f"{prefix}_state",
        "ZIP_CODE": f"{prefix}_zip_code",
        "EMPLOYER": f"{prefix}_employer",
        "OCCUPATION": f"{prefix}_occupation",
        "TRANSACTION_DT": "transaction_date",
        "TRANSACTION_AMT": "transaction_amount",
        "OTHER_ID": "other_id",
        "TRAN_ID": "transaction_id",
        "FILE_NUM": "file_number",
        "MEMO_CD": "memo_code",
        "MEMO_TEXT": "memo_text",
        "SUB_ID": "sub_id",
        "CAND_ID": "candidate_id",
    }


@dataclass(frozen=True)
class FileSpec:
    """One FEC bulk file type, and how to turn it into one Data Basis table."""

    table: str
    prefix: str  # zip basename prefix, e.g. "indiv" -> indiv26.zip
    member: str  # the single text file inside the zip
    source_columns: list[str]
    rename: dict[str, str]
    date_format: str | None = None
    extra_fields: int = 0  # undocumented trailing fields to drop
    first_cycle: int = FIRST_CYCLE
    date_columns: tuple[str, ...] = field(default=("transaction_date",))

    def url(self, cycle: int) -> str:
        return f"{BULK_BASE}/{cycle}/{self.prefix}{cycle % 100:02d}.zip"

    def zip_path(self, cycle: int) -> Path:
        return INPUT / f"{self.prefix}{cycle % 100:02d}.zip"

    @property
    def field_count(self) -> int:
        return len(self.source_columns) + self.extra_fields

    @property
    def read_names(self) -> list[str]:
        return self.source_columns + [
            f"_unused_{i}" for i in range(self.extra_fields)
        ]


SPECS: dict[str, FileSpec] = {
    "candidate": FileSpec(
        table="candidate",
        prefix="cn",
        member="cn.txt",
        source_columns=[
            "CAND_ID",
            "CAND_NAME",
            "CAND_PTY_AFFILIATION",
            "CAND_ELECTION_YR",
            "CAND_OFFICE_ST",
            "CAND_OFFICE",
            "CAND_OFFICE_DISTRICT",
            "CAND_ICI",
            "CAND_STATUS",
            "CAND_PCC",
            "CAND_ST1",
            "CAND_ST2",
            "CAND_CITY",
            "CAND_ST",
            "CAND_ZIP",
        ],
        rename={
            "CAND_ID": "candidate_id",
            "CAND_NAME": "candidate_name",
            "CAND_PTY_AFFILIATION": "party",
            "CAND_ELECTION_YR": "election_year",
            "CAND_OFFICE_ST": "office_state",
            "CAND_OFFICE": "office",
            "CAND_OFFICE_DISTRICT": "office_district",
            "CAND_ICI": "incumbent_challenger_status",
            "CAND_STATUS": "candidate_status",
            "CAND_PCC": "principal_committee_id",
            "CAND_ST1": "address_1",
            "CAND_ST2": "address_2",
            "CAND_CITY": "city",
            "CAND_ST": "state",
            "CAND_ZIP": "zip_code",
        },
        date_columns=(),
    ),
    "committee": FileSpec(
        table="committee",
        prefix="cm",
        member="cm.txt",
        source_columns=[
            "CMTE_ID",
            "CMTE_NM",
            "TRES_NM",
            "CMTE_ST1",
            "CMTE_ST2",
            "CMTE_CITY",
            "CMTE_ST",
            "CMTE_ZIP",
            "CMTE_DSGN",
            "CMTE_TP",
            "CMTE_PTY_AFFILIATION",
            "CMTE_FILING_FREQ",
            "ORG_TP",
            "CONNECTED_ORG_NM",
            "CAND_ID",
        ],
        rename={
            "CMTE_ID": "committee_id",
            "CMTE_NM": "committee_name",
            "TRES_NM": "treasurer_name",
            "CMTE_ST1": "address_1",
            "CMTE_ST2": "address_2",
            "CMTE_CITY": "city",
            "CMTE_ST": "state",
            "CMTE_ZIP": "zip_code",
            "CMTE_DSGN": "committee_designation",
            "CMTE_TP": "committee_type",
            "CMTE_PTY_AFFILIATION": "party",
            "CMTE_FILING_FREQ": "filing_frequency",
            "ORG_TP": "organization_type",
            "CONNECTED_ORG_NM": "connected_organization_name",
            "CAND_ID": "candidate_id",
        },
        date_columns=(),
    ),
    "candidate_committee_link": FileSpec(
        table="candidate_committee_link",
        prefix="ccl",
        member="ccl.txt",
        source_columns=[
            "CAND_ID",
            "CAND_ELECTION_YR",
            "FEC_ELECTION_YR",
            "CMTE_ID",
            "CMTE_TP",
            "CMTE_DSGN",
            "LINKAGE_ID",
        ],
        rename={
            "CAND_ID": "candidate_id",
            "CAND_ELECTION_YR": "candidate_election_year",
            "FEC_ELECTION_YR": "fec_election_year",
            "CMTE_ID": "committee_id",
            "CMTE_TP": "committee_type",
            "CMTE_DSGN": "committee_designation",
            "LINKAGE_ID": "linkage_id",
        },
        first_cycle=2000,
        date_columns=(),
    ),
    "contribution_individual": FileSpec(
        table="contribution_individual",
        prefix="indiv",
        member="itcont.txt",
        source_columns=TXN_SOURCE,
        rename=txn_rename("contributor"),
        date_format="%m%d%Y",
    ),
    "contribution_committee": FileSpec(
        table="contribution_committee",
        prefix="pas2",
        member="itpas2.txt",
        source_columns=[*TXN_SOURCE[:16], "CAND_ID", *TXN_SOURCE[16:]],
        rename=txn_rename("contributor"),
        date_format="%m%d%Y",
    ),
    "committee_transaction": FileSpec(
        table="committee_transaction",
        prefix="oth",
        member="itoth.txt",
        source_columns=TXN_SOURCE,
        rename=txn_rename("counterparty"),
        date_format="%m%d%Y",
    ),
    "disbursement": FileSpec(
        table="disbursement",
        prefix="oppexp",
        member="oppexp.txt",
        source_columns=[
            "CMTE_ID",
            "AMNDT_IND",
            "RPT_YR",
            "RPT_TP",
            "IMAGE_NUM",
            "LINE_NUM",
            "FORM_TP_CD",
            "SCHED_TP_CD",
            "NAME",
            "CITY",
            "STATE",
            "ZIP_CODE",
            "TRANSACTION_DT",
            "TRANSACTION_AMT",
            "TRANSACTION_PGI",
            "PURPOSE",
            "CATEGORY",
            "CATEGORY_DESC",
            "MEMO_CD",
            "MEMO_TEXT",
            "ENTITY_TP",
            "SUB_ID",
            "FILE_NUM",
            "TRAN_ID",
            "BACK_REF_TRAN_ID",
        ],
        rename={
            "CMTE_ID": "committee_id",
            "AMNDT_IND": "amendment_indicator",
            "RPT_YR": "report_year",
            "RPT_TP": "report_type",
            "IMAGE_NUM": "image_number",
            "LINE_NUM": "line_number",
            "FORM_TP_CD": "form_type",
            "SCHED_TP_CD": "schedule_type",
            "NAME": "payee_name",
            "CITY": "payee_city",
            "STATE": "payee_state",
            "ZIP_CODE": "payee_zip_code",
            "TRANSACTION_DT": "transaction_date",
            "TRANSACTION_AMT": "transaction_amount",
            "TRANSACTION_PGI": "election_type_year",
            "PURPOSE": "purpose",
            "CATEGORY": "category",
            "CATEGORY_DESC": "category_description",
            "MEMO_CD": "memo_code",
            "MEMO_TEXT": "memo_text",
            "ENTITY_TP": "entity_type",
            "SUB_ID": "sub_id",
            "FILE_NUM": "file_number",
            "TRAN_ID": "transaction_id",
            "BACK_REF_TRAN_ID": "back_reference_transaction_id",
        },
        date_format="%m/%d/%Y",
        extra_fields=1,
        first_cycle=2004,
    ),
}


# --------------------------------------------------------------------------- #
# Architecture
# --------------------------------------------------------------------------- #


def architecture_columns(table: str) -> list[str]:
    """Column order for `table`, read from the architecture CSV (source of truth)."""
    path = ARCHITECTURE_DIR / f"{table}.csv"
    with path.open(encoding="utf-8") as fh:
        return [row["name"] for row in csv.DictReader(fh)]


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #


DOWNLOAD_ATTEMPTS = 6


def download(
    spec: FileSpec, cycle: int, *, force: bool = False
) -> Path | None:
    """Fetch one cycle's ZIP. Returns None when the FEC does not publish it.

    The FEC's S3 front end resets long-running connections, and the largest archive
    (indiv20, 5.6 GB) takes long enough that this happens routinely. Partial bytes are
    kept in a .part file and the next attempt resumes with a Range request rather than
    restarting the transfer.
    """
    dest = spec.zip_path(cycle)
    if dest.exists() and not force:
        return dest
    INPUT.mkdir(parents=True, exist_ok=True)
    url = spec.url(cycle)
    tmp = dest.with_suffix(".zip.part")

    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        have = tmp.stat().st_size if tmp.exists() else 0
        headers = {"User-Agent": USER_AGENT}
        if have:
            headers["Range"] = f"bytes={have}-"
        try:
            with requests.get(
                url, stream=True, timeout=(30, 300), headers=headers
            ) as resp:
                if resp.status_code == 404:
                    tmp.unlink(missing_ok=True)
                    return None
                if have and resp.status_code == 200:
                    # Server ignored the Range header — start over rather than
                    # appending a second copy of the file to the partial one.
                    have = 0
                    tmp.unlink(missing_ok=True)
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", 0)) + have
                with tmp.open("ab" if have else "wb") as fh:
                    for block in resp.iter_content(chunk_size=8 << 20):
                        fh.write(block)
            if total and tmp.stat().st_size != total:
                raise OSError(
                    f"short read: {tmp.stat().st_size} of {total} bytes"
                )
            tmp.replace(dest)
            return dest
        except (requests.RequestException, OSError) as exc:
            if attempt == DOWNLOAD_ATTEMPTS:
                raise
            wait = min(60, 2**attempt)
            print(
                f"  download {spec.prefix}{cycle % 100:02d} attempt {attempt} failed "
                f"({type(exc).__name__}); resuming in {wait}s",
                flush=True,
            )
            time.sleep(wait)
    return None


# --------------------------------------------------------------------------- #
# Clean
# --------------------------------------------------------------------------- #


# Columns whose values are FEC codes joined against the dicionario table. Whitespace
# here is not cosmetic — it breaks the join — so these get stripped on the way through.
CODED_COLUMNS = {
    "amendment_indicator",
    "report_type",
    "transaction_type",
    "entity_type",
    "memo_code",
    "party",
    "office",
    "incumbent_challenger_status",
    "candidate_status",
    "committee_designation",
    "committee_type",
    "filing_frequency",
    "organization_type",
    "category",
    "candidate_id",
    "committee_id",
    "other_id",
    "principal_committee_id",
    "office_state",
    "state",
}


def _to_string_frame(
    chunk: pd.DataFrame, spec: FileSpec, cycle: int, columns: list[str]
) -> pd.DataFrame:
    chunk = chunk.rename(columns=spec.rename)
    chunk["cycle"] = str(cycle)

    for date_col in spec.date_columns:
        if date_col in chunk.columns and spec.date_format:
            parsed = pd.to_datetime(
                chunk[date_col], format=spec.date_format, errors="coerce"
            )
            # ISO-8601 strings, so safe_cast(x as date) resolves in BigQuery.
            chunk[date_col] = parsed.dt.strftime("%Y-%m-%d")

    missing = [c for c in columns if c not in chunk.columns]
    if missing:
        raise ValueError(
            f"{spec.table}: architecture columns absent after rename: {missing}"
        )

    # read_csv(dtype=str) already turns empty fields into NaN, which pyarrow writes as
    # NULL. Whitespace-only fields survive that, and would break dictionary joins on
    # the coded columns, so strip those specifically — a full-frame regex replace over
    # ~70M rows is far too slow to justify for the free-text columns.
    for coded in CODED_COLUMNS.intersection(chunk.columns):
        chunk[coded] = chunk[coded].str.strip().replace("", None)

    return chunk[columns]


def clean_cycle(spec: FileSpec, cycle: int, *, quiet: bool = False) -> int:
    """Parse one cycle's ZIP into output/<table>/cycle=<YYYY>/data.parquet.

    Returns the number of rows written. Streams the member file in chunks so peak
    memory stays near one chunk regardless of the file's size — indiv20 alone is
    ~28 GB uncompressed.
    """
    zip_path = spec.zip_path(cycle)
    if not zip_path.exists():
        return 0

    columns = architecture_columns(spec.table)
    schema = pa.schema([(c, pa.string()) for c in columns])
    dest_dir = OUTPUT / spec.table / f"cycle={cycle}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "data.parquet"

    total = 0
    writer = None
    try:
        with zipfile.ZipFile(zip_path) as zf:
            member = (
                spec.member
                if spec.member in zf.namelist()
                else zf.namelist()[0]
            )
            with zf.open(member) as raw:
                reader = pd.read_csv(
                    raw,
                    sep="|",
                    header=None,
                    names=spec.read_names,
                    dtype=str,
                    encoding="latin-1",
                    quoting=csv.QUOTE_NONE,
                    on_bad_lines="skip",
                    chunksize=CHUNK_ROWS,
                    low_memory=False,
                )
                for chunk in reader:
                    frame = _to_string_frame(chunk, spec, cycle, columns)
                    table = pa.Table.from_pandas(
                        frame, schema=schema, preserve_index=False
                    )
                    if writer is None:
                        writer = pq.ParquetWriter(
                            dest, schema, compression="snappy"
                        )
                    writer.write_table(table)
                    total += len(frame)
    finally:
        if writer is not None:
            writer.close()

    if total == 0:
        dest.unlink(missing_ok=True)
        dest_dir.rmdir()
    elif not quiet:
        print(f"  {spec.table:26s} cycle={cycle}  {total:>12,} rows")
    return total


def write_header_stub(table: str) -> None:
    """Prepend a 0-row parquet to the earliest partition of a large table.

    The GitHub table-approve action's save_header_files does pd.read_parquet on the
    *first* parquet file of a staging table; on a multi-hundred-MB file that OOMs CI
    and prod materialization never runs. "00_header.parquet" sorts ahead of
    "data.parquet", so it is read instead.
    """
    partitions = sorted((OUTPUT / table).glob("cycle=*"))
    if not partitions:
        return
    first = partitions[0]
    data = first / "data.parquet"
    if not data.exists():
        return
    schema = pq.ParquetFile(data).schema_arrow
    pq.write_table(
        pa.Table.from_pylist([], schema=schema),
        first / "00_header.parquet",
        compression="snappy",
    )


def clean_all(
    tables: list[str] | None = None,
    first: int = FIRST_CYCLE,
    last: int = LAST_CYCLE,
    *,
    keep_zip: bool = False,
) -> dict[str, int]:
    """Download and clean every requested table across every cycle.

    Processes one (table, cycle) at a time and deletes the ZIP right after, so peak
    disk stays near a single archive rather than the full 22 GB.
    """
    selected = tables or list(SPECS)
    counts: dict[str, int] = {}
    for name in selected:
        spec = SPECS[name]
        rows = 0
        for cycle in cycles(max(first, spec.first_cycle), last):
            path = download(spec, cycle)
            if path is None:
                continue
            try:
                rows += clean_cycle(spec, cycle)
            finally:
                if not keep_zip:
                    path.unlink(missing_ok=True)
        write_header_stub(name)
        counts[name] = rows
        print(f"{name:26s} TOTAL {rows:>14,} rows")
    return counts


def purge_scratch() -> None:
    """Delete the whole scratch tree — step 14 of the onboarding workflow."""
    shutil.rmtree(DATA_DIR, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Recurring refresh
# --------------------------------------------------------------------------- #


def current_cycle(today: date) -> int:
    """The FEC election cycle `today` falls in — the next even year, inclusive.

    2025 and 2026 both belong to the 2026 cycle.
    """
    return today.year if today.year % 2 == 0 else today.year + 1


def refresh_cycle(
    cycle: int, work_dir: Path, tables: list[str]
) -> dict[str, object]:
    """Download and clean one cycle into `work_dir`, ready for upload.

    Returns ``{table: <dir to upload>}`` plus ``max_date``, the latest
    transaction date seen across the refreshed tables — the source's max coverage
    date, which is what the raw-source Update records.

    Each table's payload is ``work_dir/<table>/cycle=<CYCLE>/data.parquet``. The
    *table* directory is handed to ``upload_to_gcs`` so the hive prefix survives:
    the blob lands at ``staging/<ds>/<table>/cycle=<CYCLE>/data.parquet``,
    replacing exactly that partition and leaving every frozen cycle untouched.
    That is why the flow uses ``dump_mode="append"`` — "overwrite" would delete
    the whole staging table (and, via ``tb.delete(mode="all")``, the prod table)
    and the history with it.
    """
    global INPUT, OUTPUT
    previous_input, previous_output = INPUT, OUTPUT
    INPUT = work_dir / "input"
    OUTPUT = work_dir / "output"
    INPUT.mkdir(parents=True, exist_ok=True)

    result: dict[str, object] = {}
    max_date = None
    try:
        for table in tables:
            spec = SPECS[table]
            if cycle < spec.first_cycle:
                continue
            path = download(spec, cycle, force=True)
            if path is None:
                continue
            try:
                rows = clean_cycle(spec, cycle)
            finally:
                path.unlink(missing_ok=True)
            if not rows:
                continue
            result[table] = str(OUTPUT / table)
            if spec.date_columns:
                seen = _max_transaction_date(OUTPUT / table / f"cycle={cycle}")
                if seen and (max_date is None or seen > max_date):
                    max_date = seen
    finally:
        INPUT, OUTPUT = previous_input, previous_output

    result["max_date"] = max_date
    return result


def _max_transaction_date(partition: Path) -> str | None:
    """Largest non-null transaction_date in a cleaned partition, as YYYY-MM-DD.

    Dates are stored as ISO strings, so a lexicographic max is the calendar max.
    Filers do enter impossible future dates, so anything beyond today is ignored
    rather than allowed to push the recorded source coverage into the future.
    """
    today = date.today().isoformat()
    best = None
    for file in sorted(partition.glob("*.parquet")):
        table = pq.read_table(file, columns=["transaction_date"])
        column = table.column("transaction_date")
        for value in column.to_pylist():
            if value and value <= today and (best is None or value > best):
                best = value
    return best
