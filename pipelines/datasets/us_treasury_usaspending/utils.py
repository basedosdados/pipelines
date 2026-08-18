"""Pure helpers for us_treasury_usaspending — download and cleaning transform.

No Prefect imports here: ``tasks.py`` wraps these, and the one-shot bootstrap
under ``models/us_treasury_usaspending/code/`` imports them directly so the
transform exists in exactly one place.

The transform is deliberately thin. The archive ships CSV whose column names are
already English snake_case, so cleaning is: rename the handful of columns
BigQuery cannot accept, promote the fiscal year to the partition column, reorder
to the architecture, and write all-STRING partitioned parquet.

Reading every column as a string is what keeps the staging layer faithful: no
float round-trip that would turn 2007 into "2007.0", and no ``astype(str)`` that
would turn NULL into the literal "nan". The dbt model does the real casting.
"""

from __future__ import annotations

import csv
import json
import re
import subprocess
import time
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from pipelines.datasets.us_treasury_usaspending.constants import constants

ARCHIVE_BASE = constants.ARCHIVE_BASE.value
PARTITION = constants.PARTITION_COLUMN.value
SOURCE_PARTITION = constants.SOURCE_PARTITION_COLUMN.value

# Renames applied to source column names. BigQuery rejects hyphens and leading
# digits, so those five columns cannot keep their source spelling.
RENAMES = {
    SOURCE_PARTITION: PARTITION,
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award": "outlayed_amount_from_covid19_supplementals_for_overall_award",
    "obligated_amount_from_COVID-19_supplementals_for_overall_award": "obligated_amount_from_covid19_supplementals_for_overall_award",
    "1862_land_grant_college": "land_grant_college_1862",
    "1890_land_grant_college": "land_grant_college_1890",
    "1994_land_grant_college": "land_grant_college_1994",
}


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------
def architecture_columns(table: str) -> list[str]:
    """Column names for ``table``, in architecture order."""
    path = Path(constants.ARCHITECTURE_DIR.value) / f"{table}.csv"
    with path.open() as f:
        return [row["name"] for row in csv.DictReader(f)]


def staging_schema(table: str) -> pa.Schema:
    """All-STRING arrow schema carrying the architecture's column order.

    Staging is all-STRING by house convention — ``upload_to_gcs`` builds the
    BigQuery staging table from a stringified one-row header, so typed parquet
    is rejected on read. The schema here carries order, not types.
    """
    return pa.schema(
        [(name, pa.string()) for name in architecture_columns(table)]
    )


# --------------------------------------------------------------------------
# Source discovery and download
# --------------------------------------------------------------------------
def latest_stamp(fiscal_year: int, family: str = "Assistance") -> str:
    """Publication stamp (YYYYMMDD) of the current archive build.

    The archive is rebuilt monthly and every file carries the same stamp, so one
    query answers for the whole set.
    """
    body = json.dumps(
        {"fiscal_year": fiscal_year, "agency": "all", "type": family.lower()}
    ).encode()
    req = urllib.request.Request(
        constants.MONTHLY_FILES_API.value,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        payload = json.loads(r.read())
    for entry in payload["monthly_files"]:
        m = re.search(r"_(\d{8})\.zip$", entry["file_name"])
        if m and entry.get("fiscal_year") == fiscal_year:
            return m.group(1)
    raise RuntimeError(f"no archive file listed for FY{fiscal_year} {family}")


def archive_name(fiscal_year: int, family: str, stamp: str) -> str:
    return f"FY{fiscal_year}_All_{family}_Full_{stamp}.zip"


def archive_url(fiscal_year: int, family: str, stamp: str) -> str:
    return f"{ARCHIVE_BASE}/{archive_name(fiscal_year, family, stamp)}"


def remote_size(url: str) -> int:
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=60) as r:
        return int(r.headers["Content-Length"])


def download_archive(
    fiscal_year: int,
    family: str,
    stamp: str,
    dest_dir: Path,
    max_attempts: int = 200,
    max_backoff: int = 300,
) -> Path:
    """Download one archive zip, resuming until it is complete and readable.

    files.usaspending.gov is hostile to bulk clients in three specific ways, and
    all three are handled here:

    * It rate-limits on concurrency. A handful of parallel streams draws HTTP
      500s and then a blanket refusal of GET requests from the caller's address
      that outlasts the burst, so archives are fetched **one at a time**.
    * It truncates long transfers routinely, and ``curl -C -`` is not a safe
      resume against it: when it answers a Range request with 200 rather than
      206 curl overwrites the local file from byte zero. Here the response is
      appended and the status checked afterwards, rolling back to the resume
      offset if the origin resent the whole body.
    * It refuses *small* range requests outright (a two-byte range returns an
      empty reply), so the range cannot be probed cheaply before committing.

    Args:
        fiscal_year: Fiscal year of the archive.
        family: ``"Contracts"`` or ``"Assistance"``.
        stamp: Archive publication stamp, ``YYYYMMDD``.
        dest_dir: Directory to download into.
        max_attempts: Resume attempts before giving up.
        max_backoff: Ceiling, in seconds, for the backoff between stalled attempts.

    Returns:
        Path of the downloaded zip.

    Raises:
        RuntimeError: If the file could not be completed.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    url = archive_url(fiscal_year, family, stamp)
    dest = dest_dir / archive_name(fiscal_year, family, stamp)
    expected = remote_size(url)

    delay, stalls = 5, 0
    for _ in range(max_attempts):
        have = dest.stat().st_size if dest.exists() else 0
        if have == expected and _zip_ok(dest):
            return dest
        if have > expected:  # corrupt partial, start over
            dest.unlink()
            have = 0
        written = _append_range(url, dest, have)
        if written > 0:
            stalls, delay = 0, 5
        else:
            stalls += 1
            if stalls >= 80:
                raise RuntimeError(
                    f"download stalled for {dest.name} at "
                    f"{dest.stat().st_size if dest.exists() else 0}/{expected} bytes"
                )
            time.sleep(delay)
            delay = min(delay * 2, max_backoff)
    raise RuntimeError(
        f"could not download {dest.name} after {max_attempts} attempts"
    )


def _append_range(url: str, dest: Path, start: int) -> int:
    """Append the response from `start` onward to `dest`; return bytes written."""
    headers = dest.with_suffix(dest.suffix + ".headers")
    args = [
        "curl",
        "-sS",
        "--fail",
        "--speed-time",
        "180",
        "--speed-limit",
        "5000",
        "--max-time",
        "3600",
        "-D",
        str(headers),
    ]
    if start:
        args += ["-r", f"{start}-"]
    args.append(url)

    before = dest.stat().st_size if dest.exists() else 0
    with dest.open("ab") as f:
        subprocess.run(args, stdout=f, stderr=subprocess.DEVNULL, check=False)
    after = dest.stat().st_size if dest.exists() else 0

    status = ""
    if headers.exists():
        first = headers.read_text(errors="ignore").split("\n", 1)[0].split()
        status = first[1] if len(first) > 1 else ""
        headers.unlink(missing_ok=True)
    if start and status == "200":
        # The origin ignored the Range and resent the whole body; drop it.
        with dest.open("r+b") as f:
            f.truncate(start)
        return 0
    return after - before


def _zip_ok(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as z:
            return bool(z.namelist())
    except Exception:
        return False


# --------------------------------------------------------------------------
# Cleaning transform
# --------------------------------------------------------------------------
def _normalize_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.Table:
    """Rename source columns and project onto the architecture schema."""
    names = [RENAMES.get(n, n) for n in batch.schema.names]
    table = pa.Table.from_batches([batch]).rename_columns(names)
    present = set(names)
    arrays = []
    for field in schema:
        if field.name in present:
            arrays.append(table.column(field.name))
        else:
            arrays.append(pa.nulls(table.num_rows, type=pa.string()))
    return pa.Table.from_arrays(arrays, schema=schema)


def clean_archive(
    zip_path: Path,
    table: str,
    output_dir: Path,
    expected_fiscal_year: int | None = None,
) -> dict[int, int]:
    """Clean one archive zip into hive-partitioned all-STRING parquet.

    Writes ``<output_dir>/<table>/fiscal_year=<FY>/<member>.parquet`` and returns
    the row count per fiscal year. Rows are routed by their own
    ``action_date_fiscal_year`` rather than by the file name, because a fiscal
    year file is not guaranteed to be internally homogeneous.
    """
    zip_path = Path(zip_path)
    schema = staging_schema(table)
    read = pacsv.ReadOptions(block_size=64 << 20)

    counts: dict[int, int] = defaultdict(int)
    writers: dict[int, pq.ParquetWriter] = {}
    try:
        with zipfile.ZipFile(zip_path) as z:
            members = [n for n in z.namelist() if n.lower().endswith(".csv")]
            # One parquet per (archive, fiscal year), not per CSV member: the
            # members are just 1M-row slices of the same table, and a single
            # deterministic name per archive is what lets the recurring pipeline
            # overwrite the current fiscal year cleanly.
            stem = zip_path.stem
            for member in sorted(members):
                # Every column is read as a string. The type map has to name each
                # column explicitly: pyarrow copies ``column_types`` into a plain
                # dict, so a defaultdict fallback is silently dropped and sparse
                # columns get inferred as null, then fail on the first value.
                convert = pacsv.ConvertOptions(
                    column_types={
                        name: pa.string() for name in _csv_header(z, member)
                    },
                    strings_can_be_null=True,
                    null_values=[""],
                )
                with z.open(member) as raw:
                    reader = pacsv.open_csv(
                        raw, read_options=read, convert_options=convert
                    )
                    for batch in reader:
                        chunk = _normalize_batch(batch, schema)
                        for fy, part in _split_by_partition(chunk):
                            counts[fy] += part.num_rows
                            writer = writers.get(fy)
                            if writer is None:
                                out = (
                                    Path(output_dir)
                                    / table
                                    / f"{PARTITION}={fy}"
                                    / f"{stem}.parquet"
                                )
                                out.parent.mkdir(parents=True, exist_ok=True)
                                writer = pq.ParquetWriter(
                                    out, schema, compression="snappy"
                                )
                                writers[fy] = writer
                            writer.write_table(
                                part,
                                row_group_size=constants.ROW_GROUP_SIZE.value,
                            )
    finally:
        for writer in writers.values():
            writer.close()

    if expected_fiscal_year is not None and set(counts) - {
        expected_fiscal_year
    }:
        stray = {
            fy: n for fy, n in counts.items() if fy != expected_fiscal_year
        }
        print(f"  note: {zip_path.name} also carried rows for {stray}")
    return dict(counts)


def _csv_header(archive: zipfile.ZipFile, member: str) -> list[str]:
    """Column names of a zipped CSV, read without inflating the whole member."""
    with archive.open(member) as raw:
        buf = b""
        while b"\n" not in buf:
            block = raw.read(1 << 16)
            if not block:
                break
            buf += block
    line = buf.split(b"\n", 1)[0].decode("utf-8-sig").rstrip("\r")
    return next(csv.reader([line]))


def _split_by_partition(chunk: pa.Table):
    """Yield (fiscal_year, sub-table) pairs, one per distinct partition value."""
    col = chunk.column(PARTITION).to_pylist()
    distinct = {v for v in col if v}
    if len(distinct) == 1:
        yield int(next(iter(distinct))), chunk
        return
    groups: dict[str, list[int]] = defaultdict(list)
    for i, v in enumerate(col):
        groups[v or ""].append(i)
    for value, idx in groups.items():
        if not value:
            # A transaction with no fiscal year cannot be partitioned; the
            # source has never emitted one, so surface it rather than drop it.
            raise ValueError(f"{len(idx)} rows with an empty {PARTITION}")
        yield int(value), chunk.take(pa.array(idx))


def clean_fiscal_year(
    fiscal_year: int,
    family: str,
    stamp: str,
    input_dir: Path,
    output_dir: Path,
    keep_archive: bool = False,
) -> dict[int, int]:
    """Download one fiscal year's archive, clean it, and drop the zip."""
    table = constants.AWARD_FAMILIES.value[family]
    zip_path = download_archive(fiscal_year, family, stamp, Path(input_dir))
    counts = clean_archive(
        zip_path, table, Path(output_dir), expected_fiscal_year=fiscal_year
    )
    if not keep_archive:
        zip_path.unlink(missing_ok=True)
        Path(str(zip_path) + ".done").unlink(missing_ok=True)
    return counts


def write_dicionario(rows_csv: Path, output_dir: Path) -> int:
    """Write the dictionary table to parquet from its generated CSV."""
    schema = staging_schema("dicionario")
    with Path(rows_csv).open() as f:
        rows = list(csv.DictReader(f))
    arrays = [
        pa.array([r.get(name) or None for r in rows], type=pa.string())
        for name in schema.names
    ]
    out = Path(output_dir) / "dicionario"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_arrays(arrays, schema=schema),
        out / "data.parquet",
        compression="snappy",
    )
    return len(rows)
