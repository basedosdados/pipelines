"""Stream the OpenAlex snapshot into a Data Basis staging bucket.

Shared by the recurring flow and the one-shot bootstrap. No Prefect imports.

One snapshot file at a time: download it from S3, flatten it into one
all-STRING parquet per table, upload those to
``gs://<bucket>/staging/world_openalex/<table>/``, delete the local copies.
Peak disk is a few files, not the 771 GB snapshot.

The run is resumable. Each finished source file leaves a marker in GCS under
a prefix keyed by the release date and a fingerprint of the transform code
and architecture, so a restart skips finished files, and output written by
different code or a different release is never mixed with it.
"""

import hashlib
import json
import multiprocessing
import os
import shutil
import tempfile
import time
from collections.abc import Iterable
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from functools import cache
from pathlib import Path

import google.cloud.storage as gcs
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from pyarrow import fs

from pipelines.datasets.world_openalex import utils
from pipelines.datasets.world_openalex.constants import constants
from pipelines.utils.gcs import get_credentials_from_env

DATASET_ID = constants.DATASET_ID.value
# The table each entity's record count is checked against: one row per record.
ENTITY_MAIN_TABLE = {
    e: tables[0] for e, tables in constants.ENTITY_TABLES.value.items()
}


def fingerprint() -> str:
    """Hash of the transform code and every architecture CSV."""
    h = hashlib.sha256()
    here = Path(__file__).parent
    for p in [here / "utils.py", here / "loader.py", here / "constants.py"]:
        h.update(p.read_bytes())
    for p in sorted(Path(constants.ARCHITECTURE_DIR.value).glob("*.csv")):
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


def all_tables(entities: Iterable[str]) -> list[str]:
    """Tables built from the given entities."""
    return [t for e in entities for t in constants.ENTITY_TABLES.value[e]]


# --------------------------------------------------------------------------
# GCS
# --------------------------------------------------------------------------


@cache
def _bucket(bucket_name: str) -> gcs.Bucket:
    """A handle on a Data Basis bucket, with the right credentials.

    On a Prefect pod the service-account keys come from
    ``BASEDOSDADOS_CREDENTIALS_{STAGING,PROD}``, as in ``pipelines.utils.gcs``:
    the pod's own identity lacks ``serviceusage.services.use`` on the
    requester-pays dev bucket. Locally those variables are unset and
    GOOGLE_APPLICATION_CREDENTIALS is used.
    """
    mode = "prod" if bucket_name == "basedosdados" else "staging"
    credentials = (
        get_credentials_from_env(mode=mode)
        if os.environ.get(f"BASEDOSDADOS_CREDENTIALS_{mode.upper()}")
        else None
    )
    client = gcs.Client(project=bucket_name, credentials=credentials)
    # Requester-pays: bill the bucket's own project, as _upload_to_gcs does.
    return client.bucket(bucket_name, user_project=bucket_name)


def staging_prefix(table: str) -> str:
    """GCS prefix of a table's staging files."""
    return f"staging/{DATASET_ID}/{table}/"


def clear_staging(bucket_name: str, table: str) -> int:
    """Delete every staging file of one table. Returns the number deleted."""
    b = _bucket(bucket_name)
    blobs = list(b.list_blobs(prefix=staging_prefix(table)))
    for i in range(0, len(blobs), 100):
        with b.client.batch():
            for blob in blobs[i : i + 100]:
                blob.delete()
    return len(blobs)


def upload_file(bucket_name: str, table: str, local: Path, name: str) -> None:
    """Upload one staging parquet, retrying transient failures."""
    blob = _bucket(bucket_name).blob(staging_prefix(table) + name)
    for attempt in range(5):
        try:
            blob.upload_from_filename(str(local), timeout=600)
            return
        except Exception:
            if attempt == 4:
                raise
            time.sleep(10 * (attempt + 1))


def header_file(table: str, directory: Path) -> Path:
    """Write a 1-row all-STRING parquet carrying the table's architecture schema.

    One row, not zero: BigQuery infers the staging schema from this file, and
    ``gcs.dump_header`` round-trips through pandas, where a 0-row frame loses its
    string dtype and every column comes back INTEGER.
    """
    names = [n for n, _ in utils.architecture(table)]
    t = pa.table({n: pa.array(["header"], pa.string()) for n in names})
    path = directory / table / "header.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(t, path)
    return path


def create_staging_tables(bucket_name: str, tables: Iterable[str]) -> None:
    """Clear each table's staging prefix and (re)create its external table.

    Mirrors the create branch of ``pipelines.utils.tasks._upload_to_gcs``: build
    the EXTERNAL table over ``gs://<bucket>/staging/world_openalex/<table>/*``
    from a header file, then delete the header from the bucket, leaving the
    prefix empty for the data. Unlike ``_upload_to_gcs`` no data is uploaded here.
    """
    import basedosdados as bd  # heavy import, keep local

    tmp = Path(tempfile.mkdtemp(prefix="world_openalex_schema_"))
    try:
        for table in tables:
            clear_staging(bucket_name, table)
            path = header_file(table, tmp)
            kw = {
                "dataset_id": DATASET_ID,
                "table_id": table,
                "bucket_name": bucket_name,
            }
            tb = bd.Table(**kw, billing_project_id=bucket_name)
            tb.create(
                path=str(path.parent),
                if_storage_data_exists="replace",
                if_table_exists="replace",
                source_format="parquet",
            )
            bd.Storage(**kw, billing_project_id=bucket_name).delete_table(
                mode="staging", bucket_name=bucket_name, not_found_ok=True
            )
            if clear_staging(bucket_name, table):
                raise RuntimeError(f"{table}: header left behind in staging")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# --------------------------------------------------------------------------
# One source file
# --------------------------------------------------------------------------

S3_HTTPS = "https://openalex.s3.amazonaws.com/"
PART_BYTES = 32 * 1024 * 1024
TRANSFER_THREADS = 8


def _get_range(url: str, start: int, end: int) -> bytes:
    """One HTTP range GET, retried."""
    for attempt in range(6):
        try:
            r = requests.get(
                url, headers={"Range": f"bytes={start}-{end}"}, timeout=300
            )
            r.raise_for_status()
            if len(r.content) != end - start + 1:
                raise OSError(f"short read {len(r.content)} at {start}")
            return r.content
        except (requests.RequestException, OSError):
            if attempt == 5:
                raise
            time.sleep(5 * (attempt + 1))
    raise AssertionError("unreachable")


def download(path: str, dest: Path, size: int) -> None:
    """Copy one snapshot file to local disk with parallel range requests.

    A single stream ran at about 2 MB/s from the Prefect pods, which made the
    network, not the flattening, the bottleneck of the whole load. The bucket
    is public, so plain HTTPS range GETs need no AWS client.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = S3_HTTPS + path.removeprefix(f"{constants.S3_BUCKET.value}/")
    with dest.open("wb") as out:
        out.truncate(size)
    ranges = [
        (o, min(o + PART_BYTES, size) - 1) for o in range(0, size, PART_BYTES)
    ]
    fd = os.open(dest, os.O_WRONLY)
    try:
        with ThreadPoolExecutor(TRANSFER_THREADS) as ex:
            futs = {ex.submit(_get_range, url, a, b): a for a, b in ranges}
            for fut in as_completed(futs):
                os.pwrite(fd, fut.result(), futs[fut])
    finally:
        os.close(fd)
    if dest.stat().st_size != size:
        raise OSError(
            f"{path}: {dest.stat().st_size} bytes, manifest says {size}"
        )


def marker_prefix(release: str, fp: str) -> str:
    """GCS prefix of the per-file completion markers of one load."""
    return f"staging/{DATASET_ID}/_load_state/{release}_{fp}/"


def run_file(
    entity: str,
    path: str,
    size: int,
    bucket_name: str,
    scratch: str,
    markers: str,
) -> dict:
    """Download, flatten and upload one snapshot file. Runs in a worker process.

    Writes a completion marker to ``markers`` only after every table file is
    uploaded, so a marker means the file is fully staged.

    Returns:
        The marker record: entity, source path, rows per table, stage timings.
    """
    tag = utils.file_tag(path)
    work = Path(scratch) / f"{entity}_{tag}"
    shutil.rmtree(work, ignore_errors=True)
    try:
        t0 = time.time()
        src = work / "src.parquet"
        download(path, src, size)
        t1 = time.time()
        out = utils.process_file(
            entity,
            str(src),
            work / "out",
            tag,
            filesystem=fs.LocalFileSystem(),
        )
        src.unlink()
        t2 = time.time()
        with ThreadPoolExecutor(TRANSFER_THREADS) as ex:
            list(
                ex.map(
                    lambda item: upload_file(
                        bucket_name, item[0], item[1][0], item[1][0].name
                    ),
                    out.items(),
                )
            )
        t3 = time.time()
        rec = {
            "entity": entity,
            "path": path,
            "rows": {t: n for t, (_, n) in out.items()},
            "seconds": {
                "download": round(t1 - t0, 1),
                "process": round(t2 - t1, 1),
                "upload": round(t3 - t2, 1),
            },
        }
        _bucket(bucket_name).blob(
            f"{markers}{entity}__{tag}.json"
        ).upload_from_string(json.dumps(rec), content_type="application/json")
        return rec
    finally:
        shutil.rmtree(work, ignore_errors=True)


# --------------------------------------------------------------------------
# Whole snapshot
# --------------------------------------------------------------------------


def read_markers(bucket_name: str, prefix: str) -> list[dict]:
    """Every completion marker under ``prefix``."""
    blobs = list(_bucket(bucket_name).list_blobs(prefix=prefix))
    with ThreadPoolExecutor(16) as ex:
        return list(ex.map(lambda b: json.loads(b.download_as_bytes()), blobs))


def clear_markers(bucket_name: str) -> None:
    """Delete the markers of every previous load."""
    b = _bucket(bucket_name)
    blobs = list(b.list_blobs(prefix=f"staging/{DATASET_ID}/_load_state/"))
    for i in range(0, len(blobs), 100):
        with b.client.batch():
            for blob in blobs[i : i + 100]:
                blob.delete()


def load_snapshot(
    bucket_name: str,
    scratch: Path,
    entities: list[str] | None = None,
    workers: int = 4,
    fresh: bool = False,
    max_files: int | None = None,
) -> dict:
    """Load the current snapshot into ``gs://<bucket>/staging/world_openalex/``.

    State lives in GCS as one marker per finished source file, under a prefix
    keyed by the release date and the transform fingerprint. A run resumes from
    the markers of its own release and code; when there are none (first run,
    new release, or changed code), it starts fresh: clears every staging
    prefix and recreates the staging tables, so output from different code or
    releases never mixes.

    Args:
        bucket_name: ``basedosdados-dev`` or ``basedosdados``.
        scratch: Local directory for in-flight files.
        entities: Snapshot entities to load; defaults to all of them.
        workers: Source files processed in parallel (one process each).
        fresh: Start fresh even when markers for this release and code exist.
        max_files: Cap on files per entity, for test runs.

    Returns:
        ``{"release": date, "rows": {table: rows}, "checks": {entity: (rows, expected)}}``.

    Raises:
        RuntimeError: when a fully loaded entity's row count disagrees with the
            manifest.
    """
    entities = entities or list(constants.ENTITY_TABLES.value)
    scratch.mkdir(parents=True, exist_ok=True)
    manifest = utils.fetch_manifest()
    release = utils.release_date(manifest)
    fp = fingerprint()
    markers = marker_prefix(release, fp)

    done_recs = [] if fresh else read_markers(bucket_name, markers)
    if not done_recs:
        print(
            f"Fresh load of release {release} (fingerprint {fp}) into {bucket_name}"
        )
        clear_markers(bucket_name)
        create_staging_tables(bucket_name, all_tables(entities))
    done = {r["path"] for r in done_recs}

    jobs = []
    for e in entities:
        files = utils.entity_files(manifest, e)
        sizes = {
            f["url"].removeprefix("s3://"): f["meta"]["content_length"]
            for ent in manifest["entities"]
            if ent["entity"] == e
            for f in ent["files"]
        }
        if max_files:
            files = files[:max_files]
        jobs += [(e, p, sizes[p]) for p, _ in files if p not in done]
    print(
        f"{len(done)} files already loaded, {len(jobs)} to go, {workers} workers"
    )

    started = time.time()
    # spawn, not fork: the flow runs this inside a threaded Prefect process,
    # and forking a process that holds threads can deadlock the child.
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as ex:
        futs = [
            ex.submit(run_file, e, p, n, bucket_name, str(scratch), markers)
            for e, p, n in jobs
        ]
        for i, fut in enumerate(as_completed(futs), 1):
            rec = fut.result()
            if i % 10 == 0 or i == len(jobs):
                rate = i / (time.time() - started)
                print(
                    f"  {i}/{len(jobs)} files, {rate * 3600:.0f}/h, "
                    f"last {rec['path']} {rec['seconds']}"
                )

    # Totals and completeness against the manifest.
    rows: dict[str, int] = {}
    for r in read_markers(bucket_name, markers):
        for t, n in r["rows"].items():
            rows[t] = rows.get(t, 0) + n
    checks = {}
    for e in entities:
        expected = utils.entity_record_count(manifest, e)
        got = rows.get(ENTITY_MAIN_TABLE[e], 0)
        checks[e] = (got, expected)
        if not max_files and got != expected:
            raise RuntimeError(
                f"{e}: {got:,} rows staged, manifest declares {expected:,}"
            )

    if "works" in entities:
        rows["dicionario"] = _load_dicionario(bucket_name, manifest, scratch)
    return {"release": release, "rows": rows, "checks": checks}


def _load_dicionario(bucket_name: str, manifest: dict, scratch: Path) -> int:
    """Build the dicionario from the lookup entities and stage it. Returns its rows."""
    create_staging_tables(bucket_name, ["dicionario"])
    t = utils.to_staging("dicionario", utils.build_dicionario(manifest))
    path = scratch / "dicionario.parquet"
    pq.write_table(t, path, compression="snappy")
    upload_file(bucket_name, "dicionario", path, "dicionario.parquet")
    path.unlink()
    return t.num_rows
