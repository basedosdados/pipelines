"""Stream the OpenAlex snapshot into a Data Basis staging bucket.

Shared by the recurring flow and the one-shot bootstrap. No Prefect imports.

One snapshot file at a time: download it from S3, flatten it into one
all-STRING parquet per table, upload those to
``gs://<bucket>/staging/world_openalex/<table>/``, delete the local copies.
Peak disk is a few files, not the 771 GB snapshot.

The run is resumable. A JSONL state file records every finished source file
together with the release date and a fingerprint of the transform code and the
architecture. A resume under a different release or fingerprint is refused,
because skipping already-staged files would keep output written by older code.
"""

import hashlib
import json
import multiprocessing
import shutil
import tempfile
import time
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import google.cloud.storage as gcs
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

from pipelines.datasets.world_openalex import utils
from pipelines.datasets.world_openalex.constants import constants

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


def _bucket(bucket_name: str) -> gcs.Bucket:
    # Data Basis buckets are requester-pays; bill the bucket's own project, as
    # pipelines.utils.tasks._upload_to_gcs does.
    return gcs.Client(project=bucket_name).bucket(
        bucket_name, user_project=bucket_name
    )


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


def download(path: str, dest: Path) -> None:
    """Copy one snapshot file from S3 to local disk in a single stream.

    Reading column chunks straight from S3 costs one request per chunk; from a
    high-latency link that is 20 s for a 60-record file. One sequential GET
    avoids it.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(5):
        try:
            with (
                utils.s3_filesystem().open_input_stream(path) as src,
                dest.open("wb") as out,
            ):
                while chunk := src.read(16 * 1024 * 1024):
                    out.write(chunk)
            return
        except OSError:
            if attempt == 4:
                raise
            time.sleep(10 * (attempt + 1))


def run_file(entity: str, path: str, bucket_name: str, scratch: str) -> dict:
    """Download, flatten and upload one snapshot file. Runs in a worker process.

    Returns:
        A state record: entity, source path, and rows written per table.
    """
    tag = utils.file_tag(path)
    work = Path(scratch) / f"{entity}_{tag}"
    shutil.rmtree(work, ignore_errors=True)
    try:
        src = work / "src.parquet"
        download(path, src)
        out = utils.process_file(
            entity,
            str(src),
            work / "out",
            tag,
            filesystem=fs.LocalFileSystem(),
        )
        for table, (local, _) in out.items():
            upload_file(bucket_name, table, local, local.name)
        return {
            "entity": entity,
            "path": path,
            "rows": {t: n for t, (_, n) in out.items()},
        }
    finally:
        shutil.rmtree(work, ignore_errors=True)


# --------------------------------------------------------------------------
# Whole snapshot
# --------------------------------------------------------------------------


def _read_state(state_path: Path) -> list[dict]:
    if not state_path.exists():
        return []
    return [
        json.loads(line)
        for line in state_path.read_text().splitlines()
        if line
    ]


def load_snapshot(
    bucket_name: str,
    scratch: Path,
    entities: list[str] | None = None,
    workers: int = 4,
    fresh: bool = False,
    max_files: int | None = None,
) -> dict:
    """Load the current snapshot into ``gs://<bucket>/staging/world_openalex/``.

    Args:
        bucket_name: ``basedosdados-dev`` or ``basedosdados``.
        scratch: Local directory for in-flight files and the state file.
        entities: Snapshot entities to load; defaults to all of them.
        workers: Source files processed in parallel (one process each).
        fresh: Discard any previous state, clear the staging prefixes and
            recreate the staging tables. Required on the first run and after
            any change to the transform.
        max_files: Cap on files per entity, for test runs.

    Returns:
        ``{"release": date, "rows": {table: rows}, "checks": {entity: (rows, expected)}}``.

    Raises:
        RuntimeError: on a resume under a different release or fingerprint, or
            when a fully loaded entity's row count disagrees with the manifest.
    """
    entities = entities or list(constants.ENTITY_TABLES.value)
    scratch.mkdir(parents=True, exist_ok=True)
    state_path = scratch / "state.jsonl"
    manifest = utils.fetch_manifest()
    release = utils.release_date(manifest)
    fp = fingerprint()
    header = {
        "release": release,
        "fingerprint": fp,
        "bucket": bucket_name,
        "entities": entities,
    }

    state = [] if fresh else _read_state(state_path)
    if state and {k: state[0].get(k) for k in header} != header:
        raise RuntimeError(
            f"state at {state_path} was written for {state[0]}, this run is {header}; "
            "rerun with fresh=True to discard it"
        )
    if not state:
        print(
            f"Fresh load of release {release} (fingerprint {fp}) into {bucket_name}"
        )
        create_staging_tables(bucket_name, all_tables(entities))
        state_path.write_text(json.dumps(header) + "\n")
    done = {r["path"] for r in state[1:]}

    jobs = []
    for e in entities:
        files = utils.entity_files(manifest, e)
        if max_files:
            files = files[:max_files]
        jobs += [(e, p) for p, _ in files if p not in done]
    print(
        f"{len(done)} files already loaded, {len(jobs)} to go, {workers} workers"
    )

    started = time.time()
    # spawn, not fork: the flow runs this inside a threaded Prefect process,
    # and forking a process that holds threads can deadlock the child.
    ctx = multiprocessing.get_context("spawn")
    with (
        ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as ex,
        state_path.open("a") as log,
    ):
        futs = {
            ex.submit(run_file, e, p, bucket_name, str(scratch)): p
            for e, p in jobs
        }
        for i, fut in enumerate(as_completed(futs), 1):
            rec = fut.result()
            log.write(json.dumps(rec) + "\n")
            log.flush()
            if i % 10 == 0 or i == len(jobs):
                rate = i / (time.time() - started)
                print(
                    f"  {i}/{len(jobs)} files, {rate * 3600:.0f}/h, last {rec['path']}"
                )

    # Totals and completeness against the manifest.
    records = _read_state(state_path)[1:]
    rows: dict[str, int] = {}
    for r in records:
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
