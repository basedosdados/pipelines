"""Tasks for br_bd_execucao_estadual."""

from __future__ import annotations

from pathlib import Path

from prefect import task

from pipelines.datasets.br_bd_execucao_estadual.constants import constants
from pipelines.datasets.br_bd_execucao_estadual.utils import (
    REFRESHERS,
    built_tables,
)


@task(retries=2, retry_delay_seconds=300)
def refresh_state(
    state: str, work_dir: str, year: int, full_refresh: bool
) -> dict[str, str]:
    """Download and clean one state, returning {staging table: parquet dir}.

    Retried twice: three of the four sources are plain HTTP file fetches that fail
    transiently, and São Paulo's WebForms scrape drops sessions. The downloaders skip
    files already on disk, so a retry within a run resumes rather than starting over.
    Across runs there is nothing to resume from: `work_dir` is a fresh mkdtemp, which
    is why the year scope is an argument to the downloader and not a disk state.
    """
    REFRESHERS[state](work_dir, year, full_refresh)
    built = built_tables(work_dir, state)
    if not built:
        raise RuntimeError(
            f"{state}: produced no parquet. Refusing to continue -- an empty result "
            "here would upload nothing and leave the prod staging prefix stale, "
            "which looks like success."
        )
    # Only a full refresh is expected to produce every mirror. An incremental run
    # rebuilds one exercise, so a source with nothing in the open year legitimately
    # yields no parquet -- Pernambuco's `despesa_legado` covers 2008-2010 and will
    # never appear again. Warning on that daily would train the reader to ignore it.
    if full_refresh:
        expected = set(constants.STAGING_BY_STATE.value[state])
        missing = expected - set(built)
        if missing:
            print(
                f"{state}: WARNING {len(missing)} staging table(s) empty: "
                f"{sorted(missing)}"
            )
    print(f"{state}: {len(built)} staging tables ready")
    return {k: str(v) for k, v in built.items()}


@task(retries=2, retry_delay_seconds=120)
def download_frozen_mirror(
    mirror: str, work_dir: str, billing_project: str = "basedosdados"
) -> str:
    """Download a frozen staging mirror's parquet from the dev bucket to local disk.

    The frozen mirrors (`ce_*`, `sc_contrato`, `rs_contrato`) are not produced by any
    refresher -- Ceará cannot be re-scraped from the worker and the contract registries
    are one-shot bootstrap loads. Their cleaned parquet already sits in the dev staging
    bucket, so a prod seed copies it forward rather than rebuilding it. The caller then
    hands the returned directory to `upload_to_gcs(bucket_name="basedosdados")`, exactly
    as a normal refresh does, so the prod staging external table is created the same way.

    The `00_header.parquet` sentinel is skipped: `upload_to_gcs` writes its own header.
    """
    from google.cloud import storage

    dest = Path(work_dir) / "output" / mirror
    dest.mkdir(parents=True, exist_ok=True)
    client = storage.Client(project=billing_project)
    bucket = client.bucket(
        bucket_name="basedosdados-dev", user_project=billing_project
    )
    prefix = f"staging/{constants.DATASET_ID.value}/{mirror}/"
    n = 0
    for blob in bucket.list_blobs(prefix=prefix):
        name = blob.name[len(prefix) :]
        if not name.endswith(".parquet") or name == "00_header.parquet":
            continue
        blob.download_to_filename(str(dest / name))
        n += 1
    if n == 0:
        raise RuntimeError(
            f"{mirror}: no parquet under gs://basedosdados-dev/{prefix} -- the dev "
            "staging mirror must exist before it can be seeded to prod"
        )
    print(f"{mirror}: pulled {n} parquet file(s) from dev staging")
    return str(dest)


@task
def parquet_row_count(paths: dict[str, str]) -> int:
    """Total rows across a state's parquet, for the run log.

    Cheap: parquet carries its row count in the footer, so nothing is read.
    """
    import pyarrow.parquet as pq

    total = 0
    for directory in paths.values():
        for file in Path(directory).glob("*.parquet"):
            total += pq.read_metadata(file).num_rows
    return total
