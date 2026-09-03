"""Prefect 3 tasks for us_irs_form990 — thin wrappers over utils.py."""

import re
from pathlib import Path

import basedosdados as bd
from prefect import task

from pipelines.datasets.us_irs_form990 import utils
from pipelines.datasets.us_irs_form990.constants import constants

#: ``<batch>_p<k>.parquet`` — the batch id is the part file's name prefix.
_PART_RE = re.compile(r"(.+)_p\d+\.parquet$")


@task(retries=2, retry_delay_seconds=60)
def list_efile_batches() -> list[str]:
    """Every e-file ZIP URL the IRS currently hosts."""
    return utils.list_efile_zips()


@task(retries=2, retry_delay_seconds=60)
def check_source_dates(urls: list[str]) -> dict[str, str]:
    """Publication dates of the newest e-file ZIP and of the BMF extract.

    Cheap poll signal (HTTP HEAD only), compared against ``Table.Update.latest``
    so a scheduled run between releases downloads nothing.
    """
    return {
        "efile": utils.efile_source_date(urls),
        "bmf": utils.bmf_source_date(),
    }


@task(retries=1, retry_delay_seconds=60)
def loaded_batches(bucket_name: str) -> set[str]:
    """Batches already uploaded to the staging bucket.

    Read from **GCS, not BigQuery**. Each parquet part is named
    ``<batch>_p<k>.parquet``, so the set of loaded batches is already in the
    object names and one prefix listing answers the question.

    Querying the staging table instead would need ``bigquery.jobs.create`` in
    the billing project, which the Prefect worker does not hold. Worse, a
    permission error there is indistinguishable from "nothing loaded yet"
    unless it is re-raised — and swallowing it silently turns the incremental
    load into a full re-download of every IRS batch on every run, which still
    reports success.

    Reading the bucket rather than the table also keeps a re-run idempotent
    when a previous run uploaded a batch but failed before dbt.
    """
    # Go through ``bd.Storage`` rather than a bare ``storage.Client``. The
    # staging buckets are requester-pays, so the listing must name a billing
    # project, and the pod's own ADC identity holds no
    # ``serviceusage.services.use`` there. ``bd.Storage`` builds its bucket
    # handle from the credentials in the pod's ``config.toml`` — the same ones
    # ``upload_to_gcs`` writes with — and already pins ``user_project`` to
    # ``billing_project_id``.
    storage = bd.Storage(
        dataset_id=constants.DATASET_ID.value,
        table_id="return_financial",
        bucket_name=bucket_name,
        billing_project_id=bucket_name,
    )
    prefix = f"staging/{constants.DATASET_ID.value}/return_financial/"
    batches = set()
    for blob in storage.bucket.list_blobs(prefix=prefix):
        match = _PART_RE.match(Path(blob.name).name)
        if match:
            batches.add(match.group(1))
    print(f"{len(batches)} batch(es) already in gs://{bucket_name}/{prefix}")
    return batches


@task(retries=2, retry_delay_seconds=300)
def process_efile_batch(url: str, work_dir: str) -> dict:
    """Download one ZIP, parse it, delete the ZIP; return the parse summary."""
    work = Path(work_dir)
    zip_path = utils.download(url, work / "input" / Path(url).name)
    res = utils.clean_efile_zip(zip_path, work / "output")
    zip_path.unlink(missing_ok=True)
    res.pop("xpath_hits", None)
    return res


@task(retries=2, retry_delay_seconds=300)
def process_bmf(work_dir: str, extraction_date: str) -> dict:
    """Download the six BMF CSVs and stack them into one snapshot partition."""
    work = Path(work_dir)
    for name in constants.BMF_FILES.value:
        utils.download(
            constants.BMF_BASE_URL.value + f"{name}.csv",
            work / "input" / "bmf" / f"{name}.csv",
        )
    return utils.clean_bmf(
        work / "input" / "bmf", work / "output", extraction_date
    )


@task(retries=2, retry_delay_seconds=300)
def process_revocation(work_dir: str) -> dict:
    work = Path(work_dir)
    zip_path = utils.download(
        constants.REVOCATION_URL.value,
        work / "input" / "revocation" / "data-download-revocation.zip",
    )
    return utils.clean_revocation(zip_path, work / "output")


@task
def write_dicionario(work_dir: str) -> int:
    csv_path = constants.ARCHITECTURE_DIR.value.parent / "dicionario.csv"
    return utils.write_dicionario(csv_path, Path(work_dir) / "output")
