"""Prefect 3 tasks for us_irs_form990 — thin wrappers over utils.py."""

from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task

from pipelines.datasets.us_irs_form990 import utils
from pipelines.datasets.us_irs_form990.constants import constants


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
def loaded_batches(bq_project: str) -> set[str]:
    """Batches already present in the staging table (``xml_batch_id``).

    Reading the staging table, not the published one, keeps a re-run idempotent
    even when the previous run uploaded a batch but failed before dbt.

    Only a genuinely missing table yields an empty set. Every other failure —
    a permissions error above all — is re-raised: swallowing it silently turns
    the incremental load into a full re-download of every IRS batch, which
    looks like a healthy run and costs hours. A hand-created staging dataset
    missing the worker's grant is exactly how that happens.
    """
    client = bigquery.Client(project=bq_project)
    table = (
        f"{bq_project}.{constants.DATASET_ID.value}_staging.return_financial"
    )
    try:
        rows = client.query(
            f"select distinct xml_batch_id from `{table}`"
        ).result()
    except NotFound:
        print(f"{table} does not exist yet; treating this as the first load")
        return set()
    return {r.xml_batch_id for r in rows}


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
