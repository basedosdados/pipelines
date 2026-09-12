"""Prefect 3 tasks for au_geoscape_gnaf — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_geoscape_gnaf.utils import (
    clean_all,
    download_zip,
    resolve_source,
)


@task(retries=2, retry_delay_seconds=30)
def check_source_gnaf() -> dict:
    """Resolve the current GDA2020 all-states release from the CKAN API.

    Cheap poll signal — no data download. Returns the resolved download URL and
    the derived ``snapshot_date`` (first of the release month), a coverage-style
    date compared against the free ``Coverage`` so the flow only fetches the
    ~1.6 GB payload when a newer quarterly snapshot has been published.

    Returns:
        ``{"url": <download url>, "snapshot_date": "YYYY-MM-01"}``.
    """
    return resolve_source()


@task(retries=2, retry_delay_seconds=60)
def download_gnaf(work_dir: str, url: str) -> str:
    """Download the release zip into ``<work_dir>/input``.

    Args:
        work_dir: Scratch directory for this run.
        url: Download URL resolved by ``check_source_gnaf``.

    Returns:
        The path of the downloaded zip (Prefect serializes task results).
    """
    dest = download_zip(url, Path(work_dir) / "input")
    return str(dest)


@task
def clean_gnaf(work_dir: str, zip_path: str, snapshot_date: str) -> dict:
    """Clean the release into all-STRING partitioned parquet under ``output``.

    Args:
        work_dir: Scratch directory for this run.
        zip_path: Path of the downloaded release zip.
        snapshot_date: Snapshot date ``"YYYY-MM-DD"``.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"snapshot_date"`` and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(
        zip_path=Path(zip_path),
        output_dir=output_dir,
        snapshot_date=snapshot_date,
        stringify=True,
    )
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
