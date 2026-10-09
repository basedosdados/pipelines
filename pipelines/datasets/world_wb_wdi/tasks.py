"""Prefect 3 tasks for world_wb_wdi — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.world_wb_wdi.utils import clean_all, download_source


@task(retries=2, retry_delay_seconds=60)
def download_wdi(work_dir: str) -> str:
    """Download and extract WDI_CSV.zip from the World Bank.

    Retries twice: the ~270MB archive occasionally drops mid-download.

    Args:
        work_dir: Directory to download into; CSVs land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_source(input_dir)
    return str(input_dir)


@task
def clean_wdi(work_dir: str, input_dir: str) -> dict:
    """Build the six partitioned tables from the extracted WDI CSVs.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the extracted CSVs, from :func:`download_wdi`.

    Returns:
        A mapping of table slug to its output directory (as strings), plus
        ``"max_year"`` — the latest year in ``data``, which drives the
        source-update poll — and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
