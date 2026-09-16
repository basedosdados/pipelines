"""Prefect 3 tasks for us_epu_gpr — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_epu_gpr.utils import clean_all, download_all


@task(retries=2, retry_delay_seconds=30)
def download_epu_gpr(work_dir: str) -> str:
    """Download every EPU and GPR source file.

    Retries twice: policyuncertainty.com occasionally rate-limits and the GPR
    workbooks are a few MB each.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_epu_gpr(work_dir: str, input_dir: str) -> dict:
    """Rebuild the three partitioned tables from the downloaded sources.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded files, from
            :func:`download_epu_gpr`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"`` — the latest ``"YYYY-MM"`` present in the monthly
        table, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
