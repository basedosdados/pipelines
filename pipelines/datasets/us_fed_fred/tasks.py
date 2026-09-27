"""Prefect 3 tasks for us_fed_fred — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_fed_fred.utils import clean_all, download_all


@task(retries=2, retry_delay_seconds=30)
def download_fred(work_dir: str, limit: int | None = None) -> str:
    """Fetch the seed series from FRED and persist the kept ones as raw JSON.

    Retries twice: the FRED API intermittently rate-limits or drops a connection
    across the ~150 calls a full run makes.

    Args:
        work_dir: Directory to download into; JSON lands in ``<work_dir>/input``.
        limit: If given, only process the first ``limit`` seed series (smoke test).

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir, limit=limit)
    return str(input_dir)


@task
def clean_fred(work_dir: str, input_dir: str) -> dict:
    """Build the observation and series tables from the downloaded raw JSON.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the raw JSON, from :func:`download_fred`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_date"`` — the latest ``YYYY-MM-DD`` observation date, which drives
        the source-update poll — and the per-table row counts.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
