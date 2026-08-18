"""Prefect 3 tasks for us_bea — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_bea.utils import clean_all


@task
def clean_bea(work_dir: str) -> dict:
    """Download all six BEA tables from the API and write partitioned parquet.

    The download and the cleaning are one step here: the BEA source is a REST
    API with no bulk archive to cache, so ``clean_all`` streams each table's
    rows straight to all-STRING parquet.

    Args:
        work_dir: Directory to write into; tables land under
            ``<work_dir>/output``.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"`` — the latest ``"YYYY-MM"`` in the monthly NIPA
        rows, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
