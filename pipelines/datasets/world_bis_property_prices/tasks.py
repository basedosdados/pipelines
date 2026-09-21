"""Prefect 3 tasks for world_bis_property_prices — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.world_bis_property_prices.utils import (
    clean_all,
    download_flatfile,
)


@task(retries=2, retry_delay_seconds=30)
def download_bis(work_dir: str) -> str:
    """Download and extract the BIS selected property prices flat CSV.

    Retries twice: the BIS Data Portal occasionally drops the connection on the
    bulk zip.

    Args:
        work_dir: Directory to download into; the CSV lands in
            ``<work_dir>/input``.

    Returns:
        The input directory path (Prefect serializes task results as strings).
    """
    input_dir = Path(work_dir) / "input"
    download_flatfile(input_dir)
    return str(input_dir)


@task
def clean_bis(work_dir: str, input_dir: str) -> dict:
    """Build the partitioned ``price_index`` table from the downloaded CSV.

    Args:
        work_dir: Directory to write into; the table lands under
            ``<work_dir>/output``.
        input_dir: Directory holding the downloaded CSV, from
            :func:`download_bis`.

    Returns:
        A mapping of ``"price_index"`` to its partitioned output directory, plus
        ``"max_year_quarter"`` and ``"max_year_month"`` — the latest period, the
        second of which (year-MONTH, month = quarter*3) drives the source poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
