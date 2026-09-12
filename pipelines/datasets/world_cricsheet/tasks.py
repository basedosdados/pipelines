"""Prefect 3 tasks for world_cricsheet — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.world_cricsheet.utils import clean_all, download_bundle


@task(retries=2, retry_delay_seconds=60)
def download_cricsheet(work_dir: str) -> str:
    """Download + stage the Cricsheet bundle and person registry.

    Retries twice: the ~114 MB bundle download occasionally drops.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_bundle(input_dir)
    return str(input_dir)


@task
def clean_cricsheet(work_dir: str, input_dir: str) -> dict:
    """Build the four partitioned tables from the staged bundle.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Staged bundle directory, from :func:`download_cricsheet`.

    Returns:
        A mapping of table slug to its partitioned output directory (as strings),
        plus ``"max_start_date"`` — the latest match ``"YYYY-MM-DD"`` in the
        bundle, which drives the source-update poll — and ``"counts"``.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
