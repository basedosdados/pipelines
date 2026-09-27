"""Prefect tasks for world_noaa_ghcn.

Thin wrappers over the pure functions in ``utils``; the transform itself lives
there so the one-shot onboarding and this pipeline run identical code.
"""

from pathlib import Path

from prefect import task

from pipelines.datasets.world_noaa_ghcn import utils


@task(retries=2, retry_delay_seconds=60)
def download_and_clean(work_dir: str, years: list[int]) -> dict[str, str]:
    """Download the requested years plus the station metadata, and clean them.

    Args:
        work_dir: Scratch directory for this run.
        years: Year-partitions to rebuild.

    Returns:
        Mapping of table slug to the local path to upload, as strings so the
        result survives Prefect's result serialisation.
    """
    root = Path(work_dir)
    paths = utils.clean_all(root / "input", root / "output", years)
    return {table: str(path) for table, path in paths.items()}


@task(retries=2, retry_delay_seconds=30)
def source_max_date(work_dir: str, years: list[int]) -> str:
    """Latest observation date NCEI has published, as ``YYYY-MM-DD``.

    Args:
        work_dir: Scratch directory for this run.
        years: Years just written.

    Returns:
        The source's maximum coverage date.
    """
    return utils.max_observation_date(Path(work_dir) / "output", years)


@task(retries=2, retry_delay_seconds=30)
def log_source_version() -> str:
    """Record NCEI's published version string in the run log.

    GHCN-Daily carries a version like ``3.34-upd-2026090918`` whose suffix is
    the UTC hour the last update started. It is the only cheap signal that the
    archive moved: every ``by_year`` file is re-stamped on each weekly
    reconstruction, so file modification times cannot distinguish a changed
    year from an unchanged one.

    Returns:
        The version string.
    """
    version = utils.source_version()
    print(f"GHCN-Daily source version: {version}")
    return version
