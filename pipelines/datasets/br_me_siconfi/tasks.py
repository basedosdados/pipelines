"""Prefect 3 tasks for br_me_siconfi — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_me_siconfi import utils


@task(retries=2, retry_delay_seconds=60)
def download(
    work_dir: str,
    start_year: int,
    end_year: int,
    levels: tuple[str, ...],
    workers: int = 1,
) -> str:
    """Download the SICONFI DCA for the trailing window.

    Retries twice: the Tesouro API intermittently rate-limits / drops
    connections over a long download. The download is resumable (existing files
    are skipped), so a retry resumes rather than restarts.

    Args:
        work_dir: Run scratch directory; JSON lands under ``<work_dir>/input/api``.
        start_year: First window year (inclusive).
        end_year: Last window year (inclusive).
        levels: Government levels to download.
        workers: Parallel download threads. Default 1; raising it speeds the
            município-heavy full window at the cost of a higher .gov request rate.

    Returns:
        The ``input/api`` directory path.
    """
    return utils.download_window(
        work_dir, start_year, end_year, levels, workers=workers
    )


@task
def clean(
    work_dir: str,
    api_dir: str,
    start_year: int,
    end_year: int,
    levels: tuple[str, ...],
    use_cache: bool,
    cache_bucket: str,
) -> dict:
    """Build the window, convert to staging parquet, and assemble full tables.

    Fails loud on crosswalk gaps (raises with the offending keys). See
    :func:`pipelines.datasets.br_me_siconfi.utils.assemble`.

    Args:
        work_dir: Run scratch directory.
        api_dir: Downloaded JSON directory, from :func:`download`.
        start_year: First window year (inclusive).
        end_year: Last window year (inclusive).
        levels: Government levels to build.
        use_cache: Union out-of-window years from the GCS parquet cache.
        cache_bucket: Bucket holding the cache (matches the upload bucket).

    Returns:
        ``{table: parquet_dir}`` per non-empty table, plus ``"max_year"``.
    """
    result = utils.assemble(
        work_dir,
        api_dir,
        start_year,
        end_year,
        levels,
        use_cache,
        cache_bucket,
    )
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
