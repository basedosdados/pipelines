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


@task(retries=2, retry_delay_seconds=30)
def archive(work_dir: str, bucket_name: str) -> int:
    """Archive the freshly downloaded raw API JSON to the bucket's raw/ prefix.

    Provenance copy (one gzip tarball per year). See
    :func:`pipelines.datasets.br_me_siconfi.utils.archive_raw`.

    Args:
        work_dir: Run scratch directory holding ``input/api``.
        bucket_name: Target bucket (matches the materialization bucket).

    Returns:
        Number of year tarballs uploaded.
    """
    return utils.archive_raw(work_dir, bucket_name)


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


@task
def seed_legacy_cache(
    work_dir: str,
    raw_bucket: str,
    raw_prefix: str,
    cache_bucket: str,
) -> dict:
    """One-time: seed the parquet cache with 1989-2012 legacy from raw Excel.

    See :func:`pipelines.datasets.br_me_siconfi.utils.seed_legacy_cache`.

    Args:
        work_dir: Run scratch directory.
        raw_bucket: Bucket holding the raw legacy Excel.
        raw_prefix: Prefix of the raw legacy Excel.
        cache_bucket: Bucket whose parquet cache is seeded.

    Returns:
        ``{table: cache_dir}`` for each legacy table seeded.
    """
    return utils.seed_legacy_cache(
        work_dir, raw_bucket, raw_prefix, cache_bucket
    )


@task
def seed_cache_from_bq(
    cache_bucket: str,
    tables: list,
    start_year: int,
    end_year: int,
    bq_project: str = "basedosdados",
) -> dict:
    """One-time: seed the cache for API years (2013+) from the prod BQ tables.

    See :func:`pipelines.datasets.br_me_siconfi.utils.seed_cache_from_bq`.

    Args:
        cache_bucket: Bucket whose cache is seeded.
        tables: Table slugs to seed.
        start_year: First year (inclusive).
        end_year: Last year (inclusive), i.e. window_start - 1.
        bq_project: Project holding the prod tables.

    Returns:
        ``{table: cache_dir}`` for each table seeded.
    """
    return utils.seed_cache_from_bq(
        cache_bucket, tables, start_year, end_year, bq_project
    )
