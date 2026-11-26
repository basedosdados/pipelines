"""Prefect 3 tasks for br_ibama_fiscalizacao — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_ibama_fiscalizacao.utils import (
    clean_all,
    download_all,
)


@task(retries=2, retry_delay_seconds=120)
def download_ibama(work_dir: str) -> str:
    """Download the three Ibama source files into ``<work_dir>/input``.

    Retries twice: Ibama regenerates the files in place every night, and a fetch
    that overlaps the rewrite can be cut short.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    return str(download_all(Path(work_dir) / "input"))


@task
def clean_ibama(work_dir: str, input_dir: str) -> dict:
    """Build both staging tables under ``<work_dir>/output``.

    Returns:
        The :func:`utils.clean_all` result with Path values stringified, for
        Prefect result serialization.
    """
    result = clean_all(Path(input_dir), Path(work_dir) / "output")
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
