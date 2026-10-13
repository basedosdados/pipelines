"""Prefect 3 tasks for br_cgu_orcamento_publico — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_cgu_orcamento_publico.utils import (
    clean_all,
    download_all,
    probe_latest,
)


@task(retries=2, retry_delay_seconds=300)
def probe_source() -> str:
    """Return when the portal last regenerated the exercise files, ``YYYY-MM-DD``.

    One HEAD request. The exercise year itself is useless as a freshness signal
    — it moves once a year while the file contents move continuously — so the
    poll compares this publication timestamp against ``Table.Update.latest``.

    Retries are spaced five minutes apart because the failure worth retrying is
    an AWS WAF rate block, which a fast retry only deepens.
    """
    return probe_latest()


@task(retries=2, retry_delay_seconds=600)
def download_orcamento(work_dir: str) -> dict:
    """Download every published exercise ZIP into ``<work_dir>/input``.

    Returns:
        ``{"input_dir": str, "years": [int, ...]}`` — the exercises the portal
        actually published, discovered by walking forward until one is missing.
    """
    input_dir = Path(work_dir) / "input"
    years = download_all(input_dir)
    return {"input_dir": str(input_dir), "years": years}


@task
def clean_orcamento(work_dir: str, input_dir: str, years: list[int]) -> dict:
    """Clean the downloaded exercises into hive-partitioned staging CSV.

    Returns:
        ``{"path": str, "rows": {year: n}, "total": n}`` — ``path`` is the
        table directory handed to ``upload_to_gcs``.
    """
    result = clean_all(Path(input_dir), Path(work_dir) / "output", years)
    return {**result, "path": str(result["path"])}
