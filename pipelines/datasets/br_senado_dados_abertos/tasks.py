"""Prefect 3 tasks for br_senado_dados_abertos — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_senado_dados_abertos.utils import (
    clean_all,
    recent_window,
)


@task(retries=2, retry_delay_seconds=60)
def extract_clean(work_dir: str, prior_years: int = 1) -> dict:
    """Extract from the Senate API and build every table under ``work_dir``.

    Dimensions are rebuilt in full; the time-series tables are re-extracted for
    the recent window (current year + ``prior_years``). Uploading only those
    partitions with ``dump_mode="append"`` replaces them in staging and leaves
    the historical partitions untouched. Retries twice — the dados-abertos API
    intermittently drops connections on the larger year ranges.

    Args:
        work_dir: Directory to build into; tables land under ``<work_dir>/output``.
        prior_years: Earlier years (beyond the current one) to re-extract for
            the time-series tables, to pick up late edits.

    Returns:
        ``{table_slug: output_dir}`` plus ``"max_data_sessao"`` — the latest
        ``votacao.data_sessao`` (``YYYY-MM-DD``), anchoring the source update.
    """
    out_root = str(Path(work_dir) / "output")
    return clean_all(out_root, years=recent_window(prior_years))
