"""Prefect 3 tasks for br_cgu_sancoes — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.br_cgu_sancoes.utils import clean_all, download_all


@task(retries=2, retry_delay_seconds=60)
def download_sancoes(work_dir: str) -> str:
    """Download the latest CGU sanction snapshots (CEIS/CNEP/CEPIM/Acordos).

    Retries twice: the portal generates each zip on demand behind AWS WAF and can
    intermittently return 202 past the readiness poll or drop the connection.

    Args:
        work_dir: Directory to download into; CSVs land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_all(input_dir)
    return str(input_dir)


@task
def clean_sancoes(work_dir: str, input_dir: str) -> dict:
    """Build the six partitioned tables from the downloaded registry CSVs.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the extracted CSVs, from
            :func:`download_sancoes`.

    Returns:
        A mapping of table slug to its output directory, plus ``"snapshots"`` and
        ``"max_date"`` (the latest extraction date, driving the source-update
        poll). Path values are stringified for Prefect result serialization.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    return {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
