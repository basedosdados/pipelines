"""Prefect 3 tasks for au_rba_statistical_tables — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_rba_statistical_tables.utils import (
    clean_all,
    download_all,
    write_partitioned,
)


@task(retries=2, retry_delay_seconds=30)
def download_rba(work_dir: str) -> str:
    """Download every RBA statistical-table CSV.

    Retries twice: www.rba.gov.au occasionally drops a connection partway
    through the ~220-file sweep.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    saved = download_all(input_dir)
    print(f"downloaded {len(saved)} CSV files into {input_dir}")
    return str(input_dir)


@task
def clean_rba(work_dir: str, input_dir: str) -> dict:
    """Build the four tables from the downloaded CSVs.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded CSVs, from :func:`download_rba`.

    Returns:
        A mapping of table slug to its output directory, plus
        ``"max_publication_date"`` — the latest ``"YYYY-MM-DD"`` publication
        stamp across all series, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    cleaned = clean_all(Path(input_dir))
    counts = write_partitioned(cleaned, output_dir)

    for table, n in counts.items():
        print(f"  {table}: {n:,} rows")
    print(f"  excluded by licence gate: {len(cleaned['excluded']):,} series")

    result = {table: str(output_dir / table) for table in counts}
    result["max_publication_date"] = cleaned["max_publication_date"]
    return result
