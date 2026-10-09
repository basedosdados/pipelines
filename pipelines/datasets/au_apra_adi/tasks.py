"""Prefect 3 tasks for au_apra_adi — thin wrappers over utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.au_apra_adi.utils import clean_all, download_workbook


def _yq_to_ym(year_quarter: str) -> str:
    """Convert ``"YYYY-Q"`` to the quarter-end month ``"YYYY-MM"``.

    APRA's coverage is stored as a ``YearQuarter`` column, which the backend
    formats as ``MAX(DATE(year, quarter * 3, 1))`` — i.e. a year-MONTH. The
    source poll compares a ``"%Y-%m"`` string against that, so the latest period
    must be reported at month granularity, not ``"YYYY-Q"``. Q1→03, Q4→12.
    """
    y, q = year_quarter.split("-")
    return f"{int(y)}-{int(q) * 3:02d}"


@task(retries=2, retry_delay_seconds=30)
def download_adi(work_dir: str) -> str:
    """Download the current APRA ADI performance workbook.

    Retries twice: apra.gov.au intermittently drops the connection on the larger
    workbook download.

    Args:
        work_dir: Directory to download into; the file lands in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    download_workbook(input_dir)
    return str(input_dir)


@task
def clean_adi(work_dir: str, input_dir: str) -> dict:
    """Build all eight tables from the downloaded workbook.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded workbook, from
            :func:`download_adi`.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"max_year_month"`` — the latest ``"YYYY-MM"`` quarter-end present,
        which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_all(Path(input_dir), output_dir)
    out = {
        k: (str(v) if isinstance(v, Path) else v) for k, v in result.items()
    }
    out["max_year_month"] = _yq_to_ym(result["max_year_quarter"])
    return out
