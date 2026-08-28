"""Prefect 3 tasks for fr_meteofrance — thin wrappers over utils.py."""

from datetime import UTC, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.fr_meteofrance.constants import constants
from pipelines.datasets.fr_meteofrance.utils import (
    clean_normales,
    clean_station_synop,
    clean_synop,
    download_fiches,
    download_postes,
    download_synop_years,
    write_dicionario,
)


@task(retries=2, retry_delay_seconds=30)
def download_synop(work_dir: str, full_history: bool = False) -> str:
    """Download the SYNOP annual archives.

    Météo-France rewrites only the current year's file; every earlier year is
    frozen. The daily flow therefore fetches one file, and only the monthly flow
    pays for the full history (needed to recompute the station register's
    first/last observation years).

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.
        full_history: Fetch every year from 1996, rather than the current year only.

    Returns:
        The input directory path, as a string (Prefect serializes task results).
    """
    input_dir = Path(work_dir) / "input"
    current = datetime.now(UTC).year
    first = constants.SYNOP_FIRST_YEAR.value if full_history else current
    download_synop_years(input_dir, range(first, current + 1))
    download_postes(input_dir)
    return str(input_dir)


@task
def clean_synop_task(
    work_dir: str, input_dir: str, with_stations: bool = False
) -> dict:
    """Clean the downloaded SYNOP archives into ``annee=`` parquet partitions.

    Args:
        work_dir: Directory to write into; tables land under ``<work_dir>/output``.
        input_dir: Directory holding the downloaded archives, from :func:`download_synop`.
        with_stations: Also rebuild ``station_synop``. Only meaningful when the
            full history was downloaded, since the register carries each
            station's first and last observation year.

    Returns:
        A mapping of table slug to its output directory, plus ``"max_date"`` —
        the latest observation date, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_synop(Path(input_dir), output_dir)
    out = {
        "synop": str(result["synop"]),
        "max_date": result["max_date"],
        "rows": result["rows"],
    }
    if with_stations:
        path = clean_station_synop(
            result["stations"], Path(input_dir), output_dir
        )
        out["station_synop"] = str(path.parent)
    return out


@task(retries=2, retry_delay_seconds=60)
def download_fiches_task(work_dir: str) -> str:
    """Download every published climatological sheet (~1,576 small files)."""
    return str(download_fiches(Path(work_dir) / "input"))


@task
def clean_normales_task(work_dir: str, fiche_dir: str) -> dict:
    """Build the normals, the climatological station register and the dictionary.

    Returns:
        A mapping of table slug to its output directory, plus ``"max_date"`` —
        the latest sheet edition date, which drives the source-update poll.
    """
    output_dir = Path(work_dir) / "output"
    result = clean_normales(Path(fiche_dir), output_dir)
    return {
        "normale_climatologique": str(result["normale_climatologique"]),
        "station_climatologique": str(result["station_climatologique"]),
        "dicionario": str(write_dicionario(output_dir)),
        "max_date": result["max_date"],
        "rows": result["rows"],
    }
