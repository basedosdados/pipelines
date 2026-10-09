"""Prefect 3 tasks for fr_meteofrance — thin wrappers over utils.py."""

from datetime import UTC, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.fr_meteofrance import clim_schema, clim_utils
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


@task(retries=2, retry_delay_seconds=60)
def download_climatologie_task(work_dir: str) -> str:
    """Download the full daily and monthly climatological archives.

    Every period slice is fetched, not just the one that changes: the station
    register is the union over both series and all slices, so rebuilding it from
    the refreshed slice alone would drop stations that stopped reporting.

    Args:
        work_dir: Directory to download into; files land in ``<work_dir>/input``.

    Returns:
        The input directory path, as a string.
    """
    input_dir = Path(work_dir) / "input"
    clim_utils.INPUT = input_dir
    for kind in ("mens", "quot"):
        clim_utils.download(kind)
    return str(input_dir)


@task
def clean_climatologie_task(work_dir: str, input_dir: str) -> dict:
    """Clean the climatological archive into partitioned parquet.

    Only the ``latest-<years>`` slice is re-cleaned. Météo-France rewrites that
    slice alone as observations land, and staging objects are named
    ``<dept>_<period>.parquet``, so uploading it overwrites those objects in
    place and leaves the two historical slices on GCS untouched. Re-cleaning all
    of it would mean 137 million rows a month to rewrite the same bytes.

    ``poste`` is the exception and is always rebuilt in full — see
    :func:`download_climatologie_task`.

    Args:
        work_dir: Directory to write into; parquet lands in ``<work_dir>/output``.
        input_dir: Directory holding the downloaded ``.csv.gz`` archives.

    Returns:
        Mapping of table slug to the parquet directory for that table.
    """
    output_dir = Path(work_dir) / "output"
    clim_utils.INPUT = Path(input_dir)
    clim_utils.OUTPUT = output_dir

    descriptors = clim_schema.descriptors()
    sink: dict = {}
    mens = clim_utils.clean_mens(descriptors, sink, only_latest=True)
    quot = clim_utils.clean_quot(descriptors, sink, only_latest=True)
    poste = clim_utils.build_poste()
    print(
        f"climatologie: quotidienne {quot:,} rows, mensuelle {mens:,} rows, "
        f"poste {poste:,} rows (latest slice only; poste rebuilt in full)"
    )
    return {
        "poste": str(output_dir / "poste"),
        "mensuelle": str(output_dir / "mensuelle"),
        "quotidienne": str(output_dir / "quotidienne"),
    }


@task
def max_climatologie_date(quotidienne_dir: str) -> str:
    """Latest observation date in the refreshed daily slice, as ``YYYY-MM-DD``.

    Read from the cleaned parquet rather than from BigQuery, so the poll
    compares what the source just published against what is registered. Staging
    is all-STRING by house convention, so ``date`` is already an ISO string and
    sorts lexicographically.

    Args:
        quotidienne_dir: Directory of cleaned ``quotidienne`` parquet.

    Returns:
        The maximum observation date.
    """
    import pyarrow.parquet as pq

    # Read the column's row-group statistics rather than the data: the footer
    # already carries per-group max values, so this touches no row.
    latest = None
    for path in sorted(Path(quotidienne_dir).glob("*.parquet")):
        metadata = pq.ParquetFile(path).metadata
        index = metadata.schema.names.index("date")
        for group in range(metadata.num_row_groups):
            stats = metadata.row_group(group).column(index).statistics
            value = stats.max if stats is not None else None
            if value and (latest is None or value > latest):
                latest = value
    if latest is None:
        raise ValueError(f"no observation dates found under {quotidienne_dir}")
    return latest
