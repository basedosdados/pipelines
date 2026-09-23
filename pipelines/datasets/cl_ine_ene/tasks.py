"""Prefect tasks for cl_ine_ene — thin wrappers over the pure functions in utils."""

from __future__ import annotations

import pathlib

from prefect import task

from pipelines.datasets.cl_ine_ene import utils
from pipelines.datasets.cl_ine_ene.constants import constants


@task
def probe_source_max_period() -> str:
    """The newest moving quarter INE publishes, as ``YYYY-MM``."""
    # Start the walk a year back so a stalled source still resolves.
    year, month = utils.source_max_period((2026, 1))
    return f"{year}-{month:02d}"


@task
def anchor_fingerprint(work_dir: str) -> dict:
    """Row count and weight total of the anchor period, to catch a back-series rewrite."""
    year, month = constants.ANCHOR_PERIOD.value
    path = utils.download_period(
        year, month, pathlib.Path(work_dir) / "anchor"
    )
    frame = utils.read_period(path)
    weights = (
        frame["fact_cal"].str.replace(",", ".", regex=False).astype(float)
    )
    return {
        "period": f"{year}-{month:02d}",
        "rows": len(frame),
        # Rounded so float noise cannot masquerade as a recalibration.
        "weight_total": round(weights.sum(), 3),
    }


@task
def last_ingested_period(bq_project: str) -> str | None:
    """Newest moving quarter already in the destination table, as ``YYYY-MM``.

    Returns None when the table does not exist yet, which means the first run and
    therefore the whole back-series. Without this the flow would start each
    incremental run at the source's newest quarter, and any period that appeared
    while the flow was paused or failing would be skipped permanently — the poll
    only reports THAT the source is newer, not how much was missed.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=bq_project)
    table = (
        f"{bq_project}.{constants.DATASET_ID.value}.{constants.TABLE_ID.value}"
    )
    try:
        client.get_table(table)
    except Exception:
        print(f"{table} does not exist yet; ingesting the full back-series")
        return None
    row = next(
        iter(
            client.query(
                f"select max(ano * 100 + mes) as period from `{table}`"
            ).result()
        )
    )
    if row["period"] is None:
        return None
    year, month = divmod(int(row["period"]), 100)
    print(f"{table} already holds up to {year}-{month:02d}")
    return f"{year}-{month:02d}"


@task
def download_and_clean(work_dir: str, first: str, last: str) -> dict:
    """Download the requested periods and write them as partitioned parquet."""
    work = pathlib.Path(work_dir)
    input_dir, output_dir = (
        work / "input",
        work / "output" / constants.TABLE_ID.value,
    )

    def parse(value: str) -> tuple[int, int]:
        year, month = value.split("-")
        return int(year), int(month)

    wanted = utils.periods(parse(first), parse(last))
    for year, month in wanted:
        utils.download_period(year, month, input_dir)
    counts = utils.clean_all(input_dir, output_dir, wanted)
    print(f"cleaned {len(counts)} periods, {sum(counts.values()):,} rows")
    return {
        "data_path": str(output_dir),
        "counts": counts,
        "periods": len(counts),
    }
