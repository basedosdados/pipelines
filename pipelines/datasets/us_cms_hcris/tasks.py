"""Prefect tasks for us_cms_hcris — thin wrappers over the pure functions.

Every one of these delegates to ``utils``, which the one-shot onboarding under
``models/us_cms_hcris/code/`` imports too, so the refresh and the bootstrap
cannot drift.
"""

from pathlib import Path

import duckdb
from prefect import task

from pipelines.datasets.us_cms_hcris.constants import constants
from pipelines.datasets.us_cms_hcris.utils import (
    clear_staging_prefix,
    download_extract,
    list_extracts,
    source_last_modified,
)
from pipelines.utils.utils import log

FORMS: dict = constants.FORMS.value


@task
def list_extracts_task(last_year: int) -> list[tuple[str, int]]:
    """Enumerate the fiscal-year extracts CMS currently publishes.

    Probed rather than hardcoded: CMS adds a federal fiscal year every October,
    and a hardcoded end year would silently stop ingesting the newest one.

    Args:
        last_year: Highest federal fiscal year to probe.

    Returns:
        ``(form, year)`` pairs, 2552-96 before 2552-10.
    """
    extracts = list_extracts(last_year)
    log(f"{len(extracts)} extracts published, {extracts[0]} to {extracts[-1]}")
    return extracts


@task
def source_max_date_task(extracts: list[tuple[str, int]]) -> str:
    """Return the newest publication date across the archives, ``YYYY-MM-DD``.

    See ``utils.source_last_modified`` for why this, and not the source's
    maximum coverage date, is the freshness signal for HCRIS.

    Args:
        extracts: ``(form, year)`` pairs to probe.

    Returns:
        The newest ``Last-Modified`` date.
    """
    stamp = source_last_modified(extracts)
    log(f"source last published {stamp}")
    return stamp


@task
def download_and_clean_task(
    extracts: list[tuple[str, int]],
    input_dir: str,
    output_dir: str,
    keep_archives: bool = False,
) -> dict[str, int]:
    """Download, clean and discard one extract at a time.

    Interleaved rather than download-all-then-clean-all so peak disk stays near
    one archive plus one unpacked extract (about 1.6 GB) rather than the 3.5 GB
    of archives plus 16 GB unpacked the whole series needs. The cleaned parquet
    accumulates either way, at about 3.2 GB.

    Args:
        extracts: ``(form, year)`` pairs.
        input_dir: Directory to download into.
        output_dir: Root output directory.
        keep_archives: Keep each archive after cleaning it. False on the
            worker; True is useful locally, where re-cleaning is common.

    Returns:
        Rows written per table.

    Raises:
        RuntimeError: If a table came out empty, which would otherwise replace
            a complete history with a truncated one.
    """
    import shutil

    from pipelines.datasets.us_cms_hcris.utils import (
        FORM_TAG,
        clean_extract,
        unpack_extract,
    )

    inp, out = Path(input_dir), Path(output_dir)
    con = duckdb.connect()
    totals: dict[str, int] = {}
    for form, year in extracts:
        archive = download_extract(form, year, inp)
        stage = inp / "_work" / f"{FORM_TAG[form]}_{year}"
        try:
            parts = unpack_extract(archive, form, year, stage)
            counts = clean_extract(parts, form, year, out, con=con)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
            if not keep_archives:
                archive.unlink(missing_ok=True)
        for table, rows in counts.items():
            totals[table] = totals.get(table, 0) + rows
        log(
            f"{form} FY{year}: "
            + ", ".join(f"{t} {n:,}" for t, n in counts.items())
        )

    for table, rows in sorted(totals.items()):
        log(f"{table}: {rows:,} rows total")
    empty = [t for t, n in totals.items() if n == 0]
    if empty:
        raise RuntimeError(f"cleaned nothing for {empty} — refusing to upload")
    return totals


@task
def clear_staging_task(bucket_name: str, table_id: str) -> int:
    """Delete a table's staging prefix before the run re-uploads it.

    CMS republishes the whole history every quarter, so every run rebuilds
    every partition. Clearing first makes that a replacement: without it a
    partition that needed two part files last quarter and one this quarter
    keeps the stale second file and double counts those rows.

    Args:
        bucket_name: GCS bucket, also the billing project.
        table_id: Table slug.

    Returns:
        Blobs deleted.
    """
    n = clear_staging_prefix(bucket_name, constants.DATASET_ID.value, table_id)
    log(f"{table_id}: cleared {n} blobs from gs://{bucket_name}/staging/…")
    return n
