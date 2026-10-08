"""Prefect 3 tasks for br_rf_cno — thin wrappers over the shared `rf` crawler
(`pipelines.crawler.rf`), plus the check_update/extract_and_load entry points
the staged pipeline dispatches through (see the banner above
`get_latest_update` below).
"""

from pipelines.crawler.rf.tasks import (
    check_need_for_update,
    crawl,
    process_file,
)
from pipelines.datasets.br_rf_cno.constants import COVERAGE, constants
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection

# ──────────────────────────────────────────────────────────────────────────────
# check_update / extract_and_load
#
# A single `cno.zip` (~306 MB) builds all 4 published tables (microdados,
# vinculos, areas, cnaes) at once — the source doesn't let you download just
# one table's slice (see README's *Sobre o Sistema*). Splitting this into 4
# independent check_update/extract_and_load pairs (the usual
# one-pipeline-per-table_id shape — see `pipeline_factory` in
# stage_dispatch.py) would redownload the whole zip up to 4x per cycle for no
# reason. So this dataset keeps ONE check_update + ONE extract_and_load for
# the whole dataset (anchored on `CORE_TABLE`), and `extract_load_data`
# returns one `ExtractAndLoad` per table — `flows.py` loops over them to
# upload + dispatch `build_and_promote` once per table, using the
# stage_dispatch building blocks directly instead of
# `CheckThenExtractLoadPipeline` (built for the one-table-per-call shape).
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    """Read the source zip's ``Last-Modified`` header (cheap, no download).

    Reuses `check_need_for_update` (`pipelines.crawler.rf.tasks`), unchanged
    from the old per-table flow (`pipelines.crawler.rf.flows._run_rf`): a
    `HEAD` on `cno.zip`, not a date derived from the file's contents — see
    README's *Acesso à fonte e o WAF*.

    Returns:
        `SourceInspection` with the resolved date as `reference_date`. No
        `extra_download_params`: the download URL is a fixed constant
        (`pipelines.crawler.rf.constants.constants.URLS`), not resolved per
        run, so there's nothing to round-trip through `extract_load_data`.
    """
    reference_date = check_need_for_update(
        dataset_id=constants.DATASET_ID.value
    )
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> dict[str, ExtractAndLoad]:
    """Download `cno.zip` once, process it into all 4 tables.

    Args:
        download_params: `{"reference_date": "<data_extracao>"}` —
            `reference_date` is `get_latest_update`'s resolved date,
            round-tripped through `check_update_and_dispatch`
            (`date.isoformat()`), as a plain `"YYYY-MM-DD"` string.

    Returns:
        One `ExtractAndLoad` per table in `constants.ALL_TABLES`, all
        sharing `dump_mode="append"`/`source_format="parquet"` — same as the
        old `_run_rf` (each run appends a full `data_extracao` snapshot; the
        incremental dbt models accumulate history, the staging dump itself
        doesn't need to retain it).

        `reference_date` is passed through to `process_file` as the plain
        date string (no time component): `process_file`/`process_chunk`
        write the partition folder as `data=<date>`, and a datetime instead
        of a date there made `SAFE_CAST(data AS DATE)` return NULL in the
        incremental dbt models, so the filter never inserted (see README's
        *Causa raiz do congelamento*, 2026-07-17 fix).
    """
    dataset_id = constants.DATASET_ID.value
    reference_date = download_params["reference_date"]

    crawl(dataset_id=dataset_id, input_dir="input")

    results = {}
    for table_id in constants.ALL_TABLES.value:
        output_path = process_file(
            dataset_id=dataset_id,
            table_id=table_id,
            input_dir="input",
            output_dir="output",
            partition_date=reference_date,
            chunksize=constants.CHUNKSIZE.value,
        )
        if output_path is None:
            raise RuntimeError(
                f"process_file não gerou saída para a tabela {table_id!r} "
                f"de {dataset_id} — arquivo não reconhecido em "
                "TABLES_RENAME ou falha durante o processamento (ver logs)."
            )
        results[table_id] = ExtractAndLoad(
            coverage=COVERAGE[table_id].model_dump(),
            data_path=output_path,
            dump_mode="append",
            source_format="parquet",
        )
    return results
