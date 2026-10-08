"""Flows for au_ato_abr — Prefect 3.

Australian Business Register "ABN Bulk Extract" (data.gov.au, ATO). The source
republishes a **full snapshot weekly**. We stack snapshots (CNPJ model): each run
uploads the new snapshot to staging with ``dump_mode="overwrite"`` and the
**incremental** dbt models append its ``extraction_date`` partition to the prod
tables, so history accumulates.

Migrated to the staged pipeline: check_update -> extract_and_load ->
build_and_promote. Unlike the usual one-pipeline-per-table_id shape (see
``pipelines.utils.stage_dispatch.pipeline_factory``), this dataset has only
ONE check_update and ONE extract_and_load for the whole dataset: the two
source ZIPs build all 4 tables (entity, other_name, dgr, dicionario) in one
streaming ``lxml.iterparse`` pass (see the banner in ``tasks.py``), so
``extract_and_load`` loops over ``extract_load_data``'s per-table results and
dispatches one ``build_and_promote`` per table, using the stage_dispatch
building blocks directly instead of ``CheckThenExtractLoadPipeline``.

check_update polls cheaply first (an HTTP HEAD on the ZIPs, compared against
``Table.Update.latest`` — see ``get_latest_update``'s docstring in tasks.py for
why ``table_update`` rather than ``coverage``) and only dispatches
extract_and_load — which downloads the ~1 GB payload — when the source has
actually republished, so a scheduled run is a cheap no-op between weekly
releases.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool activates
it (deployed paused).
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.au_ato_abr.constants import constants
from pipelines.datasets.au_ato_abr.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    Etapa,
    check_update_and_dispatch,
    deploy_tags,
    discover_partition_folders,
    dispatch_build_and_promote,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table, upload_to_gcs

DATASET_ID = constants.DATASET_ID.value
CORE_TABLE = constants.CORE_TABLE.value

# Deployment name of `au_ato_abr_extract_and_load` below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`). Passed
# explicitly to `check_update_and_dispatch` since it isn't literally
# `"extract_and_load"` (the `Etapa` value `deployment_name()` defaults to).
_EXTRACT_AND_LOAD_DEPLOYMENT = "au_ato_abr_extract_and_load"


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def au_ato_abr_check_update() -> bool:
    """Poll the ABN Bulk Extract ZIPs for a newer weekly snapshot, dispatch extract_and_load.

    Returns:
        `True` if a newer snapshot was found (and `extract_and_load` was
        dispatched); `False` otherwise.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=CORE_TABLE
    )

    result = get_latest_update()

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=CORE_TABLE,
        reference_date=result.reference_date,
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        extra_download_params=result.extra_download_params,
        compare_against=result.compare_against,
    )


au_ato_abr_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# The source republishes weekly; the exact weekday drifts, so poll on several
# days at 16:00 BRT. The HEAD-based source poll no-ops (no download) until a new
# snapshot actually appears.
au_ato_abr_check_update.deploy_schedules = [
    Cron("30 16 * * 1,2,3,4", timezone="America/Sao_Paulo")
]


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def au_ato_abr_extract_and_load(download_params: dict) -> None:
    """Download + clean the weekly snapshot, upload every table, dispatch promotion.

    One pair of ZIPs produces all 4 tables at once (see `tasks.py`), so this
    loops over `extract_load_data`'s per-table results instead of relying on
    `CheckThenExtractLoadPipeline.run_extract_and_load` (built for exactly one
    `ExtractAndLoad` per call).

    Args:
        download_params: dict received from check_update via
            `run_deployment()` — `reference_date` (the source's HTTP
            publication date; see `tasks.extract_load_data` for why it's
            unused here).
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=CORE_TABLE
    )

    # work_dir is created/cleaned up here, not inside extract_load_data: its
    # returned ExtractAndLoad.data_paths point inside it and are only read
    # (via upload_to_gcs) below — cleaning it up before that would delete
    # them first.
    work_dir = tempfile.mkdtemp(prefix="au_ato_abr_")
    try:
        results = extract_load_data(work_dir, download_params)
        for table_id, result in results.items():
            result.partition_folders = discover_partition_folders(
                result.data_path
            )
            upload_to_gcs(
                data_path=result.data_path,
                dataset_id=DATASET_ID,
                table_id=table_id,
                bucket_name="basedosdados-dev",
                dump_mode=result.dump_mode,
                source_format=result.source_format,
            )
            dispatch_build_and_promote(
                dataset_id=DATASET_ID,
                table_id=table_id,
                result=result,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


au_ato_abr_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# The clean step streams from the ZIPs and flushes in 400k-row chunks, but the
# download is ~1 GB; give the worker headroom.
au_ato_abr_extract_and_load.job_variables = {"memory": "8Gi"}
