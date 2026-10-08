"""Flows for au_geoscape_gnaf — Prefect 3.

Geoscape **G-NAF** (Geocoded National Address File, data.gov.au). The source
republishes a **full snapshot quarterly** (Feb/May/Aug/Nov). We stack snapshots
(CNPJ model): each run uploads the new snapshot to staging with
``dump_mode="overwrite"`` and the **incremental** dbt models append its
``snapshot_date`` partition to the prod tables, so history accumulates.

G-NAF is Open-G-NAF/CC-BY, so every table is ``AllFree`` — no BD Pro rolling
window, no Row Access Policies. The quarterly cadence is well below the
monthly-or-more paywall threshold.

Migrated to the staged pipeline: check_update -> extract_and_load ->
build_and_promote. Unlike the usual one-pipeline-per-table_id shape (see
``pipelines.utils.stage_dispatch.pipeline_factory``), this dataset has only
ONE check_update and ONE extract_and_load for the whole dataset: a single
release zip builds all 4 tables (address_detail, street_locality, locality,
dicionario) in one pass (see the banner in ``tasks.py``), so
``extract_and_load`` loops over ``extract_load_data``'s per-table results and
dispatches one ``build_and_promote`` per table, using the stage_dispatch
building blocks directly instead of ``CheckThenExtractLoadPipeline``.

check_update resolves the current release from the CKAN API and polls cheaply
first (the resolved ``snapshot_date`` vs the free ``Coverage``), only
dispatching extract_and_load — which downloads the ~1.6 GB payload — when a
newer quarterly snapshot has actually been published, so a scheduled run is a
cheap no-op between quarterly releases.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool activates
it (deployed paused).
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.au_geoscape_gnaf.constants import constants
from pipelines.datasets.au_geoscape_gnaf.tasks import (
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

# Deployment name of `au_geoscape_gnaf_extract_and_load` below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`). Passed
# explicitly to `check_update_and_dispatch` since it isn't literally
# `"extract_and_load"` (the `Etapa` value `deployment_name()` defaults to).
_EXTRACT_AND_LOAD_DEPLOYMENT = "au_geoscape_gnaf_extract_and_load"


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def au_geoscape_gnaf_check_update() -> bool:
    """Poll CKAN for a new quarterly G-NAF release, dispatch extract_and_load.

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


au_geoscape_gnaf_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# The source republishes quarterly (Feb/May/Aug/Nov), landing mid-month; the
# exact day drifts (the Aug 2026 release landed on the 17th). Poll on several
# days across the second half of each release month at 16:00 BRT. The
# coverage-based source poll no-ops (no download) until a new snapshot appears.
au_geoscape_gnaf_check_update.deploy_schedules = [
    Cron("35 16 14,17,20,23,26 2,5,8,11 *", timezone="America/Sao_Paulo")
]


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def au_geoscape_gnaf_extract_and_load(download_params: dict) -> None:
    """Download + clean the release, upload every table, dispatch promotion.

    One download produces all 4 tables at once (see `tasks.py`), so this
    loops over `extract_load_data`'s per-table results instead of relying on
    `CheckThenExtractLoadPipeline.run_extract_and_load` (built for exactly one
    `ExtractAndLoad` per call).

    Args:
        download_params: dict received from check_update via
            `run_deployment()` — `reference_date` (the snapshot date) and
            `url` (the resolved CKAN download link).
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=CORE_TABLE
    )

    # work_dir is created/cleaned up here, not inside extract_load_data: its
    # returned ExtractAndLoad.data_paths point inside it and are only read
    # (via upload_to_gcs) below — cleaning it up before that would delete
    # them first.
    work_dir = tempfile.mkdtemp(prefix="au_geoscape_gnaf_")
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


au_geoscape_gnaf_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# The clean step builds one state's frames at a time (NSW is the largest) and the
# download is ~1.6 GB; give the worker headroom.
au_geoscape_gnaf_extract_and_load.job_variables = {"memory": "16Gi"}
