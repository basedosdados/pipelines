"""
Flows for us_bls_oes — Prefect 3.

US Occupational Employment and Wage Statistics (BLS). OEWS publishes once a year,
each release covering a May reference period, and never restates an earlier year.
Each run therefore cleans and appends **only the new year's partition**
(``dump_mode="append"``) rather than rebuilding the panel: the staging object path
carries the partition, so re-running a year overwrites that partition and leaves
every earlier one untouched.

The `dicionario` table is deliberately not refreshed here — its temporal-coverage
column is computed over the whole panel, which a single-year run does not hold.
`clean_oes` instead asserts that the new year introduces no unlabelled code, and
fails the run if it does (see `utils.assert_dictionary_labels`).

Migrated to the staged pipeline: check_update -> extract_and_load ->
build_and_promote. Unlike the usual one-pipeline-per-table_id shape (see
``pipelines.utils.stage_dispatch.pipeline_factory``), this dataset has only ONE
check_update and ONE extract_and_load for the whole dataset: a single release zip
builds both `area` and `industry` in one pass (see the banner in `tasks.py`), so
`extract_and_load` loops over `extract_load_data`'s per-table results and
dispatches one `build_and_promote` per table, using the stage_dispatch building
blocks directly instead of `CheckThenExtractLoadPipeline` — same shape as
`au_geoscape_gnaf`.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool activates
it.
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.us_bls_oes.constants import DATASET_ID, POLL_TABLE
from pipelines.datasets.us_bls_oes.tasks import (
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

# Deployment name of `us_bls_oes_extract_and_load` below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`). Passed
# explicitly to `check_update_and_dispatch` since it isn't literally
# `"extract_and_load"` (the `Etapa` value `deployment_name()` defaults to).
_EXTRACT_AND_LOAD_DEPLOYMENT = "us_bls_oes_extract_and_load"


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def us_bls_oes_check_update() -> bool:
    """Poll BLS for a newer OEWS reference year, dispatch extract_and_load.

    Returns:
        `True` if a newer reference year was found (and `extract_and_load` was
        dispatched); `False` otherwise.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    result = get_latest_update()

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        reference_date=result.reference_date,
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        date_format="%Y",
        extra_download_params=result.extra_download_params,
        compare_against=result.compare_against,
    )


us_bls_oes_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# OEWS releases once a year, in the northern spring, with the exact date varying
# between late March and May. Poll weekly across those three months; the
# source-poll guard no-ops until a new reference year actually appears.
us_bls_oes_check_update.deploy_schedules = [
    Cron("47 17 1,8,15,22,29 3,4,5 *", timezone="America/Sao_Paulo")
]


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def us_bls_oes_extract_and_load(download_params: dict) -> None:
    """Download + clean the release, upload both tables, dispatch promotion.

    One download builds both tables at once (see `tasks.py`), so this loops
    over `extract_load_data`'s per-table results instead of relying on
    `CheckThenExtractLoadPipeline.run_extract_and_load` (built for exactly one
    `ExtractAndLoad` per call).

    Args:
        download_params: dict received from check_update via
            `run_deployment()` — `reference_date` (`"<year>-05-01"`).
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_bls_oes_")
    try:
        results = extract_load_data(
            work_dir=work_dir, download_params=download_params
        )
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
        # Covers both a clean run and any exception. The k8s work pool gives
        # each run a fresh pod, but a process worker reuses its filesystem, and
        # one release is ~80 MB compressed. Cleanup happens here, after every
        # table has been uploaded — not inside `extract_load_data` — since the
        # `ExtractAndLoad.data_path`s it returns point inside `work_dir`.
        shutil.rmtree(work_dir, ignore_errors=True)


us_bls_oes_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# One release is ~430k rows held in pandas plus the Excel reader's own buffers.
us_bls_oes_extract_and_load.job_variables = {"memory": "8Gi"}
