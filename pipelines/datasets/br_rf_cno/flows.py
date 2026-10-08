"""Flows for br_rf_cno (Cadastro Nacional de Obras — Receita Federal) — Prefect 3.

The source publishes a single non-decomposable `cno.zip` (~306 MB) that
builds all 4 published tables (microdados, vinculos, areas, cnaes) in one
pass — see `pipelines/datasets/br_rf_cno/README.md` for the WAF access
workarounds and the per-table data quality notes.

Migrated to the staged pipeline: check_update -> extract_and_load ->
build_and_promote. Unlike the usual one-pipeline-per-table_id shape (see
`pipelines.utils.stage_dispatch.pipeline_factory`, used e.g. by
`br_ms_cnes`), this dataset has only ONE check_update and ONE
extract_and_load for the whole dataset: a single zip builds all 4 tables in
one pass (see the banner in `tasks.py`), so `extract_and_load` loops over
`extract_load_data`'s per-table results and dispatches one
`build_and_promote` per table, using the stage_dispatch building blocks
directly instead of `CheckThenExtractLoadPipeline` — same shape as
`au_geoscape_gnaf`, migrated earlier for the same one-download/N-tables
reason.

check_update reads the source zip's `Last-Modified` header (cheap, no
download) and polls that date against the free `Coverage` first, only
dispatching extract_and_load — which downloads the ~306 MB zip — when the
source actually has new data.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool
activates it (deployed paused).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_rf_cno.constants import constants
from pipelines.datasets.br_rf_cno.tasks import (
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

# Deployment name of `br_rf_cno_extract_and_load` below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`).
# Passed explicitly to `check_update_and_dispatch` since it isn't literally
# `"extract_and_load"` (the `Etapa` value `deployment_name()` defaults to).
_EXTRACT_AND_LOAD_DEPLOYMENT = "br_rf_cno_extract_and_load"


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def br_rf_cno_check_update() -> bool:
    """Poll the source zip's `Last-Modified` date, dispatch extract_and_load.

    Returns:
        `True` if a newer `data_extracao` was found (and `extract_and_load`
        was dispatched); `False` otherwise.
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


br_rf_cno_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# Same cron the old per-table flow used for `microdados` (the table this
# check_update is anchored on) — the old `vinculos`/`areas`/`cnaes` crons
# (:15/:25/:35) only existed to stagger 4 independent, redundant downloads of
# the same zip; a single shared check_update makes them unnecessary.
br_rf_cno_check_update.deploy_schedules = [
    Cron("5 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def br_rf_cno_extract_and_load(download_params: dict) -> None:
    """Download `cno.zip`, process every table, upload + dispatch promotion.

    One download produces all 4 tables at once (see `tasks.py`), so this
    loops over `extract_load_data`'s per-table results instead of relying on
    `CheckThenExtractLoadPipeline.run_extract_and_load` (built for exactly
    one `ExtractAndLoad` per call).

    Args:
        download_params: dict received from check_update via
            `run_deployment()` — `reference_date` (the source's
            `data_extracao`, `"YYYY-MM-DD"`, no time component).
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=CORE_TABLE
    )

    results = extract_load_data(download_params)
    for table_id, result in results.items():
        result.partition_folders = discover_partition_folders(result.data_path)
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


br_rf_cno_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
