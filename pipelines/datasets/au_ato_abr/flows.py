"""Flows for au_ato_abr — Prefect 3.

Australian Business Register "ABN Bulk Extract" (data.gov.au, ATO). The source
republishes a **full snapshot weekly**. We stack snapshots (CNPJ model): each run
uploads the new snapshot to staging with ``dump_mode="overwrite"`` and the
**incremental** dbt models append its ``extraction_date`` partition to the prod
tables, so history accumulates.

Migrated to the one-pipeline-per-table_id shape: 4 independent check_update /
extract_and_load pairs (one per table — entity, other_name, dgr, dicionario),
wired through ``CheckThenExtractLoadPipeline``/``pipeline_factory`` (see
``tasks.py``). Each table's extract_and_load redownloads the two source ZIPs
and reruns the whole streaming ``lxml.iterparse`` pass from scratch, keeping
only its own table — redundant (a single pass already builds all 4 together,
see ``utils.clean_all``), but uniform 1-flow-per-table is the rule now.

check_update polls cheaply first (an HTTP HEAD on the ZIPs, compared against
``Table.Update.latest`` — see ``get_latest_update``'s docstring in tasks.py for
why ``table_update`` rather than ``coverage``) and only dispatches its own
extract_and_load — which downloads the ~1 GB payload — when the source has
actually republished, so a scheduled run is a cheap no-op between weekly
releases.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers all 8 flows
below; the dev pool ignores the check_update schedules, the prod pool
activates them (deployed paused).
"""

from prefect.schedules import Cron

from pipelines.datasets.au_ato_abr.constants import (
    DGR_TABLE_ID,
    DICIONARIO_TABLE_ID,
    ENTITY_TABLE_ID,
    OTHER_NAME_TABLE_ID,
    constants,
)
from pipelines.datasets.au_ato_abr.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value

# The source republishes weekly; the exact weekday drifts, so poll on several
# days at 16:00 BRT. The HEAD-based source poll no-ops (no download) until a
# new snapshot actually appears. Same schedule for all 4 tables — one shared
# weekly snapshot, same as before the per-table split.
_CHECK_UPDATE_SCHEDULE = [
    Cron("30 16 * * 1,2,3,4", timezone="America/Sao_Paulo")
]

# The clean step streams from the ZIPs and flushes in 400k-row chunks, but the
# download is ~1 GB per table (each extract_and_load redownloads it on its
# own); give the worker headroom.
_JOB_VARIABLES = {"memory": "8Gi"}

# ──────────────────────────────────────────────────────────────────────────────
# entity
# check_update: au_ato_abr__entity
# extract_and_load: au_ato_abr__entity
# ──────────────────────────────────────────────────────────────────────────────

_entity_pipeline = make_pipeline(ENTITY_TABLE_ID)


@flow(name=_entity_pipeline.check_update_flow_name, log_prints=True)
def au_ato_abr_entity_check_update() -> None:
    _entity_pipeline.run_check_update()


au_ato_abr_entity_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, ENTITY_TABLE_ID
)
au_ato_abr_entity_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_entity_pipeline.extract_and_load_flow_name, log_prints=True)
def au_ato_abr_entity_download(download_params: dict) -> None:
    _entity_pipeline.run_extract_and_load(download_params)


au_ato_abr_entity_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, ENTITY_TABLE_ID
)
au_ato_abr_entity_download.job_variables = _JOB_VARIABLES
_entity_pipeline.extract_load_deployment = (
    au_ato_abr_entity_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# other_name
# check_update: au_ato_abr__other_name
# extract_and_load: au_ato_abr__other_name
# ──────────────────────────────────────────────────────────────────────────────

_other_name_pipeline = make_pipeline(OTHER_NAME_TABLE_ID)


@flow(name=_other_name_pipeline.check_update_flow_name, log_prints=True)
def au_ato_abr_other_name_check_update() -> None:
    _other_name_pipeline.run_check_update()


au_ato_abr_other_name_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, OTHER_NAME_TABLE_ID
)
au_ato_abr_other_name_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_other_name_pipeline.extract_and_load_flow_name, log_prints=True)
def au_ato_abr_other_name_download(download_params: dict) -> None:
    _other_name_pipeline.run_extract_and_load(download_params)


au_ato_abr_other_name_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, OTHER_NAME_TABLE_ID
)
au_ato_abr_other_name_download.job_variables = _JOB_VARIABLES
_other_name_pipeline.extract_load_deployment = (
    au_ato_abr_other_name_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dgr
# check_update: au_ato_abr__dgr
# extract_and_load: au_ato_abr__dgr
# ──────────────────────────────────────────────────────────────────────────────

_dgr_pipeline = make_pipeline(DGR_TABLE_ID)


@flow(name=_dgr_pipeline.check_update_flow_name, log_prints=True)
def au_ato_abr_dgr_check_update() -> None:
    _dgr_pipeline.run_check_update()


au_ato_abr_dgr_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DGR_TABLE_ID
)
au_ato_abr_dgr_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_dgr_pipeline.extract_and_load_flow_name, log_prints=True)
def au_ato_abr_dgr_download(download_params: dict) -> None:
    _dgr_pipeline.run_extract_and_load(download_params)


au_ato_abr_dgr_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DGR_TABLE_ID
)
au_ato_abr_dgr_download.job_variables = _JOB_VARIABLES
_dgr_pipeline.extract_load_deployment = au_ato_abr_dgr_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: au_ato_abr__dicionario
# extract_and_load: au_ato_abr__dicionario
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline(DICIONARIO_TABLE_ID)


@flow(name=_dicionario_pipeline.check_update_flow_name, log_prints=True)
def au_ato_abr_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


au_ato_abr_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DICIONARIO_TABLE_ID
)
au_ato_abr_dicionario_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_dicionario_pipeline.extract_and_load_flow_name, log_prints=True)
def au_ato_abr_dicionario_download(download_params: dict) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


au_ato_abr_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DICIONARIO_TABLE_ID
)
au_ato_abr_dicionario_download.job_variables = _JOB_VARIABLES
_dicionario_pipeline.extract_load_deployment = (
    au_ato_abr_dicionario_download.fn.__name__
)
