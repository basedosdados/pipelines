"""Flows for au_abs_labour_force — Prefect 3.

Labour Force, Australia (ABS cat. 6202.0), monthly. Both sources ship the full
history every release — the SDMX ``all`` query returns every period, and each ABS
Excel spreadsheet carries the whole series — so every run is a **full replace**
(``dump_mode="overwrite"``), not an incremental append.

Migrated to the one-pipeline-per-table_id shape: 4 independent check_update /
extract_and_load pairs (one per table — labour_force_status, hours_worked,
status_in_employment, underutilisation), wired through
``CheckThenExtractLoadPipeline``/``pipeline_factory`` (see ``tasks.py``). The
4 dbt models have no cross-table test (each only has ``relationships`` tests
against the shared ``br_bd_diretorios_data_tempo`` directory tables, never
``ref()`` on one another), so there is no "build all before testing any"
dependency forcing a shared ``build_and_promote`` — each table promotes
independently through the generic ``dispatch_build_and_promote``.

Unlike au_ato_abr's single shared ~1 GB download (every table's
extract_and_load must redownload the whole ZIP pair to get at its own rows),
this dataset's two source mechanisms already split cleanly by table:
labour_force_status/underutilisation read only their own SDMX dataflows,
hours_worked/status_in_employment read only their own Excel spreadsheets —
see the module banner in ``tasks.py``. So each table's extract_and_load
downloads exactly what it needs, with no shared-download redundancy to trade
off.

Every table refreshes monthly, so every table carries the BD Pro rolling window
(``PartBdpro``): the most recent six months are pro-only, older data is free, and
the window rolls forward on its own each run (see ``COVERAGE`` in
``constants.py``).

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers all 8 flows
below; the dev pool ignores the check_update schedules, the prod pool activates
them (deployed paused until armed).
"""

from prefect.schedules import Cron

from pipelines.datasets.au_abs_labour_force.constants import (
    HOURS_WORKED_TABLE_ID,
    LABOUR_FORCE_STATUS_TABLE_ID,
    STATUS_IN_EMPLOYMENT_TABLE_ID,
    UNDERUTILISATION_TABLE_ID,
    constants,
)
from pipelines.datasets.au_abs_labour_force.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value

# ABS releases Labour Force monthly, on a Thursday roughly the 3rd-4th week, at
# 11:30 Canberra time. Poll daily across that window at 06:00 BRT (= evening AEST,
# after the morning release); the source-poll guard no-ops until a new month
# lands. Same schedule for all 4 tables — one shared monthly release, same as
# before the per-table split.
_CHECK_UPDATE_SCHEDULE = [Cron("30 6 14-27 * *", timezone="America/Sao_Paulo")]

# ──────────────────────────────────────────────────────────────────────────────
# labour_force_status
# check_update: au_abs_labour_force__labour_force_status
# extract_and_load: au_abs_labour_force__labour_force_status
# ──────────────────────────────────────────────────────────────────────────────

_labour_force_status_pipeline = make_pipeline(LABOUR_FORCE_STATUS_TABLE_ID)


@flow(
    name=_labour_force_status_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_abs_labour_force_labour_force_status_check_update() -> None:
    _labour_force_status_pipeline.run_check_update()


au_abs_labour_force_labour_force_status_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, LABOUR_FORCE_STATUS_TABLE_ID
)
au_abs_labour_force_labour_force_status_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_labour_force_status_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_abs_labour_force_labour_force_status_download(
    download_params: dict,
) -> None:
    _labour_force_status_pipeline.run_extract_and_load(download_params)


au_abs_labour_force_labour_force_status_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, LABOUR_FORCE_STATUS_TABLE_ID
)
_labour_force_status_pipeline.extract_load_deployment = (
    au_abs_labour_force_labour_force_status_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# underutilisation
# check_update: au_abs_labour_force__underutilisation
# extract_and_load: au_abs_labour_force__underutilisation
# ──────────────────────────────────────────────────────────────────────────────

_underutilisation_pipeline = make_pipeline(UNDERUTILISATION_TABLE_ID)


@flow(
    name=_underutilisation_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_abs_labour_force_underutilisation_check_update() -> None:
    _underutilisation_pipeline.run_check_update()


au_abs_labour_force_underutilisation_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, UNDERUTILISATION_TABLE_ID
)
au_abs_labour_force_underutilisation_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_underutilisation_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_abs_labour_force_underutilisation_download(
    download_params: dict,
) -> None:
    _underutilisation_pipeline.run_extract_and_load(download_params)


au_abs_labour_force_underutilisation_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, UNDERUTILISATION_TABLE_ID
)
_underutilisation_pipeline.extract_load_deployment = (
    au_abs_labour_force_underutilisation_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# hours_worked
# check_update: au_abs_labour_force__hours_worked
# extract_and_load: au_abs_labour_force__hours_worked
# ──────────────────────────────────────────────────────────────────────────────

_hours_worked_pipeline = make_pipeline(HOURS_WORKED_TABLE_ID)


@flow(
    name=_hours_worked_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_abs_labour_force_hours_worked_check_update() -> None:
    _hours_worked_pipeline.run_check_update()


au_abs_labour_force_hours_worked_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, HOURS_WORKED_TABLE_ID
)
au_abs_labour_force_hours_worked_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_hours_worked_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_abs_labour_force_hours_worked_download(download_params: dict) -> None:
    _hours_worked_pipeline.run_extract_and_load(download_params)


au_abs_labour_force_hours_worked_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, HOURS_WORKED_TABLE_ID
)
_hours_worked_pipeline.extract_load_deployment = (
    au_abs_labour_force_hours_worked_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# status_in_employment
# check_update: au_abs_labour_force__status_in_employment
# extract_and_load: au_abs_labour_force__status_in_employment
# ──────────────────────────────────────────────────────────────────────────────

_status_in_employment_pipeline = make_pipeline(STATUS_IN_EMPLOYMENT_TABLE_ID)


@flow(
    name=_status_in_employment_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_abs_labour_force_status_in_employment_check_update() -> None:
    _status_in_employment_pipeline.run_check_update()


au_abs_labour_force_status_in_employment_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, STATUS_IN_EMPLOYMENT_TABLE_ID)
)
au_abs_labour_force_status_in_employment_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_status_in_employment_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_abs_labour_force_status_in_employment_download(
    download_params: dict,
) -> None:
    _status_in_employment_pipeline.run_extract_and_load(download_params)


au_abs_labour_force_status_in_employment_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, STATUS_IN_EMPLOYMENT_TABLE_ID
)
# openpyxl reads the ~38 MB SEM1 pivot — the only table that touches it; give
# the worker headroom.
au_abs_labour_force_status_in_employment_download.job_variables = {
    "memory": "6Gi"
}
_status_in_employment_pipeline.extract_load_deployment = (
    au_abs_labour_force_status_in_employment_download.fn.__name__
)
