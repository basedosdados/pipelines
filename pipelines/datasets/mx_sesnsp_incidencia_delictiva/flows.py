"""
Flows for mx_sesnsp_incidencia_delictiva — Prefect 3.

Mexican monthly crime counts from SESNSP. Only the four new-methodology tables
that gain a month on every release are refreshed here — the three
``*_2015_2025`` legacy tables are frozen and never touched by this pipeline.
Each SESNSP release ships the full current-year history, so every run is a
full replace (``dump_mode="overwrite"``), not an incremental append.

Migrated to the one-pipeline-per-table_id shape: 4 independent check_update /
extract_and_load pairs (municipio_delitos, estatal_delitos, estatal_victimas,
municipio_victimas), wired through ``CheckThenExtractLoadPipeline``/
``pipeline_factory`` (see ``tasks.py``). The originally-planned
"check_and_download" variant was cancelled — every table uses the standard
check_update -> extract_and_load shape, even though check_update must itself
scrape the rotating SharePoint tokens and download the table's zip (several
hundred MB) to read off the latest published month, and extract_and_load
downloads it again, independently (accepted duplicated cost, same as
``br_cgu_sancoes``/``au_ato_abr``/``au_geoscape_gnaf``).

No real cross-table dbt dependency exists between the four tables: every
model in ``models/mx_sesnsp_incidencia_delictiva/*.sql`` selects from its own
staging source with no ``ref()`` to a sibling table — the only ``ref()``s in
``schema.yml`` point at ``br_bd_diretorios_mx`` (an external directory, not a
sibling table in this dataset). So the repo-wide "1 flow per table" default
applies cleanly here, with no ``dispatch_build_and_promote``-bypassing group
flow needed.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers all 8 flows
below; the dev pool ignores the schedules, the prod pool activates them.
"""

from prefect.schedules import Cron

from pipelines.datasets.mx_sesnsp_incidencia_delictiva.constants import (
    ESTATAL_DELITOS_TABLE_ID,
    ESTATAL_VICTIMAS_TABLE_ID,
    MUNICIPIO_DELITOS_TABLE_ID,
    MUNICIPIO_VICTIMAS_TABLE_ID,
    constants,
)
from pipelines.datasets.mx_sesnsp_incidencia_delictiva.tasks import (
    make_pipeline,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value

# SESNSP releases monthly, ~20th of the month, Mexico City time. Poll across a
# few days; the source-poll guard no-ops until a new month actually appears.
# Same schedule for all 4 tables — one shared monthly cadence, same as the old
# monolithic flow.
_CHECK_UPDATE_SCHEDULE = [
    Cron("20 9 20,21,22,23,24 * *", timezone="America/Mexico_City")
]

# The municipal melt holds ~1.7M rows in pandas; give the worker headroom.
# Only the municipio_* tables run that melt (check_update and extract_and_load
# both clean the full CSV) — the estatal_* tables are far smaller and run with
# the default memory.
_MUNICIPAL_JOB_VARIABLES = {"memory": "8Gi"}

# ──────────────────────────────────────────────────────────────────────────────
# municipio_delitos
# check_update: mx_sesnsp_incidencia_delictiva__municipio_delitos
# extract_and_load: mx_sesnsp_incidencia_delictiva__municipio_delitos
# ──────────────────────────────────────────────────────────────────────────────

_municipio_delitos_pipeline = make_pipeline(MUNICIPIO_DELITOS_TABLE_ID)


@flow(name=_municipio_delitos_pipeline.check_update_flow_name, log_prints=True)
def mx_sesnsp_incidencia_delictiva_municipio_delitos_check_update() -> None:
    _municipio_delitos_pipeline.run_check_update()


mx_sesnsp_incidencia_delictiva_municipio_delitos_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, MUNICIPIO_DELITOS_TABLE_ID)
)
mx_sesnsp_incidencia_delictiva_municipio_delitos_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE
mx_sesnsp_incidencia_delictiva_municipio_delitos_check_update.job_variables = (
    _MUNICIPAL_JOB_VARIABLES
)


@flow(
    name=_municipio_delitos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def mx_sesnsp_incidencia_delictiva_municipio_delitos_download(
    download_params: dict,
) -> None:
    _municipio_delitos_pipeline.run_extract_and_load(download_params)


mx_sesnsp_incidencia_delictiva_municipio_delitos_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, MUNICIPIO_DELITOS_TABLE_ID)
)
mx_sesnsp_incidencia_delictiva_municipio_delitos_download.job_variables = (
    _MUNICIPAL_JOB_VARIABLES
)
_municipio_delitos_pipeline.extract_load_deployment = (
    mx_sesnsp_incidencia_delictiva_municipio_delitos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estatal_delitos
# check_update: mx_sesnsp_incidencia_delictiva__estatal_delitos
# extract_and_load: mx_sesnsp_incidencia_delictiva__estatal_delitos
# ──────────────────────────────────────────────────────────────────────────────

_estatal_delitos_pipeline = make_pipeline(ESTATAL_DELITOS_TABLE_ID)


@flow(name=_estatal_delitos_pipeline.check_update_flow_name, log_prints=True)
def mx_sesnsp_incidencia_delictiva_estatal_delitos_check_update() -> None:
    _estatal_delitos_pipeline.run_check_update()


mx_sesnsp_incidencia_delictiva_estatal_delitos_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, ESTATAL_DELITOS_TABLE_ID)
)
mx_sesnsp_incidencia_delictiva_estatal_delitos_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(
    name=_estatal_delitos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def mx_sesnsp_incidencia_delictiva_estatal_delitos_download(
    download_params: dict,
) -> None:
    _estatal_delitos_pipeline.run_extract_and_load(download_params)


mx_sesnsp_incidencia_delictiva_estatal_delitos_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, ESTATAL_DELITOS_TABLE_ID)
)
_estatal_delitos_pipeline.extract_load_deployment = (
    mx_sesnsp_incidencia_delictiva_estatal_delitos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estatal_victimas
# check_update: mx_sesnsp_incidencia_delictiva__estatal_victimas
# extract_and_load: mx_sesnsp_incidencia_delictiva__estatal_victimas
# ──────────────────────────────────────────────────────────────────────────────

_estatal_victimas_pipeline = make_pipeline(ESTATAL_VICTIMAS_TABLE_ID)


@flow(name=_estatal_victimas_pipeline.check_update_flow_name, log_prints=True)
def mx_sesnsp_incidencia_delictiva_estatal_victimas_check_update() -> None:
    _estatal_victimas_pipeline.run_check_update()


mx_sesnsp_incidencia_delictiva_estatal_victimas_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, ESTATAL_VICTIMAS_TABLE_ID)
)
mx_sesnsp_incidencia_delictiva_estatal_victimas_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(
    name=_estatal_victimas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def mx_sesnsp_incidencia_delictiva_estatal_victimas_download(
    download_params: dict,
) -> None:
    _estatal_victimas_pipeline.run_extract_and_load(download_params)


mx_sesnsp_incidencia_delictiva_estatal_victimas_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, ESTATAL_VICTIMAS_TABLE_ID)
)
_estatal_victimas_pipeline.extract_load_deployment = (
    mx_sesnsp_incidencia_delictiva_estatal_victimas_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# municipio_victimas
# check_update: mx_sesnsp_incidencia_delictiva__municipio_victimas
# extract_and_load: mx_sesnsp_incidencia_delictiva__municipio_victimas
# ──────────────────────────────────────────────────────────────────────────────

_municipio_victimas_pipeline = make_pipeline(MUNICIPIO_VICTIMAS_TABLE_ID)


@flow(
    name=_municipio_victimas_pipeline.check_update_flow_name, log_prints=True
)
def mx_sesnsp_incidencia_delictiva_municipio_victimas_check_update() -> None:
    _municipio_victimas_pipeline.run_check_update()


mx_sesnsp_incidencia_delictiva_municipio_victimas_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, MUNICIPIO_VICTIMAS_TABLE_ID)
)
mx_sesnsp_incidencia_delictiva_municipio_victimas_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE
mx_sesnsp_incidencia_delictiva_municipio_victimas_check_update.job_variables = _MUNICIPAL_JOB_VARIABLES


@flow(
    name=_municipio_victimas_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def mx_sesnsp_incidencia_delictiva_municipio_victimas_download(
    download_params: dict,
) -> None:
    _municipio_victimas_pipeline.run_extract_and_load(download_params)


mx_sesnsp_incidencia_delictiva_municipio_victimas_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, MUNICIPIO_VICTIMAS_TABLE_ID
    )
)
mx_sesnsp_incidencia_delictiva_municipio_victimas_download.job_variables = (
    _MUNICIPAL_JOB_VARIABLES
)
_municipio_victimas_pipeline.extract_load_deployment = (
    mx_sesnsp_incidencia_delictiva_municipio_victimas_download.fn.__name__
)
