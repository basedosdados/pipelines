"""
Flows for br_cgu_sancoes — Prefect 3.

CGU sanctions registries (CEIS, CNEP, CEPIM, Acordos de Leniência) from the
Portal da Transparência. Each registry is an on-demand live snapshot with no
historical archive, so every run fetches the latest available snapshot and
does a full replace (``dump_mode="overwrite"``), stamping ``data_extracao``.

Migrated to the one-pipeline-per-table_id shape: 6 independent check_update /
extract_and_load pairs (ceis, cnep, cepim, acordos_leniencia,
acordos_leniencia_efeitos, dicionario), wired through
``CheckThenExtractLoadPipeline``/``pipeline_factory`` (see ``tasks.py``). The
originally-planned "check_and_download" variant was cancelled — every table
uses the standard check_update -> extract_and_load shape, even though
check_update must itself download a registry zip to read off a snapshot date
(``get_latest_update``) and extract_and_load downloads it again, independently
(accepted duplicated cost, same as ``au_ato_abr``/``au_geoscape_gnaf``).

ceis/cnep/cepim each own an independent portal registry; acordos_leniencia/
acordos_leniencia_efeitos share the single "acordos-leniencia" zip but still
get fully independent flows (no shared download step) — see the module
banner in ``constants.py`` for why 1-flow-per-table applies cleanly here (no
real cross-table dbt dependency). dicionario needs no download to build (it's
authored directly in Python), but its check_update borrows ceis's registry as
a real freshness signal.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers all 12
flows below; the dev pool ignores the schedules, the prod pool activates them.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_sancoes.constants import (
    ACORDOS_LENIENCIA_EFEITOS_TABLE_ID,
    ACORDOS_LENIENCIA_TABLE_ID,
    CEIS_TABLE_ID,
    CEPIM_TABLE_ID,
    CNEP_TABLE_ID,
    DICIONARIO_TABLE_ID,
    constants,
)
from pipelines.datasets.br_cgu_sancoes.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value

# The registries refresh irregularly (a live on-demand snapshot). Poll weekly
# across a couple of days (Mon + Tue) at 08:00 BRT so a day where the
# on-demand generation fails still gets a retry; overwrite makes a second run
# idempotent. Same schedule for all 6 tables — one shared weekly cadence, same
# as the old monolithic flow.
_CHECK_UPDATE_SCHEDULE = [Cron("30 8 * * 1,2", timezone="America/Sao_Paulo")]

# ──────────────────────────────────────────────────────────────────────────────
# ceis
# check_update: br_cgu_sancoes__ceis
# extract_and_load: br_cgu_sancoes__ceis
# ──────────────────────────────────────────────────────────────────────────────

_ceis_pipeline = make_pipeline(CEIS_TABLE_ID)


@flow(name=_ceis_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_sancoes_ceis_check_update() -> None:
    _ceis_pipeline.run_check_update()


br_cgu_sancoes_ceis_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CEIS_TABLE_ID
)
br_cgu_sancoes_ceis_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_ceis_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_sancoes_ceis_download(download_params: dict) -> None:
    _ceis_pipeline.run_extract_and_load(download_params)


br_cgu_sancoes_ceis_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CEIS_TABLE_ID
)
_ceis_pipeline.extract_load_deployment = (
    br_cgu_sancoes_ceis_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# cnep
# check_update: br_cgu_sancoes__cnep
# extract_and_load: br_cgu_sancoes__cnep
# ──────────────────────────────────────────────────────────────────────────────

_cnep_pipeline = make_pipeline(CNEP_TABLE_ID)


@flow(name=_cnep_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_sancoes_cnep_check_update() -> None:
    _cnep_pipeline.run_check_update()


br_cgu_sancoes_cnep_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CNEP_TABLE_ID
)
br_cgu_sancoes_cnep_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_cnep_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_sancoes_cnep_download(download_params: dict) -> None:
    _cnep_pipeline.run_extract_and_load(download_params)


br_cgu_sancoes_cnep_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CNEP_TABLE_ID
)
_cnep_pipeline.extract_load_deployment = (
    br_cgu_sancoes_cnep_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# cepim
# check_update: br_cgu_sancoes__cepim
# extract_and_load: br_cgu_sancoes__cepim
# ──────────────────────────────────────────────────────────────────────────────

_cepim_pipeline = make_pipeline(CEPIM_TABLE_ID)


@flow(name=_cepim_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_sancoes_cepim_check_update() -> None:
    _cepim_pipeline.run_check_update()


br_cgu_sancoes_cepim_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CEPIM_TABLE_ID
)
br_cgu_sancoes_cepim_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_cepim_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_sancoes_cepim_download(download_params: dict) -> None:
    _cepim_pipeline.run_extract_and_load(download_params)


br_cgu_sancoes_cepim_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CEPIM_TABLE_ID
)
_cepim_pipeline.extract_load_deployment = (
    br_cgu_sancoes_cepim_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# acordos_leniencia
# check_update: br_cgu_sancoes__acordos_leniencia
# extract_and_load: br_cgu_sancoes__acordos_leniencia
# ──────────────────────────────────────────────────────────────────────────────

_acordos_leniencia_pipeline = make_pipeline(ACORDOS_LENIENCIA_TABLE_ID)


@flow(name=_acordos_leniencia_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_sancoes_acordos_leniencia_check_update() -> None:
    _acordos_leniencia_pipeline.run_check_update()


br_cgu_sancoes_acordos_leniencia_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, ACORDOS_LENIENCIA_TABLE_ID
)
br_cgu_sancoes_acordos_leniencia_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_acordos_leniencia_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_sancoes_acordos_leniencia_download(download_params: dict) -> None:
    _acordos_leniencia_pipeline.run_extract_and_load(download_params)


br_cgu_sancoes_acordos_leniencia_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, ACORDOS_LENIENCIA_TABLE_ID
)
_acordos_leniencia_pipeline.extract_load_deployment = (
    br_cgu_sancoes_acordos_leniencia_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# acordos_leniencia_efeitos
# check_update: br_cgu_sancoes__acordos_leniencia_efeitos
# extract_and_load: br_cgu_sancoes__acordos_leniencia_efeitos
# ──────────────────────────────────────────────────────────────────────────────

_acordos_leniencia_efeitos_pipeline = make_pipeline(
    ACORDOS_LENIENCIA_EFEITOS_TABLE_ID
)


@flow(
    name=_acordos_leniencia_efeitos_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_cgu_sancoes_acordos_leniencia_efeitos_check_update() -> None:
    _acordos_leniencia_efeitos_pipeline.run_check_update()


br_cgu_sancoes_acordos_leniencia_efeitos_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, ACORDOS_LENIENCIA_EFEITOS_TABLE_ID
    )
)
br_cgu_sancoes_acordos_leniencia_efeitos_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_acordos_leniencia_efeitos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_sancoes_acordos_leniencia_efeitos_download(
    download_params: dict,
) -> None:
    _acordos_leniencia_efeitos_pipeline.run_extract_and_load(download_params)


br_cgu_sancoes_acordos_leniencia_efeitos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, ACORDOS_LENIENCIA_EFEITOS_TABLE_ID
)
_acordos_leniencia_efeitos_pipeline.extract_load_deployment = (
    br_cgu_sancoes_acordos_leniencia_efeitos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: br_cgu_sancoes__dicionario
# extract_and_load: br_cgu_sancoes__dicionario
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline(DICIONARIO_TABLE_ID)


@flow(name=_dicionario_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_sancoes_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


br_cgu_sancoes_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DICIONARIO_TABLE_ID
)
br_cgu_sancoes_dicionario_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(name=_dicionario_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_sancoes_dicionario_download(download_params: dict) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


br_cgu_sancoes_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DICIONARIO_TABLE_ID
)
_dicionario_pipeline.extract_load_deployment = (
    br_cgu_sancoes_dicionario_download.fn.__name__
)
