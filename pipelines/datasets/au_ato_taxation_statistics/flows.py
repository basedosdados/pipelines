"""Flows for au_ato_taxation_statistics — Prefect 3.

ATO Taxation Statistics (data.gov.au). The ATO reissues the whole collection
once a year and revises earlier years in place, so each run refetches every
in-scope release and does a **full replace** (``dump_mode="overwrite"``)
rather than appending the newest year.

Migrated to the one-pipeline-per-table_id shape: 6 independent check_update /
extract_and_load pairs (one per table — individuals_income_state,
individuals_industry, individuals_postcode, company_industry, gst_industry,
dicionario), wired through ``CheckThenExtractLoadPipeline``/
``pipeline_factory`` (see ``tasks.py``). Each table's extract_and_load
redownloads the curated workbooks and recleans them from scratch, keeping
only its own table — redundant (a single pass already builds all 6 together,
see ``utils.clean_all``), but uniform 1-flow-per-table is the rule now (same
trade-off ``au_ato_abr`` took for a similar shared-download, multi-table
source).

check_update downloads the whole collection just to learn the newest
release's year (data.gov.au has no cheaper signal) and only dispatches its
own extract_and_load — which redownloads and recleans the same payload —
when the source has actually published a year this table hasn't ingested, so
a scheduled run is a cheap-ish no-op between annual releases.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers all 12
flows below; the dev pool ignores the check_update schedules, the prod pool
activates them (deployed paused).
"""

from prefect.schedules import Cron

from pipelines.datasets.au_ato_taxation_statistics.tasks import (
    DATASET_ID,
    DICIONARIO_TABLE_ID,
    GST_TABLE_ID,
    make_pipeline,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# The ATO publishes Taxation Statistics once a year, historically around
# April-June. Poll monthly at 16:45 BRT on the 20th; the source-poll guard
# no-ops (no extract_and_load dispatch) until a new financial year actually
# appears. Same schedule for all 6 tables — one shared annual collection,
# same as before the per-table split.
_CHECK_UPDATE_SCHEDULE = [Cron("45 16 20 * *", timezone="America/Sao_Paulo")]

# The clean step holds ~4.5M rows in pandas before writing partitions; only
# extract_and_load runs it (check_update stops at the release year).
_JOB_VARIABLES = {"memory": "8Gi"}

INDIVIDUALS_INCOME_STATE_TABLE_ID = "individuals_income_state"
INDIVIDUALS_INDUSTRY_TABLE_ID = "individuals_industry"
INDIVIDUALS_POSTCODE_TABLE_ID = "individuals_postcode"
COMPANY_INDUSTRY_TABLE_ID = "company_industry"

# ──────────────────────────────────────────────────────────────────────────────
# individuals_income_state
# check_update: au_ato_taxation_statistics__individuals_income_state
# extract_and_load: au_ato_taxation_statistics__individuals_income_state
# ──────────────────────────────────────────────────────────────────────────────

_individuals_income_state_pipeline = make_pipeline(
    INDIVIDUALS_INCOME_STATE_TABLE_ID
)


@flow(
    name=_individuals_income_state_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_individuals_income_state_check_update() -> None:
    _individuals_income_state_pipeline.run_check_update()


au_ato_taxation_statistics_individuals_income_state_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, INDIVIDUALS_INCOME_STATE_TABLE_ID
)
au_ato_taxation_statistics_individuals_income_state_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(
    name=_individuals_income_state_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_individuals_income_state_download(
    download_params: dict,
) -> None:
    _individuals_income_state_pipeline.run_extract_and_load(download_params)


au_ato_taxation_statistics_individuals_income_state_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, INDIVIDUALS_INCOME_STATE_TABLE_ID
    )
)
au_ato_taxation_statistics_individuals_income_state_download.job_variables = (
    _JOB_VARIABLES
)
_individuals_income_state_pipeline.extract_load_deployment = (
    au_ato_taxation_statistics_individuals_income_state_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# individuals_industry
# check_update: au_ato_taxation_statistics__individuals_industry
# extract_and_load: au_ato_taxation_statistics__individuals_industry
# ──────────────────────────────────────────────────────────────────────────────

_individuals_industry_pipeline = make_pipeline(INDIVIDUALS_INDUSTRY_TABLE_ID)


@flow(
    name=_individuals_industry_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_individuals_industry_check_update() -> None:
    _individuals_industry_pipeline.run_check_update()


au_ato_taxation_statistics_individuals_industry_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, INDIVIDUALS_INDUSTRY_TABLE_ID)
)
au_ato_taxation_statistics_individuals_industry_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(
    name=_individuals_industry_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_individuals_industry_download(
    download_params: dict,
) -> None:
    _individuals_industry_pipeline.run_extract_and_load(download_params)


au_ato_taxation_statistics_individuals_industry_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, INDIVIDUALS_INDUSTRY_TABLE_ID
    )
)
au_ato_taxation_statistics_individuals_industry_download.job_variables = (
    _JOB_VARIABLES
)
_individuals_industry_pipeline.extract_load_deployment = (
    au_ato_taxation_statistics_individuals_industry_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# individuals_postcode
# check_update: au_ato_taxation_statistics__individuals_postcode
# extract_and_load: au_ato_taxation_statistics__individuals_postcode
# ──────────────────────────────────────────────────────────────────────────────

_individuals_postcode_pipeline = make_pipeline(INDIVIDUALS_POSTCODE_TABLE_ID)


@flow(
    name=_individuals_postcode_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_individuals_postcode_check_update() -> None:
    _individuals_postcode_pipeline.run_check_update()


au_ato_taxation_statistics_individuals_postcode_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, INDIVIDUALS_POSTCODE_TABLE_ID)
)
au_ato_taxation_statistics_individuals_postcode_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(
    name=_individuals_postcode_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_individuals_postcode_download(
    download_params: dict,
) -> None:
    _individuals_postcode_pipeline.run_extract_and_load(download_params)


au_ato_taxation_statistics_individuals_postcode_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, INDIVIDUALS_POSTCODE_TABLE_ID
    )
)
au_ato_taxation_statistics_individuals_postcode_download.job_variables = (
    _JOB_VARIABLES
)
_individuals_postcode_pipeline.extract_load_deployment = (
    au_ato_taxation_statistics_individuals_postcode_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# company_industry
# check_update: au_ato_taxation_statistics__company_industry
# extract_and_load: au_ato_taxation_statistics__company_industry
# ──────────────────────────────────────────────────────────────────────────────

_company_industry_pipeline = make_pipeline(COMPANY_INDUSTRY_TABLE_ID)


@flow(
    name=_company_industry_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_company_industry_check_update() -> None:
    _company_industry_pipeline.run_check_update()


au_ato_taxation_statistics_company_industry_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, COMPANY_INDUSTRY_TABLE_ID)
)
au_ato_taxation_statistics_company_industry_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_company_industry_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_company_industry_download(
    download_params: dict,
) -> None:
    _company_industry_pipeline.run_extract_and_load(download_params)


au_ato_taxation_statistics_company_industry_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, COMPANY_INDUSTRY_TABLE_ID
)
au_ato_taxation_statistics_company_industry_download.job_variables = (
    _JOB_VARIABLES
)
_company_industry_pipeline.extract_load_deployment = (
    au_ato_taxation_statistics_company_industry_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# gst_industry
# check_update: au_ato_taxation_statistics__gst_industry
# extract_and_load: au_ato_taxation_statistics__gst_industry
# ──────────────────────────────────────────────────────────────────────────────

_gst_industry_pipeline = make_pipeline(GST_TABLE_ID)


@flow(
    name=_gst_industry_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_gst_industry_check_update() -> None:
    _gst_industry_pipeline.run_check_update()


au_ato_taxation_statistics_gst_industry_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, GST_TABLE_ID
)
au_ato_taxation_statistics_gst_industry_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_gst_industry_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_gst_industry_download(
    download_params: dict,
) -> None:
    _gst_industry_pipeline.run_extract_and_load(download_params)


au_ato_taxation_statistics_gst_industry_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, GST_TABLE_ID
)
au_ato_taxation_statistics_gst_industry_download.job_variables = _JOB_VARIABLES
_gst_industry_pipeline.extract_load_deployment = (
    au_ato_taxation_statistics_gst_industry_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: au_ato_taxation_statistics__dicionario
# extract_and_load: au_ato_taxation_statistics__dicionario
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline(DICIONARIO_TABLE_ID)


@flow(
    name=_dicionario_pipeline.check_update_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


au_ato_taxation_statistics_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DICIONARIO_TABLE_ID
)
au_ato_taxation_statistics_dicionario_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(
    name=_dicionario_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def au_ato_taxation_statistics_dicionario_download(
    download_params: dict,
) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


au_ato_taxation_statistics_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DICIONARIO_TABLE_ID
)
au_ato_taxation_statistics_dicionario_download.job_variables = _JOB_VARIABLES
_dicionario_pipeline.extract_load_deployment = (
    au_ato_taxation_statistics_dicionario_download.fn.__name__
)
