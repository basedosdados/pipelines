"""Flows for us_bea — Prefect 3.

US Bureau of Economic Analysis (BEA) economic accounts, pulled from the BEA REST
API. BEA benchmark revisions rewrite historical values, so each table's
extract_and_load re-fetches its full history and overwrites staging
(``dump_mode="overwrite"``), rather than appending.

Migrated to the staged pipeline: check_update -> extract_and_load ->
build_and_promote, one pair of flows per table (``CheckThenExtractLoadPipeline``
via ``pipeline_factory`` — the repo-wide default shape, same layout as
``br_ibge_ipca``/``au_geoscape_gnaf``). The original one-shot
``check_and_download`` variant considered for this dataset was cancelled before
being built; every dataset in this repo uses check_update -> extract_and_load,
even when (as here, for nipa/gdp_by_industry/regional_state) the check itself
has to download the series to find a reference date — ``extract_load_data``
downloads it again, independently, moments later if new data is found. The
duplicated download cost is accepted in exchange for a single pipeline shape
across the whole repo.

Each BEA "dataset" family (NIPA / GDPbyIndustry / Regional) already has its own
independent GetData calls — there is no single shared download being split
across tables here (contrast au_geoscape_gnaf, where one release zip builds 4
tables at once). ``models/us_bea/schema.yml`` also has no ``ref()`` between the
six us_bea models, so there is no dbt test-ordering reason for a batched
extract_and_load either (contrast br_bcb_ifdata/au_rba_statistical_tables).

Coverage tier: all six tables are ``AllFree``. ``nipa`` is a mixed-frequency
table (annual/quarterly rows have a NULL ``month``), so a monthly Row Access
Policy would paywall those rows forever (``DATE(year, month, 1)`` is NULL) — it
is therefore NOT paywalled. ``nipa``'s ``(year, month)`` coverage still drives
its own monthly source poll. ``dicionario`` has no date column, so it takes
``NonHistorical`` coverage and polls via ``compare_against="table_update"``,
reusing nipa's monthly reference as a freshness proxy (see the banner in
tasks.py). If a rolling BD Pro paywall is wanted later, add an end-of-period
``date`` column to ``nipa`` and key the policy on it.

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers all 12
flows below; the dev pool ignores the check_update schedules, the prod pool
activates them (deployed paused).
"""

from prefect.schedules import Cron, Schedule

from pipelines.datasets.us_bea.constants import constants
from pipelines.datasets.us_bea.tasks import (
    make_extract_load_data,
    make_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags, pipeline_factory

DATASET_ID = constants.DATASET_ID.value

make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)

# BEA refreshes NIPA monthly (personal income near month-end; benchmark and GDP
# revisions land irregularly), and the other tables tend to update around the
# same late-month window. Poll across the same few late-month days as the old
# monolithic schedule (``40 16 25,26,27,28 * *``) at 16:00 BRT, staggered a few
# minutes apart per table so their extract_and_loads, if all triggered by the
# same release window, don't all claim a worker at once. ``nipa`` keeps the
# exact original minute.
_POLL_DAYS = "25,26,27,28"

# regional_county streams through pandas/arrow in 500k-row flushes (~50M rows,
# same as the old monolithic flow) — give both its check_update (which fetches
# the same full family to find the latest year) and its extract_and_load the
# same memory headroom the single flow used to get.
_COUNTY_JOB_VARIABLES = {"memory": "8Gi"}


def _cron(hour: int, minute: int) -> list[Schedule]:
    return [
        Cron(f"{minute} {hour} {_POLL_DAYS} * *", timezone="America/Sao_Paulo")
    ]


# ──────────────────────────────────────────────────────────────────────────────
# nipa
# check_update: check_update: us_bea__nipa
# extract_and_load: extract_and_load: us_bea__nipa
# ──────────────────────────────────────────────────────────────────────────────

_nipa_pipeline = make_pipeline("nipa", date_format="%Y-%m")


@flow(name=_nipa_pipeline.check_update_flow_name, log_prints=True)
def us_bea_nipa_check_update() -> None:
    _nipa_pipeline.run_check_update()


us_bea_nipa_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "nipa"
)
us_bea_nipa_check_update.deploy_schedules = _cron(16, 40)


@flow(name=_nipa_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bea_nipa_download(download_params: dict) -> None:
    _nipa_pipeline.run_extract_and_load(download_params)


us_bea_nipa_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "nipa"
)
_nipa_pipeline.extract_load_deployment = us_bea_nipa_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# gdp_by_industry
# check_update: check_update: us_bea__gdp_by_industry
# extract_and_load: extract_and_load: us_bea__gdp_by_industry
# ──────────────────────────────────────────────────────────────────────────────

_gdp_by_industry_pipeline = make_pipeline(
    "gdp_by_industry", date_format="%Y-%m"
)


@flow(name=_gdp_by_industry_pipeline.check_update_flow_name, log_prints=True)
def us_bea_gdp_by_industry_check_update() -> None:
    _gdp_by_industry_pipeline.run_check_update()


us_bea_gdp_by_industry_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "gdp_by_industry"
)
us_bea_gdp_by_industry_check_update.deploy_schedules = _cron(16, 45)


@flow(
    name=_gdp_by_industry_pipeline.extract_and_load_flow_name, log_prints=True
)
def us_bea_gdp_by_industry_download(download_params: dict) -> None:
    _gdp_by_industry_pipeline.run_extract_and_load(download_params)


us_bea_gdp_by_industry_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "gdp_by_industry"
)
_gdp_by_industry_pipeline.extract_load_deployment = (
    us_bea_gdp_by_industry_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# regional_state
# check_update: check_update: us_bea__regional_state
# extract_and_load: extract_and_load: us_bea__regional_state
# ──────────────────────────────────────────────────────────────────────────────

_regional_state_pipeline = make_pipeline("regional_state", date_format="%Y-%m")


@flow(name=_regional_state_pipeline.check_update_flow_name, log_prints=True)
def us_bea_regional_state_check_update() -> None:
    _regional_state_pipeline.run_check_update()


us_bea_regional_state_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "regional_state"
)
us_bea_regional_state_check_update.deploy_schedules = _cron(16, 50)


@flow(
    name=_regional_state_pipeline.extract_and_load_flow_name, log_prints=True
)
def us_bea_regional_state_download(download_params: dict) -> None:
    _regional_state_pipeline.run_extract_and_load(download_params)


us_bea_regional_state_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "regional_state"
)
_regional_state_pipeline.extract_load_deployment = (
    us_bea_regional_state_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# regional_metro
# check_update: check_update: us_bea__regional_metro
# extract_and_load: extract_and_load: us_bea__regional_metro
# ──────────────────────────────────────────────────────────────────────────────

_regional_metro_pipeline = make_pipeline("regional_metro", date_format="%Y")


@flow(name=_regional_metro_pipeline.check_update_flow_name, log_prints=True)
def us_bea_regional_metro_check_update() -> None:
    _regional_metro_pipeline.run_check_update()


us_bea_regional_metro_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "regional_metro"
)
us_bea_regional_metro_check_update.deploy_schedules = _cron(16, 55)


@flow(
    name=_regional_metro_pipeline.extract_and_load_flow_name, log_prints=True
)
def us_bea_regional_metro_download(download_params: dict) -> None:
    _regional_metro_pipeline.run_extract_and_load(download_params)


us_bea_regional_metro_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "regional_metro"
)
_regional_metro_pipeline.extract_load_deployment = (
    us_bea_regional_metro_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# regional_county
# check_update: check_update: us_bea__regional_county
# extract_and_load: extract_and_load: us_bea__regional_county
#
# ~50M rows — see `_COUNTY_JOB_VARIABLES` above.
# ──────────────────────────────────────────────────────────────────────────────

_regional_county_pipeline = make_pipeline("regional_county", date_format="%Y")


@flow(name=_regional_county_pipeline.check_update_flow_name, log_prints=True)
def us_bea_regional_county_check_update() -> None:
    _regional_county_pipeline.run_check_update()


us_bea_regional_county_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "regional_county"
)
us_bea_regional_county_check_update.deploy_schedules = _cron(17, 0)
us_bea_regional_county_check_update.job_variables = _COUNTY_JOB_VARIABLES


@flow(
    name=_regional_county_pipeline.extract_and_load_flow_name, log_prints=True
)
def us_bea_regional_county_download(download_params: dict) -> None:
    _regional_county_pipeline.run_extract_and_load(download_params)


us_bea_regional_county_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "regional_county"
)
us_bea_regional_county_download.job_variables = _COUNTY_JOB_VARIABLES
_regional_county_pipeline.extract_load_deployment = (
    us_bea_regional_county_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: check_update: us_bea__dicionario
# extract_and_load: extract_and_load: us_bea__dicionario
#
# NonHistorical (no date column) — `make_get_latest_update` reuses nipa's
# monthly reference and compares against `Table.Update.latest` instead of
# `Coverage` for this table_id, see the banner in tasks.py.
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline("dicionario", date_format="%Y-%m")


@flow(name=_dicionario_pipeline.check_update_flow_name, log_prints=True)
def us_bea_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


us_bea_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "dicionario"
)
us_bea_dicionario_check_update.deploy_schedules = _cron(17, 5)


@flow(name=_dicionario_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bea_dicionario_download(download_params: dict) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


us_bea_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "dicionario"
)
_dicionario_pipeline.extract_load_deployment = (
    us_bea_dicionario_download.fn.__name__
)
