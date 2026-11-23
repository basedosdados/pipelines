"""Flows for us_bls_cpi — Prefect 3.

US Consumer Price Index (BLS), CPI-U + CPI-W. The BLS flat files carry the full
history on every release, so each table's extract_and_load rebuilds and
overwrites it in full (``dump_mode="overwrite"``), rather than appending.

Migrated to the staged pipeline: check_update -> extract_and_load ->
build_and_promote, one pair of flows per table (``CheckThenExtractLoadPipeline``
via ``pipeline_factory`` — the repo-wide default shape, same layout as
``br_ibge_ipca``/``us_bea``/``au_geoscape_gnaf``). The ``check_and_download``
one-shot variant once considered for this dataset was cancelled before being
built; every dataset in this repo uses check_update -> extract_and_load, even
when (as here) the check itself has to download the flat files to find a
reference date — ``extract_load_data`` downloads them again, independently,
moments later if new data is found. The duplicated download cost (several
hundred MB) is accepted in exchange for a single pipeline shape across the
whole repo.

A single download builds all 4 tables (``monthly``, ``annual``, ``semiannual``,
``dicionario`` — ``clean_all`` in utils.py), but each still gets its own
check_update/extract_and_load pair: ``models/us_bls_cpi/schema.yml`` has no
``ref()`` between the dataset's own models (only to the external
``br_bd_diretorios_data_tempo`` directory tables), so there is no cross-table
dbt test-ordering reason to batch them (contrast ``br_bcb_ifdata``/
``au_rba_statistical_tables`, which keep a single pair for exactly that
reason). See the banner in tasks.py for the redundant-re-download rationale.

Coverage tier: ``monthly`` is ``PartBdpro`` (BD Pro rolling window, most recent
6 months pro-only); ``annual``/``semiannual`` are ``AllFree``; ``dicionario``
has no date column, so it takes ``NonHistorical`` and polls via
``compare_against="table_update"``, reusing the monthly table's reference as a
freshness proxy (see constants.py / the banner in tasks.py).

Memory: ``clean_cpi`` always rebuilds all 4 tables regardless of which
table_id asked (``clean_all`` holds ~4M rows in pandas), so every check_update
and extract_and_load flow below needs the same headroom as the old monolithic
flow.

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers all 8
flows below; the dev pool ignores the check_update schedules, the prod pool
activates them (deployed paused).
"""

from prefect.schedules import Cron, Schedule

from pipelines.datasets.us_bls_cpi.constants import constants
from pipelines.datasets.us_bls_cpi.tasks import (
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

# BLS releases CPI monthly, ~2nd week, on a US business day. Poll across the
# same mid-month days as the old monolithic schedule
# (``"5 16 10,11,12,13,14,15 * *"``) at 16:00 BRT, staggered a few minutes
# apart per table so their extract_and_loads, if all triggered by the same
# release window, don't all claim a worker at once. ``monthly`` keeps the
# exact original minute.
_POLL_DAYS = "10,11,12,13,14,15"


def _cron(hour: int, minute: int) -> list[Schedule]:
    return [
        Cron(f"{minute} {hour} {_POLL_DAYS} * *", timezone="America/Sao_Paulo")
    ]


# `clean_cpi` holds ~4M rows in pandas for every call, regardless of table_id
# — same headroom the old single monolithic flow used.
_JOB_VARIABLES = {"memory": "8Gi"}


# ──────────────────────────────────────────────────────────────────────────────
# monthly
# check_update: check_update: us_bls_cpi__monthly
# extract_and_load: extract_and_load: us_bls_cpi__monthly
# ──────────────────────────────────────────────────────────────────────────────

_monthly_pipeline = make_pipeline("monthly", date_format="%Y-%m")


@flow(name=_monthly_pipeline.check_update_flow_name, log_prints=True)
def us_bls_cpi_monthly_check_update() -> None:
    _monthly_pipeline.run_check_update()


us_bls_cpi_monthly_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "monthly"
)
us_bls_cpi_monthly_check_update.deploy_schedules = _cron(16, 5)
us_bls_cpi_monthly_check_update.job_variables = _JOB_VARIABLES


@flow(name=_monthly_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bls_cpi_monthly_download(download_params: dict) -> None:
    _monthly_pipeline.run_extract_and_load(download_params)


us_bls_cpi_monthly_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "monthly"
)
us_bls_cpi_monthly_download.job_variables = _JOB_VARIABLES
_monthly_pipeline.extract_load_deployment = (
    us_bls_cpi_monthly_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# annual
# check_update: check_update: us_bls_cpi__annual
# extract_and_load: extract_and_load: us_bls_cpi__annual
# ──────────────────────────────────────────────────────────────────────────────

_annual_pipeline = make_pipeline("annual", date_format="%Y")


@flow(name=_annual_pipeline.check_update_flow_name, log_prints=True)
def us_bls_cpi_annual_check_update() -> None:
    _annual_pipeline.run_check_update()


us_bls_cpi_annual_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "annual"
)
us_bls_cpi_annual_check_update.deploy_schedules = _cron(16, 10)
us_bls_cpi_annual_check_update.job_variables = _JOB_VARIABLES


@flow(name=_annual_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bls_cpi_annual_download(download_params: dict) -> None:
    _annual_pipeline.run_extract_and_load(download_params)


us_bls_cpi_annual_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "annual"
)
us_bls_cpi_annual_download.job_variables = _JOB_VARIABLES
_annual_pipeline.extract_load_deployment = (
    us_bls_cpi_annual_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# semiannual
# check_update: check_update: us_bls_cpi__semiannual
# extract_and_load: extract_and_load: us_bls_cpi__semiannual
# ──────────────────────────────────────────────────────────────────────────────

_semiannual_pipeline = make_pipeline("semiannual", date_format="%Y")


@flow(name=_semiannual_pipeline.check_update_flow_name, log_prints=True)
def us_bls_cpi_semiannual_check_update() -> None:
    _semiannual_pipeline.run_check_update()


us_bls_cpi_semiannual_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "semiannual"
)
us_bls_cpi_semiannual_check_update.deploy_schedules = _cron(16, 15)
us_bls_cpi_semiannual_check_update.job_variables = _JOB_VARIABLES


@flow(name=_semiannual_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bls_cpi_semiannual_download(download_params: dict) -> None:
    _semiannual_pipeline.run_extract_and_load(download_params)


us_bls_cpi_semiannual_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "semiannual"
)
us_bls_cpi_semiannual_download.job_variables = _JOB_VARIABLES
_semiannual_pipeline.extract_load_deployment = (
    us_bls_cpi_semiannual_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: check_update: us_bls_cpi__dicionario
# extract_and_load: extract_and_load: us_bls_cpi__dicionario
#
# NonHistorical (no date column) — `make_get_latest_update` reuses the
# monthly table's `(year, month)` reference and compares against
# `Table.Update.latest` instead of `Coverage` for this table_id, see the
# banner in tasks.py.
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline("dicionario", date_format="%Y-%m")


@flow(name=_dicionario_pipeline.check_update_flow_name, log_prints=True)
def us_bls_cpi_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


us_bls_cpi_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "dicionario"
)
us_bls_cpi_dicionario_check_update.deploy_schedules = _cron(16, 20)
us_bls_cpi_dicionario_check_update.job_variables = _JOB_VARIABLES


@flow(name=_dicionario_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bls_cpi_dicionario_download(download_params: dict) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


us_bls_cpi_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "dicionario"
)
us_bls_cpi_dicionario_download.job_variables = _JOB_VARIABLES
_dicionario_pipeline.extract_load_deployment = (
    us_bls_cpi_dicionario_download.fn.__name__
)
