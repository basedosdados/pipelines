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
build_and_promote, one pair of flows per table (``CheckThenExtractLoadPipeline``
via ``pipeline_factory`` — the repo-wide default shape, same layout as
``br_ibge_ipca``). A single release zip builds all 4 tables at once
(``clean_all``, see the banner in ``tasks.py``), but each table's
check_update/extract_and_load pair re-resolves and re-downloads that zip
independently and keeps only the table it's responsible for — redundant
downloads accepted in exchange for uniformity, see the banner in ``tasks.py``.

Each table's check_update resolves the current release from the CKAN API and
polls cheaply first (the resolved ``snapshot_date`` vs the free ``Coverage``,
or vs ``Table.Update.latest`` for ``dicionario``), only dispatching its own
extract_and_load — which downloads the ~1.6 GB payload — when a newer
quarterly snapshot has actually been published, so a scheduled run is a cheap
no-op between quarterly releases. The 4 check_update schedules are staggered
across the same release days (30 min apart) so their extract_and_loads, if all
triggered by the same release, don't all claim a 16Gi worker at once.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers all 8 flows
below; the dev pool ignores the check_update schedules, the prod pool
activates them (deployed paused).
"""

from prefect.schedules import Cron, Schedule

from pipelines.datasets.au_geoscape_gnaf.constants import constants
from pipelines.datasets.au_geoscape_gnaf.tasks import (
    make_extract_load_data,
    make_get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags, pipeline_factory

DATASET_ID = constants.DATASET_ID.value

# The source republishes quarterly (Feb/May/Aug/Nov), landing mid-month; the
# exact day drifts (the Aug 2026 release landed on the 17th). Poll on several
# days across the second half of each release month at 16:00 BRT, staggered
# 30 min apart per table (see the module banner). The coverage-based source
# poll no-ops (no download) until a new snapshot appears.
_POLL_DAYS = "14,17,20,23,26"
_POLL_MONTHS = "2,5,8,11"


def _cron(hour: int, minute: int) -> list[Schedule]:
    return [
        Cron(
            f"{minute} {hour} {_POLL_DAYS} {_POLL_MONTHS} *",
            timezone="America/Sao_Paulo",
        )
    ]


make_pipeline = pipeline_factory(
    DATASET_ID,
    make_get_latest_update,
    make_extract_load_data,
)

# The clean step builds one state's frames at a time (NSW is the largest) and the
# download is ~1.6 GB; give each worker headroom, same as the old monolithic flow.
_EXTRACT_AND_LOAD_JOB_VARIABLES = {"memory": "16Gi"}

# ──────────────────────────────────────────────────────────────────────────────
# address_detail
# check_update: address_detail: au_geoscape_gnaf__address_detail
# extract_and_load: extract_and_load: au_geoscape_gnaf__address_detail
# ──────────────────────────────────────────────────────────────────────────────

_address_detail_pipeline = make_pipeline("address_detail")


@flow(name=_address_detail_pipeline.check_update_flow_name, log_prints=True)
def au_geoscape_gnaf_address_detail_check_update() -> None:
    _address_detail_pipeline.run_check_update()


au_geoscape_gnaf_address_detail_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "address_detail"
)
au_geoscape_gnaf_address_detail_check_update.deploy_schedules = _cron(16, 35)


@flow(
    name=_address_detail_pipeline.extract_and_load_flow_name, log_prints=True
)
def au_geoscape_gnaf_address_detail_download(download_params: dict) -> None:
    _address_detail_pipeline.run_extract_and_load(download_params)


au_geoscape_gnaf_address_detail_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "address_detail"
)
au_geoscape_gnaf_address_detail_download.job_variables = (
    _EXTRACT_AND_LOAD_JOB_VARIABLES
)
_address_detail_pipeline.extract_load_deployment = (
    au_geoscape_gnaf_address_detail_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# street_locality
# check_update: check_update: au_geoscape_gnaf__street_locality
# extract_and_load: extract_and_load: au_geoscape_gnaf__street_locality
# ──────────────────────────────────────────────────────────────────────────────

_street_locality_pipeline = make_pipeline("street_locality")


@flow(name=_street_locality_pipeline.check_update_flow_name, log_prints=True)
def au_geoscape_gnaf_street_locality_check_update() -> None:
    _street_locality_pipeline.run_check_update()


au_geoscape_gnaf_street_locality_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "street_locality"
)
au_geoscape_gnaf_street_locality_check_update.deploy_schedules = _cron(17, 5)


@flow(
    name=_street_locality_pipeline.extract_and_load_flow_name, log_prints=True
)
def au_geoscape_gnaf_street_locality_download(download_params: dict) -> None:
    _street_locality_pipeline.run_extract_and_load(download_params)


au_geoscape_gnaf_street_locality_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "street_locality"
)
au_geoscape_gnaf_street_locality_download.job_variables = (
    _EXTRACT_AND_LOAD_JOB_VARIABLES
)
_street_locality_pipeline.extract_load_deployment = (
    au_geoscape_gnaf_street_locality_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# locality
# check_update: check_update: au_geoscape_gnaf__locality
# extract_and_load: extract_and_load: au_geoscape_gnaf__locality
# ──────────────────────────────────────────────────────────────────────────────

_locality_pipeline = make_pipeline("locality")


@flow(name=_locality_pipeline.check_update_flow_name, log_prints=True)
def au_geoscape_gnaf_locality_check_update() -> None:
    _locality_pipeline.run_check_update()


au_geoscape_gnaf_locality_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "locality"
)
au_geoscape_gnaf_locality_check_update.deploy_schedules = _cron(17, 35)


@flow(name=_locality_pipeline.extract_and_load_flow_name, log_prints=True)
def au_geoscape_gnaf_locality_download(download_params: dict) -> None:
    _locality_pipeline.run_extract_and_load(download_params)


au_geoscape_gnaf_locality_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "locality"
)
au_geoscape_gnaf_locality_download.job_variables = (
    _EXTRACT_AND_LOAD_JOB_VARIABLES
)
_locality_pipeline.extract_load_deployment = (
    au_geoscape_gnaf_locality_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: check_update: au_geoscape_gnaf__dicionario
# extract_and_load: extract_and_load: au_geoscape_gnaf__dicionario
#
# NonHistorical (no date column) — `make_get_latest_update` compares against
# `Table.Update.latest` instead of `Coverage` for this table_id, see tasks.py.
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline("dicionario")


@flow(name=_dicionario_pipeline.check_update_flow_name, log_prints=True)
def au_geoscape_gnaf_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


au_geoscape_gnaf_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "dicionario"
)
au_geoscape_gnaf_dicionario_check_update.deploy_schedules = _cron(18, 5)


@flow(name=_dicionario_pipeline.extract_and_load_flow_name, log_prints=True)
def au_geoscape_gnaf_dicionario_download(download_params: dict) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


au_geoscape_gnaf_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "dicionario"
)
au_geoscape_gnaf_dicionario_download.job_variables = (
    _EXTRACT_AND_LOAD_JOB_VARIABLES
)
_dicionario_pipeline.extract_load_deployment = (
    au_geoscape_gnaf_dicionario_download.fn.__name__
)
