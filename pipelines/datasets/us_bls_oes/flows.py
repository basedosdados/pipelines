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
build_and_promote, one pair of flows per table (`area`, `industry`) — the usual
one-pipeline-per-table_id shape (`pipelines.utils.stage_dispatch.pipeline_factory`
/ `CheckThenExtractLoadPipeline`), same as `br_ibge_ipca`. Both tables are built
from the same release zip, so each table's extract_and_load downloads + cleans
the release independently (redundancy accepted for uniformity — see the banner
in `tasks.py`); there is no cross-table dependency left to justify the old
single-check_update/single-extract_and_load shape this dataset used before.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers the flows
below; the dev pool ignores the check_update schedules, the prod pool activates
them.
"""

from prefect.schedules import Cron

from pipelines.datasets.us_bls_oes.constants import DATASET_ID, constants
from pipelines.datasets.us_bls_oes.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

AREA_TABLE_ID, INDUSTRY_TABLE_ID = constants.DATA_TABLES.value

# OEWS releases once a year, in the northern spring, with the exact date varying
# between late March and May. Poll weekly across those three months; the
# source-poll guard no-ops until a new reference year actually appears. Same
# cron for both tables — they share one release.
_CHECK_UPDATE_SCHEDULE = [
    Cron("47 17 1,8,15,22,29 3,4,5 *", timezone="America/Sao_Paulo")
]
# One release is ~430k rows held in pandas plus the Excel reader's own buffers.
_EXTRACT_AND_LOAD_JOB_VARIABLES = {"memory": "8Gi"}

# ──────────────────────────────────────────────────────────────────────────────
# area
# check_update: us_bls_oes__area
# extract_and_load: us_bls_oes__area
# ──────────────────────────────────────────────────────────────────────────────

_area_pipeline = make_pipeline(AREA_TABLE_ID)


@flow(name=_area_pipeline.check_update_flow_name, log_prints=True)
def us_bls_oes_area_check_update() -> None:
    """Poll BLS for a newer OEWS reference year, dispatch extract_and_load."""
    _area_pipeline.run_check_update()


us_bls_oes_area_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, AREA_TABLE_ID
)
us_bls_oes_area_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_area_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bls_oes_area_extract_and_load(download_params: dict) -> None:
    """Download + clean the release, upload `area`, dispatch promotion.

    Args:
        download_params: dict received from check_update via
            `run_deployment()` — `reference_date` (`"<year>-05-01"`).
    """
    _area_pipeline.run_extract_and_load(download_params)


us_bls_oes_area_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, AREA_TABLE_ID
)
us_bls_oes_area_extract_and_load.job_variables = (
    _EXTRACT_AND_LOAD_JOB_VARIABLES
)
_area_pipeline.extract_load_deployment = (
    us_bls_oes_area_extract_and_load.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# industry
# check_update: us_bls_oes__industry
# extract_and_load: us_bls_oes__industry
# ──────────────────────────────────────────────────────────────────────────────

_industry_pipeline = make_pipeline(INDUSTRY_TABLE_ID)


@flow(name=_industry_pipeline.check_update_flow_name, log_prints=True)
def us_bls_oes_industry_check_update() -> None:
    """Poll BLS for a newer OEWS reference year, dispatch extract_and_load."""
    _industry_pipeline.run_check_update()


us_bls_oes_industry_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, INDUSTRY_TABLE_ID
)
us_bls_oes_industry_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(name=_industry_pipeline.extract_and_load_flow_name, log_prints=True)
def us_bls_oes_industry_extract_and_load(download_params: dict) -> None:
    """Download + clean the release, upload `industry`, dispatch promotion.

    Args:
        download_params: dict received from check_update via
            `run_deployment()` — `reference_date` (`"<year>-05-01"`).
    """
    _industry_pipeline.run_extract_and_load(download_params)


us_bls_oes_industry_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, INDUSTRY_TABLE_ID
)
us_bls_oes_industry_extract_and_load.job_variables = (
    _EXTRACT_AND_LOAD_JOB_VARIABLES
)
_industry_pipeline.extract_load_deployment = (
    us_bls_oes_industry_extract_and_load.fn.__name__
)
