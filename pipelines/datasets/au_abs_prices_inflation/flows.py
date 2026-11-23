"""Flows for au_abs_prices_inflation — Prefect 3.

Migrated to the one-flow-per-table shape: 7 independent check_update /
extract_and_load pairs -- `cpi_quarterly`, `cpi_monthly`, and the five other
ABS releases (`wage_price_index`, `producer_price_index`,
`international_trade_price_index`, `living_cost_index`, `dwelling_value`) --
wired through `CheckThenExtractLoadPipeline`/`pipeline_factory` (see
`tasks.py`). No cross-table dbt test ties any of these tables together
(`models/au_abs_prices_inflation/schema.yml` scopes every test to its own
model), so the repo-wide one-pipeline-per-table_id rule applies with no
exception here -- including the two CPI tables, which used to share a single
flow (see below).

ABS exposes no cheap "has this changed" signal (no trustworthy Last-Modified,
no version header): the only way to know the latest published period is to
download and parse the release's workbooks. So, same as every other
staged-pipeline dataset, check_update downloads and parses whatever
extract_and_load would need to decide the reference date, and
extract_and_load downloads and parses it again, independently, when
dispatched -- duplicate download cost, accepted in exchange for the uniform
check_update -> extract_and_load shape (see `tasks.py`'s module docstring).

`cpi_quarterly` and `cpi_monthly` used to share one flow
(`au_abs_prices_inflation_cpi_flow`) that downloaded the release once and
materialized both tables inline. Splitting them one-per-table costs nothing
extra here: `constants.SOURCE_TABLES` already lists each frequency's source
workbooks disjointly, so `cpi.download_frequency` downloads only what each
table's own pipeline needs -- no redundant fetch between the two, unlike the
genuinely-shared-download cases (`au_ato_abr`, `br_mf_divida_ativa`) that
still pay a real duplicate-download cost for the one-per-table shape. The
five other releases were already one table per flow; they gain the
check_update/extract_and_load split and `build_and_promote` dispatch instead
of materializing inline.

Every ABS release ships the full history in its workbooks, so each run is a
full replace, not an incremental append -- see `tasks.py`'s module docstring
for why `ExtractAndLoad`'s default `dump_mode="append"` is correct here
despite the name. The source poll (`CheckThenExtractLoadPipeline.run_check_update`)
short-circuits a run until the source has published a newer period, which
makes a scheduled check_update run a cheap no-op between releases.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers every flow
defined here; the dev pool ignores the schedules, the prod pool activates them.
"""

from prefect.schedules import Cron

from pipelines.datasets.au_abs_prices_inflation.constants import (
    CPI_MONTHLY_TABLE_ID,
    CPI_QUARTERLY_TABLE_ID,
    constants,
)
from pipelines.datasets.au_abs_prices_inflation.tasks import (
    make_cpi_pipeline,
    make_release_pipeline,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value

# ABS publishes the monthly CPI release in the last week of each month (moving
# to the 4th Wednesday from Feb 2027). Poll across the last week at 16:00 BRT;
# the source-poll guard no-ops until a new period lands for that table -- a
# new month for cpi_monthly, a new quarter for cpi_quarterly (same release,
# different cadence per table, see constants.py's CPI_COVERAGE comment).
_CPI_CHECK_UPDATE_SCHEDULE = [
    Cron("15 16 22,23,24,25,26,27,28 * *", timezone="America/Sao_Paulo")
]

# Poll windows follow each release's published calendar, read off its landing
# page. The minutes are deliberately distinct and none is 0: same-instant runs
# compete for BigQuery slots, and if the daily quota trips they all fail
# together. The source poll no-ops until a new quarter lands, so a generous
# window costs only a download.
_RELEASE_CHECK_UPDATE_CRON = {
    # Released mid-February, May, August and November (18 Nov 2026, 17 Feb 2027).
    "wage_price_index": "17 14 16,17,18,19,20,21,22 2,5,8,11 *",
    # Released end of January, April, July and October (30 Oct 2026, 29 Jan 2027).
    "producer_price_index": "27 14 27,28,29,30,31 1,4,7,10 *",
    # Released a day before Producer (29 Oct 2026, 28 Jan 2027).
    "international_trade_price_index": "32 14 26,27,28,29,30,31 1,4,7,10 *",
    # Released in the first week of February, May, August and November.
    "living_cost_index": "37 14 1,2,3,4,5,6,7,8 2,5,8,11 *",
    # Released in the first fortnight of March, June, September and December.
    "dwelling_value": "42 14 1,2,3,4,5,6,7,8,9,10,11,12 3,6,9,12 *",
}


def _build_cpi_table_flows(table_id: str, schedule: list):
    """Build one CPI table's check_update/extract_and_load flow pair."""
    pipeline = make_cpi_pipeline(table_id)

    @flow(name=pipeline.check_update_flow_name, log_prints=True)
    def check_update() -> None:
        pipeline.run_check_update()

    check_update.deploy_tags = deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, table_id
    )
    check_update.deploy_schedules = schedule

    @flow(name=pipeline.extract_and_load_flow_name, log_prints=True)
    def extract_and_load(download_params: dict) -> None:
        pipeline.run_extract_and_load(download_params)

    extract_and_load.deploy_tags = deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, table_id
    )
    pipeline.extract_load_deployment = extract_and_load.fn.__name__

    return check_update, extract_and_load


def _build_release_flows(release: str):
    """Build one ABS release's check_update/extract_and_load flow pair."""
    pipeline = make_release_pipeline(release)

    @flow(name=pipeline.check_update_flow_name, log_prints=True)
    def check_update() -> None:
        pipeline.run_check_update()

    check_update.deploy_tags = deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, release
    )
    check_update.deploy_schedules = [
        Cron(_RELEASE_CHECK_UPDATE_CRON[release], timezone="America/Sao_Paulo")
    ]

    @flow(name=pipeline.extract_and_load_flow_name, log_prints=True)
    def extract_and_load(download_params: dict) -> None:
        pipeline.run_extract_and_load(download_params)

    extract_and_load.deploy_tags = deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, release
    )
    pipeline.extract_load_deployment = extract_and_load.fn.__name__

    return check_update, extract_and_load


# ──────────────────────────────────────────────────────────────────────────────
# cpi_quarterly
# check_update: au_abs_prices_inflation__cpi_quarterly
# extract_and_load: au_abs_prices_inflation__cpi_quarterly
# ──────────────────────────────────────────────────────────────────────────────

(
    au_abs_prices_inflation_cpi_quarterly_check_update,
    au_abs_prices_inflation_cpi_quarterly_download,
) = _build_cpi_table_flows(CPI_QUARTERLY_TABLE_ID, _CPI_CHECK_UPDATE_SCHEDULE)


# ──────────────────────────────────────────────────────────────────────────────
# cpi_monthly
# check_update: au_abs_prices_inflation__cpi_monthly
# extract_and_load: au_abs_prices_inflation__cpi_monthly
# ──────────────────────────────────────────────────────────────────────────────

(
    au_abs_prices_inflation_cpi_monthly_check_update,
    au_abs_prices_inflation_cpi_monthly_download,
) = _build_cpi_table_flows(CPI_MONTHLY_TABLE_ID, _CPI_CHECK_UPDATE_SCHEDULE)


# ──────────────────────────────────────────────────────────────────────────────
# wage_price_index
# check_update: au_abs_prices_inflation__wage_price_index
# extract_and_load: au_abs_prices_inflation__wage_price_index
# ──────────────────────────────────────────────────────────────────────────────

(
    au_abs_prices_inflation_wage_price_index_check_update,
    au_abs_prices_inflation_wage_price_index_download,
) = _build_release_flows("wage_price_index")


# ──────────────────────────────────────────────────────────────────────────────
# producer_price_index
# check_update: au_abs_prices_inflation__producer_price_index
# extract_and_load: au_abs_prices_inflation__producer_price_index
# ──────────────────────────────────────────────────────────────────────────────

(
    au_abs_prices_inflation_producer_price_index_check_update,
    au_abs_prices_inflation_producer_price_index_download,
) = _build_release_flows("producer_price_index")


# ──────────────────────────────────────────────────────────────────────────────
# international_trade_price_index
# check_update: au_abs_prices_inflation__international_trade_price_index
# extract_and_load: au_abs_prices_inflation__international_trade_price_index
# ──────────────────────────────────────────────────────────────────────────────

(
    au_abs_prices_inflation_international_trade_price_index_check_update,
    au_abs_prices_inflation_international_trade_price_index_download,
) = _build_release_flows("international_trade_price_index")


# ──────────────────────────────────────────────────────────────────────────────
# living_cost_index
# check_update: au_abs_prices_inflation__living_cost_index
# extract_and_load: au_abs_prices_inflation__living_cost_index
# ──────────────────────────────────────────────────────────────────────────────

(
    au_abs_prices_inflation_living_cost_index_check_update,
    au_abs_prices_inflation_living_cost_index_download,
) = _build_release_flows("living_cost_index")


# ──────────────────────────────────────────────────────────────────────────────
# dwelling_value
# check_update: au_abs_prices_inflation__dwelling_value
# extract_and_load: au_abs_prices_inflation__dwelling_value
# ──────────────────────────────────────────────────────────────────────────────

(
    au_abs_prices_inflation_dwelling_value_check_update,
    au_abs_prices_inflation_dwelling_value_download,
) = _build_release_flows("dwelling_value")
