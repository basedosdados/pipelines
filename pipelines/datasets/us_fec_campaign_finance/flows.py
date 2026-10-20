"""
Flows for us_fec_campaign_finance — Prefect 3.

FEC bulk campaign-finance data. The FEC republishes the **current** election cycle
daily, so a scheduled run re-pulls only that cycle and overwrites its one partition.
Every other cycle stays in the staging bucket untouched, and the dbt models — plain
`materialized="table"` — rebuild the full 1980-present table from all of them.

**Past cycles are NOT frozen, and this flow does not pick up their changes.** Amendments
keep landing in a closed cycle for years; the bulk files are re-published when they do.
Checking ``Last-Modified`` on the individual-contributions zips (2026-08-20)::

    1980, 1996 -> 2017-08-31    2020 -> 2024-12-29
    2008       -> 2021-11-01    2024 -> 2026-03-08
    2016       -> 2022-10-16    2022 -> 2026-08-16   (four days earlier)

Only the oldest cycles look settled. Since each table's extract_and_load downloads just
the current cycle, an amendment filed today against 2022 never reaches these tables — the
data is correct as of each cycle's last ingest, not as of today. Note the full table
rebuild does **not** mitigate this: staging for past cycles never changes, so every
rebuilt past partition is recomputed byte-identical. Fixing it means widening the refresh
window to the current cycle plus one or two prior, which pairs naturally with an
incremental (``insert_overwrite`` on ``year``) materialization.

The upload uses ``dump_mode="append"``. "overwrite" would delete the whole staging table
and, via ``tb.delete(mode="all")``, the prod table with it — throwing away 45 years of
history to refresh one cycle. "append" with a deterministic blob path
(``staging/<ds>/<table>/year=<CYCLE>/data.parquet``) replaces exactly the current
cycle's partition, which is the intended semantics.

Migrated to the one-pipeline-per-table_id shape: 7 independent check_update /
extract_and_load pairs, one per refreshed table (candidate, committee,
candidate_committee_link, contribution_individual, contribution_committee,
committee_transaction, disbursement), wired through
``CheckThenExtractLoadPipeline``/``pipeline_factory`` (see ``tasks.py``). ``dicionario``
is not part of this: it is static, built once at onboarding and never rebuilt by the
recurring pipeline (see ``constants.py``).

Every FEC bulk file type is its own ZIP, so there is no shared download to decompose —
each table's extract_and_load already downloads only its own file, same as before the
split. The dataset's only same-dataset ``ref()`` is ``schema.yml``'s
``custom_dictionary_coverage`` test on each of the 7 tables, which reads
``ref('us_fec_campaign_finance__dicionario')``. Unlike ``br_bcb_ifdata`` (where the
referenced sibling is rebuilt by the very same recurring flow every cycle, creating a
real run-before-sibling-exists race), ``dicionario`` here is a permanent fixture of both
dev and prod from onboarding and is never touched again — so the generic
``dispatch_build_and_promote`` (one deployment per table, run+test per table
independently) is safe to use as-is, no batched materialization needed.

Two check strategies, split by table (see ``tasks.CONTENT_TABLES``):

* contribution_individual / contribution_committee / committee_transaction /
  disbursement (the four transaction tables): the source republishes the whole current
  cycle daily regardless of whether new rows actually landed, so a cheap Last-Modified
  HEAD isn't a reliable "new data" signal. check_update downloads and parses the current
  cycle for just that one table and compares the real max ``transaction_date`` against
  the table's own Coverage — paying the download cost twice (once in check_update, again
  in extract_and_load) when there is new data, per the house decision to always keep the
  two stages separate.
* candidate / committee / candidate_committee_link (registration snapshots): their
  Coverage is year-only (the two-year election cycle), which doesn't move within a
  cycle, so comparing against it would silently stop refreshing them after the first run
  of each cycle. check_update instead HEADs the table's own bulk ZIP and compares its
  Last-Modified against ``Table.Update.latest`` (``compare_against="table_update"``) —
  the same fix au_ato_abr uses for its rolling-window tables, and cheaper besides (no
  download at all).

The four transaction tables carry the BD Pro rolling window: the most recent 6 months
are pro-only, everything older is free. The registration tables (candidate, committee,
candidate_committee_link) have no date column and stay fully free.

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers all 14 flows below.
The dev pool strips the schedules entirely. The prod pool keeps them but deploys
``paused=True``, and the backend sync leaves an unknown deployment paused — arming is a
manual step in Django admin (``/admin/admin_data_tools/disabledflowschedule/``), not a
consequence of merging.
"""

from prefect.schedules import Cron

from pipelines.datasets.us_fec_campaign_finance.constants import (
    CANDIDATE_COMMITTEE_LINK_TABLE_ID,
    CANDIDATE_TABLE_ID,
    COMMITTEE_TABLE_ID,
    COMMITTEE_TRANSACTION_TABLE_ID,
    CONTRIBUTION_COMMITTEE_TABLE_ID,
    CONTRIBUTION_INDIVIDUAL_TABLE_ID,
    DISBURSEMENT_TABLE_ID,
    constants,
)
from pipelines.datasets.us_fec_campaign_finance.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value

# The FEC reposts the current cycle's bulk files daily, but individual contributions
# alone are multi-GB, so a daily full-cycle re-pull is wasteful. Weekly on Sunday at
# 05:00 BRT keeps the lag under a week outside the pre-election crunch; the
# check_update poll makes a week with nothing new a cheap no-op. Same cadence for all
# 7 tables — matches the previous single-flow cron.
_CHECK_UPDATE_SCHEDULE = [Cron("20 5 * * 0", timezone="America/Sao_Paulo")]

# contribution_individual's current-cycle file is ~2 GB compressed and is parsed in 1M-
# row chunks; the other three transaction tables are smaller but still sizable, so size
# every heavy table's worker the same as before the split. Both check_update (which
# downloads and parses the file to find the real max transaction_date) and
# extract_and_load pay this cost. The three registration snapshots (candidate,
# committee, candidate_committee_link) are small and use the pool default instead.
_HEAVY_JOB_VARIABLES = {"memory": "8Gi"}

# ──────────────────────────────────────────────────────────────────────────────
# contribution_individual
# check_update: us_fec_campaign_finance__contribution_individual
# extract_and_load: us_fec_campaign_finance__contribution_individual
# ──────────────────────────────────────────────────────────────────────────────

_contribution_individual_pipeline = make_pipeline(
    CONTRIBUTION_INDIVIDUAL_TABLE_ID
)


@flow(
    name=_contribution_individual_pipeline.check_update_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_contribution_individual_check_update() -> None:
    _contribution_individual_pipeline.run_check_update()


us_fec_campaign_finance_contribution_individual_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, CONTRIBUTION_INDIVIDUAL_TABLE_ID
    )
)
us_fec_campaign_finance_contribution_individual_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE
us_fec_campaign_finance_contribution_individual_check_update.job_variables = (
    _HEAVY_JOB_VARIABLES
)


@flow(
    name=_contribution_individual_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_contribution_individual_download(
    download_params: dict,
) -> None:
    _contribution_individual_pipeline.run_extract_and_load(download_params)


us_fec_campaign_finance_contribution_individual_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, CONTRIBUTION_INDIVIDUAL_TABLE_ID
    )
)
us_fec_campaign_finance_contribution_individual_download.job_variables = (
    _HEAVY_JOB_VARIABLES
)
_contribution_individual_pipeline.extract_load_deployment = (
    us_fec_campaign_finance_contribution_individual_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# contribution_committee
# check_update: us_fec_campaign_finance__contribution_committee
# extract_and_load: us_fec_campaign_finance__contribution_committee
# ──────────────────────────────────────────────────────────────────────────────

_contribution_committee_pipeline = make_pipeline(
    CONTRIBUTION_COMMITTEE_TABLE_ID
)


@flow(
    name=_contribution_committee_pipeline.check_update_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_contribution_committee_check_update() -> None:
    _contribution_committee_pipeline.run_check_update()


us_fec_campaign_finance_contribution_committee_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, CONTRIBUTION_COMMITTEE_TABLE_ID
    )
)
us_fec_campaign_finance_contribution_committee_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE
us_fec_campaign_finance_contribution_committee_check_update.job_variables = (
    _HEAVY_JOB_VARIABLES
)


@flow(
    name=_contribution_committee_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_contribution_committee_download(
    download_params: dict,
) -> None:
    _contribution_committee_pipeline.run_extract_and_load(download_params)


us_fec_campaign_finance_contribution_committee_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, CONTRIBUTION_COMMITTEE_TABLE_ID
    )
)
us_fec_campaign_finance_contribution_committee_download.job_variables = (
    _HEAVY_JOB_VARIABLES
)
_contribution_committee_pipeline.extract_load_deployment = (
    us_fec_campaign_finance_contribution_committee_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# committee_transaction
# check_update: us_fec_campaign_finance__committee_transaction
# extract_and_load: us_fec_campaign_finance__committee_transaction
# ──────────────────────────────────────────────────────────────────────────────

_committee_transaction_pipeline = make_pipeline(COMMITTEE_TRANSACTION_TABLE_ID)


@flow(
    name=_committee_transaction_pipeline.check_update_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_committee_transaction_check_update() -> None:
    _committee_transaction_pipeline.run_check_update()


us_fec_campaign_finance_committee_transaction_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, COMMITTEE_TRANSACTION_TABLE_ID)
)
us_fec_campaign_finance_committee_transaction_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)
us_fec_campaign_finance_committee_transaction_check_update.job_variables = (
    _HEAVY_JOB_VARIABLES
)


@flow(
    name=_committee_transaction_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_committee_transaction_download(
    download_params: dict,
) -> None:
    _committee_transaction_pipeline.run_extract_and_load(download_params)


us_fec_campaign_finance_committee_transaction_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, COMMITTEE_TRANSACTION_TABLE_ID
    )
)
us_fec_campaign_finance_committee_transaction_download.job_variables = (
    _HEAVY_JOB_VARIABLES
)
_committee_transaction_pipeline.extract_load_deployment = (
    us_fec_campaign_finance_committee_transaction_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# disbursement
# check_update: us_fec_campaign_finance__disbursement
# extract_and_load: us_fec_campaign_finance__disbursement
# ──────────────────────────────────────────────────────────────────────────────

_disbursement_pipeline = make_pipeline(DISBURSEMENT_TABLE_ID)


@flow(name=_disbursement_pipeline.check_update_flow_name, log_prints=True)
def us_fec_campaign_finance_disbursement_check_update() -> None:
    _disbursement_pipeline.run_check_update()


us_fec_campaign_finance_disbursement_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DISBURSEMENT_TABLE_ID
)
us_fec_campaign_finance_disbursement_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)
us_fec_campaign_finance_disbursement_check_update.job_variables = (
    _HEAVY_JOB_VARIABLES
)


@flow(name=_disbursement_pipeline.extract_and_load_flow_name, log_prints=True)
def us_fec_campaign_finance_disbursement_download(
    download_params: dict,
) -> None:
    _disbursement_pipeline.run_extract_and_load(download_params)


us_fec_campaign_finance_disbursement_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DISBURSEMENT_TABLE_ID
)
us_fec_campaign_finance_disbursement_download.job_variables = (
    _HEAVY_JOB_VARIABLES
)
_disbursement_pipeline.extract_load_deployment = (
    us_fec_campaign_finance_disbursement_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# candidate
# check_update: us_fec_campaign_finance__candidate
# extract_and_load: us_fec_campaign_finance__candidate
# ──────────────────────────────────────────────────────────────────────────────

_candidate_pipeline = make_pipeline(CANDIDATE_TABLE_ID)


@flow(name=_candidate_pipeline.check_update_flow_name, log_prints=True)
def us_fec_campaign_finance_candidate_check_update() -> None:
    _candidate_pipeline.run_check_update()


us_fec_campaign_finance_candidate_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CANDIDATE_TABLE_ID
)
us_fec_campaign_finance_candidate_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(name=_candidate_pipeline.extract_and_load_flow_name, log_prints=True)
def us_fec_campaign_finance_candidate_download(download_params: dict) -> None:
    _candidate_pipeline.run_extract_and_load(download_params)


us_fec_campaign_finance_candidate_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CANDIDATE_TABLE_ID
)
_candidate_pipeline.extract_load_deployment = (
    us_fec_campaign_finance_candidate_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# committee
# check_update: us_fec_campaign_finance__committee
# extract_and_load: us_fec_campaign_finance__committee
# ──────────────────────────────────────────────────────────────────────────────

_committee_pipeline = make_pipeline(COMMITTEE_TABLE_ID)


@flow(name=_committee_pipeline.check_update_flow_name, log_prints=True)
def us_fec_campaign_finance_committee_check_update() -> None:
    _committee_pipeline.run_check_update()


us_fec_campaign_finance_committee_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, COMMITTEE_TABLE_ID
)
us_fec_campaign_finance_committee_check_update.deploy_schedules = (
    _CHECK_UPDATE_SCHEDULE
)


@flow(name=_committee_pipeline.extract_and_load_flow_name, log_prints=True)
def us_fec_campaign_finance_committee_download(download_params: dict) -> None:
    _committee_pipeline.run_extract_and_load(download_params)


us_fec_campaign_finance_committee_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, COMMITTEE_TABLE_ID
)
_committee_pipeline.extract_load_deployment = (
    us_fec_campaign_finance_committee_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# candidate_committee_link
# check_update: us_fec_campaign_finance__candidate_committee_link
# extract_and_load: us_fec_campaign_finance__candidate_committee_link
# ──────────────────────────────────────────────────────────────────────────────

_candidate_committee_link_pipeline = make_pipeline(
    CANDIDATE_COMMITTEE_LINK_TABLE_ID
)


@flow(
    name=_candidate_committee_link_pipeline.check_update_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_candidate_committee_link_check_update() -> None:
    _candidate_committee_link_pipeline.run_check_update()


us_fec_campaign_finance_candidate_committee_link_check_update.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.CHECK_UPDATE, CANDIDATE_COMMITTEE_LINK_TABLE_ID
    )
)
us_fec_campaign_finance_candidate_committee_link_check_update.deploy_schedules = _CHECK_UPDATE_SCHEDULE


@flow(
    name=_candidate_committee_link_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def us_fec_campaign_finance_candidate_committee_link_download(
    download_params: dict,
) -> None:
    _candidate_committee_link_pipeline.run_extract_and_load(download_params)


us_fec_campaign_finance_candidate_committee_link_download.deploy_tags = (
    deploy_tags(
        DATASET_ID, Etapa.EXTRACT_AND_LOAD, CANDIDATE_COMMITTEE_LINK_TABLE_ID
    )
)
_candidate_committee_link_pipeline.extract_load_deployment = (
    us_fec_campaign_finance_candidate_committee_link_download.fn.__name__
)
