"""
Flows for us_fec_campaign_finance — Prefect 3.

FEC bulk campaign-finance data. The FEC republishes the **current** election cycle
daily and freezes past cycles, so a scheduled run re-pulls only the current cycle and
overwrites that one partition. Every frozen cycle stays in the staging bucket
untouched, and the dbt models — plain `materialized="table"` — rebuild the full
1980-present table from all of them.

That is why the upload uses ``dump_mode="append"``. "overwrite" would delete the whole
staging table and, via ``tb.delete(mode="all")``, the prod table with it — throwing
away 45 years of frozen cycles to refresh one. "append" with a deterministic blob path
(``staging/<ds>/<table>/cycle=<CYCLE>/data.parquet``) replaces exactly the current
cycle's partition, which is the intended semantics.

The four transaction tables are high-frequency, so they carry the BD Pro rolling
window: the most recent 6 months are pro-only, everything older is free. The
registration tables (candidate, committee, candidate_committee_link) have no date
column and stay fully free.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``us_fec_campaign_finance_flow``;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_fec_campaign_finance.constants import constants
from pipelines.datasets.us_fec_campaign_finance.tasks import (
    refresh_current_cycle,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
    YearOnly,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value

# The table the source poll and the source Update are anchored on. It is the
# highest-volume table and the one that moves first when the FEC publishes, so it
# is the right proxy for "did the source release anything new".
POLL_TABLE = "contribution_individual"

# Coverage spec per table.
#
# The four transaction tables refresh daily, so the house rule paywalls their most
# recent window: free up to source_max - 6 months, pro after that.
# register_table_materialization_task recomputes free_end and re-issues the BigQuery
# Row Access Policies on every run, so the window rolls forward by itself and the dbt
# models stay untouched. part_bdpro requires BOTH a free (is_closed=False) and a pro
# (is_closed=True) Coverage to already exist on the table, or assert_coverage_topology
# raises before anything is written — both are created at onboarding.
#
# candidate / committee / candidate_committee_link are registration snapshots with no
# date column, so a rolling window is not defined for them and they stay AllFree on
# the cycle. dicionario is static and is not refreshed at all.
_FREE_LAG = FreeLag(unit="months", value=6)

_COVERAGE = {
    "contribution_individual": PartBdpro(
        date_column=DateOnly(col="transaction_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    "contribution_committee": PartBdpro(
        date_column=DateOnly(col="transaction_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    "committee_transaction": PartBdpro(
        date_column=DateOnly(col="transaction_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    "disbursement": PartBdpro(
        date_column=DateOnly(col="transaction_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=_FREE_LAG,
    ),
    # Registration snapshots: their only temporal column is the cycle, so coverage
    # is expressed on it at year granularity.
    "candidate": AllFree(
        date_column=YearOnly(col="cycle"), date_format=DateFormat.YEAR
    ),
    "committee": AllFree(
        date_column=YearOnly(col="cycle"), date_format=DateFormat.YEAR
    ),
    "candidate_committee_link": AllFree(
        date_column=YearOnly(col="cycle"), date_format=DateFormat.YEAR
    ),
}


@flow(name="us_fec_campaign_finance", log_prints=True)
def us_fec_campaign_finance_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    cycle: int | None = None,
) -> None:
    """Re-pull the current FEC election cycle and materialize every table.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the prod
            staging bucket and run dbt against ``target="prod"``. Set False to
            exercise only the dev half — required for a safe test run, since the
            default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new data.
        cycle: Refresh this cycle instead of the current one. For backfilling a
            single past cycle by hand; leave unset on scheduled runs.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_fec_campaign_finance_")
    try:
        result = refresh_current_cycle(work_dir=work_dir, cycle=cycle)
        max_date = result["max_date"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Commit the source Update before materializing: if the flow fails midway,
        # the source metadata still records that new data was published.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = [t for t in constants.ALL_TABLES.value if t in result]

        # Run every table, THEN test every table — never interleave per table. The
        # custom_dictionary_coverage tests read the sibling dicionario model, so a
        # per-table run/test would test a table before its sibling exists in a clean
        # environment. Same class of bug as us_fed_fred's observation<->series FK.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="dev",
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        # Prod: upload + run ALL tables first, THEN test (see the dev-phase note).
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="prod",
            )

        if update_metadata:
            for table in tables:
                # Indexed, not .get() with a default: every refreshed table must
                # declare its own tier. A silent fallback here would register a
                # PartBdpro table as free and quietly disable its paywall, so a
                # missing spec should fail loudly instead.
                coverage = _COVERAGE[table]
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# The FEC reposts the current cycle's bulk files daily, but individual contributions
# alone are multi-GB, so a daily full-cycle re-pull is wasteful. Weekly on Sunday at
# 05:00 BRT keeps the lag under a week outside the pre-election crunch; the
# source-poll guard makes a run with nothing new a cheap no-op.
# pyrefly: ignore [missing-attribute]
us_fec_campaign_finance_flow.deploy_schedules = [
    {"cron": "0 5 * * 0", "timezone": "America/Sao_Paulo"}
]

# The current cycle's individual-contributions file is ~2 GB compressed and is parsed
# in 1M-row chunks; size the worker to the parse peak plus parquet buffers.
# pyrefly: ignore [missing-attribute]
us_fec_campaign_finance_flow.job_variables = {"memory": "8Gi"}
