"""
Flows for us_dot_fars — Prefect 3.

NHTSA's Fatality Analysis Reporting System publishes one annual zip per year at a
predictable URL, roughly a year in arrears: the Annual Report File for year Y
lands late in Y+1. It also **restates**: the ARF revises the preceding year's
file, and NHTSA has re-cut the whole 1975-2024 series into CSV at least once.

So each run re-downloads and re-cleans **every** year and rewrites every year
partition, exactly as the NOAA storm-events flow does and for the same reason.
An append-only refresh keyed on the new year would silently miss every revision
to the prior year, which is the one revision FARS makes on a schedule. Rebuilding
the whole corpus is affordable at this cadence — the flow runs a handful of times
a year — and it makes the restatement problem structurally impossible instead of
inferred from state this pipeline has nowhere to keep.

The BigQuery cost is unchanged either way: the dbt models are
``materialized="table"``, so they rebuild in full from the staging external table
on every run regardless of how many partitions moved.

``dump_mode="append"`` is load-bearing: ``"overwrite"`` calls
``tb.delete(mode="all")``, which drops the **production** table, and fires from
the dev half of the flow too. With ``"append"`` the upload ends in
``Storage.upload(if_exists="replace")``, which replaces each partition blob at its
own path — the same end state, without the delete.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_dot_fars_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_dot_fars.constants import constants
from pipelines.datasets.us_dot_fars.tasks import clean_corpus, probe_source
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
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
PRIMARY_TABLE = constants.PRIMARY_TABLE.value

# Coverage spec per table.
#
# FARS is an ANNUAL release, so none of these tables takes the BD Pro rolling
# window: the house rule paywalls the recent slice only for tables refreshed
# monthly or more often, and lower-frequency tables stay free. All three data
# tables are therefore AllFree, which also means this flow never issues BigQuery
# Row Access Policies — needs_row_access_policy is true only for PartBdpro.
#
# `dicionario` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in constants.DATA_TABLES.value
}


@flow(name="us_dot_fars", log_prints=True)
def us_dot_fars_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the Fatality Analysis Reporting System from NHTSA's annual files.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
            This is the switch for an Annual Report File that revises the prior
            year without adding a new one, which leaves the max coverage date
            unmoved and so does not trip the poll.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=PRIMARY_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_dot_fars_")
    try:
        probe = probe_source()
        print(f"source years {probe['years'][0]}-{probe['latest']}")
        print(f"source max coverage date: {probe['max_date']}")

        # Skip the run when NHTSA has not published a newer year. A scheduled run
        # is then a cheap no-op — note Prefect still reports COMPLETED, so read
        # the logs rather than the state to tell a no-op from a real ingest.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=PRIMARY_TABLE,
            source_max_date=probe["max_date"],
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            print("Não há novas atualizações na fonte original")
            return

        result = clean_corpus(work_dir=work_dir, probe=probe)
        print(f"row counts: {result['row_counts']}")

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=PRIMARY_TABLE,
            source_max_date=probe["max_date"],
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Build EVERY table before testing ANY of them, in both environments.
        # Each data table's custom_dictionary_coverage test reads dicionario via
        # ref(), and vehicle and person both have a relationships test against
        # crash. Interleaved run/test per table, those tests would run before the
        # sibling they reference exists and fail with "Not found: Table ...".
        # A re-run hides it, because a stale sibling from an earlier build
        # survives — so it only bites in a clean environment, which is prod.
        if not materialize_to_prod:
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
            return

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
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        # The corpus is ~620 MB of CSV zips plus ~500 MB of SAS zips for the
        # pre-2015 codebook, and the parquet it produces.
        shutil.rmtree(work_dir, ignore_errors=True)


# FARS is annual and lands late in the following year, but NHTSA does not commit
# to a date, so poll twice a month and let the source-poll guard no-op. The probe
# step only issues HEAD requests, so a no-op run costs almost nothing.
# The minute and hour are chosen, not defaulted: 38 11 is unused across the repo.
# Defaulting to :00 piles every pipeline onto the same instant, where they compete
# for BigQuery slots and trip the daily byte quota together.
# pyrefly: ignore [missing-attribute]
us_dot_fars_flow.deploy_schedules = [
    {"cron": "38 11 6,21 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds one year of rows in memory at a time (2015's person file
# is the largest at ~82k rows), but the dicionario build reads each pre-2015
# sas7bdat through pandas to recover the variable -> format binding, which is the
# real peak.
#
# memory_limit is the key the work pool's job template actually exposes; a flow
# that sets only `memory` is silently capped at the 4Gi default no matter what
# value it names.
# pyrefly: ignore [missing-attribute]
us_dot_fars_flow.job_variables = {
    "memory": "8Gi",
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
