"""
Flows for us_cfpb_complaints — Prefect 3.

CFPB Consumer Complaint Database. The CFPB publishes a **full snapshot of the whole
database every day**, so each run re-cleans the entire export and rewrites every year
partition. That is deliberate rather than lazy: complaints do not only get added, they
get *revised*. On an existing row, ``company_response_to_consumer`` moves off
``In progress``, ``timely_response`` resolves, ``company_public_response`` is published
within 180 days, and the consumer narrative arrives months later once the CFPB has
scrubbed it (only 2.4% of 2026 complaints carry one, against 21.9% overall). An
append-only load keyed on ``complaint_id`` would capture none of those.

Rewriting everything is cheap here because the expensive step — parsing the 9.3 GB CSV —
has to happen regardless. The BigQuery cost is one scan of the ~1.4 GB parquet.

``dump_mode="append"`` is load-bearing: ``"overwrite"`` calls ``tb.delete(mode="all")``,
which drops the **production** table, and fires from the dev half of the flow too. With
``"append"`` the upload ends in ``Storage.upload(if_exists="replace")``, which replaces
each partition blob wholesale — the same end state, without the delete.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_cfpb_complaints_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_cfpb_complaints.constants import constants
from pipelines.datasets.us_cfpb_complaints.tasks import (
    clean_complaints,
    download_complaints,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
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
COMPLAINT = constants.COMPLAINT.value
DICIONARIO = constants.DICIONARIO.value

# Coverage spec per table.
#
# `complaint` refreshes daily, so it carries the BD Pro rolling window: the most
# recent 6 months are pro-only, everything older is free. Each run recomputes
# free_end = source_end - free_lag, rewrites both DateTimeRanges, and re-issues the
# BigQuery Row Access Policies, so the window slides forward on its own.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises before
# anything is written.
#
# `dicionario` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    COMPLAINT: PartBdpro(
        date_column=DateOnly(col="date_received"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
}


@flow(name="us_cfpb_complaints", log_prints=True)
def us_cfpb_complaints_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the CFPB Consumer Complaint Database from the daily full snapshot.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the prod
            staging bucket and run dbt against ``target="prod"``. Set False to
            exercise only the dev half — required for a safe test run, since the
            default writes production and applies the Row Access Policies.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=COMPLAINT
    )

    work_dir = tempfile.mkdtemp(prefix="us_cfpb_complaints_")
    try:
        input_dir = download_complaints(work_dir=work_dir)
        result = clean_complaints(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_date_received"]
        print(f"snapshot max date_received: {max_date}")
        print(f"row counts: {result['row_counts']}")

        # Skip the run when the CFPB has not published a newer complaint date.
        # A scheduled run is then a cheap no-op — note Prefect still reports
        # COMPLETED, so read the logs rather than the state to tell the two apart.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=COMPLAINT,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            print("Não há novas atualizações na fonte original")
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=COMPLAINT,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Build EVERY table before testing ANY of them, in both environments.
        # complaint's custom_dictionary_coverage test reads dicionario via ref();
        # interleaved run/test per table, that test would run before dicionario
        # exists and fail with "Not found: Table ... us_cfpb_complaints.dicionario".
        # A re-run hides it, because a stale sibling from an earlier build survives
        # — so it only bites in a clean environment, which is exactly prod.
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
        # Covers both early returns (no new data, dev-only) and any exception. The
        # snapshot is ~1.4 GB zipped and 9.3 GB unzipped, plus ~1.4 GB of parquet.
        shutil.rmtree(work_dir, ignore_errors=True)


# The CFPB refreshes the export daily, early UTC. Poll once a day at 07:45 BRT; the
# source-poll guard no-ops when nothing new has landed. The minute is chosen, not
# defaulted: hour 7 already holds 23 (daily), 37 and 51 (monthly), so 45 keeps at
# least five minutes' spacing from each. Defaulting to :00 piles every pipeline onto
# the same instant, where they compete for BigQuery slots and trip the daily quota
# together.
# pyrefly: ignore [missing-attribute]
us_cfpb_complaints_flow.deploy_schedules = [
    {"cron": "45 7 * * *", "timezone": "America/Sao_Paulo"}
]
# The clean step streams the CSV and buffers at most ~800k rows of Python strings,
# but the 9.3 GB unzipped snapshot and ~1.4 GB of parquet share the pod's disk.
# pyrefly: ignore [missing-attribute]
us_cfpb_complaints_flow.job_variables = {"memory": "8Gi"}
