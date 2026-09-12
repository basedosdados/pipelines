"""
Flows for us_bls_employment — Prefect 3.

Three BLS employment programs: Current Employment Statistics national (`ce`) and
state/metro (`sm`), Local Area Unemployment Statistics (`la`), and JOLTS (`jt`).

**Every run is a full replace, not an append.** BLS revises the trailing months
of each program with every release and restates the whole back series at the
annual benchmark — CES against the QCEW employment count, LAUS against new
population controls. A pipeline that appended the new month and left history
alone would diverge from the published series within a year and never converge
back. The flat files carry the full history on every release, so rebuilding from
them (``dump_mode="overwrite"``) absorbs the trailing revisions and the annual
benchmark alike, with no separate benchmark branch to maintain.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_bls_employment_flow`;
the dev pool strips the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_bls_employment.constants import constants
from pipelines.datasets.us_bls_employment.tasks import (
    clean_employment,
    download_employment,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
    YearMonth,
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

# The table the source poll is anchored to. CES national is released first each
# month, so it is the earliest signal that BLS has published a new reference
# period; a full replace rebuilds every table regardless of which one moved.
POLL_TABLE = "ces_national"

# Coverage spec per table.
#
# All four fact tables refresh monthly, so each carries the BD Pro rolling
# window: the most recent 6 months are pro-only, everything older is free. Each
# run recomputes free_end = source_end - free_lag, rewrites both DateTimeRanges,
# and re-issues the BigQuery Row Access Policies, so the window slides forward
# on its own and the dbt models stay untouched.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written.
#
# `dicionario` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    table: PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    )
    for table in constants.DATA_TABLES.value
}


@flow(name="us_bls_employment", log_prints=True)
def us_bls_employment_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the BLS employment flat files, rebuild every table, materialize.

    The source poll short-circuits the run when BLS has not published a new
    reference month, which makes a scheduled run a cheap no-op between releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="employment"
    )

    work_dir = tempfile.mkdtemp(prefix="us_bls_employment_")
    try:
        input_dir = download_employment(work_dir=work_dir)
        result = clean_employment(work_dir=work_dir, input_dir=input_dir)
        max_ym = result["max_year_month"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Commit the source Update before materializing: if the flow fails
        # midway, the source metadata still records that BLS had published a
        # newer month, even though the tables were not refreshed.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_ym,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value
        env, bucket = (
            ("prod", "basedosdados")
            if materialize_to_prod
            else ("dev", "basedosdados-dev")
        )

        # Upload and build every table first, then test every table. The tests
        # read sibling models — the dictionary-coverage and directory
        # relationship checks — so interleaving run and test per table would
        # test a model before the model it references exists, which only shows
        # up in a clean environment.
        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket,
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target=env,
            )
        for table in tables:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=env,
            )

        if materialize_to_prod and update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns (no new data) and any exception. The k8s
        # work pool gives each run a fresh pod, but a process worker reuses its
        # filesystem — the download is roughly 2.6 GB.
        shutil.rmtree(work_dir, ignore_errors=True)


# BLS releases the Employment Situation (CES + LAUS national) on the first
# Friday, state LAUS mid-month, SAE and JOLTS later in the month. Poll daily
# across the second half of the month at 16:20 BRT; the source-poll guard
# no-ops until a new reference month actually appears. The minute is chosen to
# avoid the crowded :00 and :05 slots.
# pyrefly: ignore [missing-attribute]
us_bls_employment_flow.deploy_schedules = [
    {"cron": "20 16 16,17,18,19,20,21 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step shards 2.6 GB of flat files through pandas one file at a time
# and holds at most one year of LAUS (~500k rows) in memory at once.
# `memory` alone is NOT a variable of the work pool's job template and is
# dropped silently, leaving the pod on the 4Gi default — `memory_limit` is the
# one that is actually applied.
# pyrefly: ignore [missing-attribute]
us_bls_employment_flow.job_variables = {
    "memory": "12Gi",
    "memory_limit": "12Gi",
    "memory_request": "4Gi",
}
