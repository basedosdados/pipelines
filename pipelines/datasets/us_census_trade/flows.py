"""
Flows for us_census_trade — Prefect 3.

U.S. Census Bureau monthly merchandise trade at HS6 x partner country x place,
on three place dimensions (customs district, port, state), from the
International Trade timeseries API.

Unlike ``us_bls_cpi``, the source does NOT ship the full history each release,
so this is an incremental **partition refresh**, not a full replace. Each run
re-downloads January of the previous year through the newest published month
and rewrites only those ``year=`` partitions, with ``dump_mode="append"``:
``upload_to_gcs`` replaces objects by path, and one file per year means the
refreshed years are replaced exactly and the earlier ones are left alone.
``dump_mode="overwrite"`` would delete the whole table and rebuild it from the
window, erasing every year before it.

That window is sized to the source's revision policy: Census revises
year-to-date months at every release AND revises all previously released data
with the publication of April statistics.

Every request to api.census.gov requires a key. Provision ``CENSUS_API_KEY`` in
Vault at secret path ``us_census_trade`` before triggering a run — without it
the download raises immediately.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_census_trade_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_census_trade.constants import constants
from pipelines.datasets.us_census_trade.tasks import (
    build_dicionario_task,
    download_schedules_task,
    harvest_task,
    latest_available_month_task,
)
from pipelines.datasets.us_census_trade.utils import (
    FACT_TABLES,
    refresh_window,
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

# The table whose coverage the source poll is compared against, and whose
# raw data source carries the Update record.
POLL_TABLE = "import"

# Coverage spec per fact table.
#
# All six refresh monthly, so each carries the BD Pro rolling window under the
# house rule: the most recent 6 months are pro-only, everything older is free.
# Each run recomputes free_end = source_end - free_lag, rewrites both
# DateTimeRanges and re-issues the BigQuery Row Access Policies, so the window
# slides forward on its own and the dbt models never mention it.
#
# part_bdpro requires BOTH a free (is_closed=False) and a pro (is_closed=True)
# Coverage to already exist on the table, or assert_coverage_topology raises
# before anything is written. Create the pro Coverage at onboarding.
#
# `dicionario` has no date column, so it takes no coverage spec at all.
_COVERAGE = {
    table: PartBdpro(
        date_column=YearMonth(year="year", month="month"),
        date_format=DateFormat.YEAR_MONTH,
        free_lag=FreeLag(unit="months", value=6),
    )
    for table in FACT_TABLES
}

ALL_TABLES = [*FACT_TABLES, "dicionario"]


@flow(name="us_census_trade", log_prints=True)
def us_census_trade_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    tables: list[str] | None = None,
    first_month: str | None = None,
    last_month: str | None = None,
) -> None:
    """Refresh the revised window of U.S. Census trade statistics.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new month.
        tables: Subset of tables to build. Defaults to all seven. A full
            backfill of every table over every year is a long single run; this
            lets it be sliced by table.
        first_month: Override the start of the refresh window, ``YYYY-MM``.
            Set this to ``2010-01`` for the initial backfill. Defaults to
            January of the previous year, which covers both the year-to-date
            revisions and the annual April revision.
        last_month: Override the end of the window, ``YYYY-MM``. Defaults to
            the newest month the API has published.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="trade"
    )

    selected = list(tables) if tables else list(ALL_TABLES)
    fact_selected = [t for t in selected if t in FACT_TABLES]

    work_dir = tempfile.mkdtemp(prefix="us_census_trade_")
    try:
        schedule_c, schedule_d = download_schedules_task()
        latest = last_month or latest_available_month_task(
            probe_table=POLL_TABLE
        )

        # Skip the run when Census has not published a newer month. Compared
        # against the table's registered coverage, at month granularity, so the
        # comparison is like-for-like: a year-granular date against a
        # month-granular coverage silently degrades this to an annual pipeline.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=latest,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        start = first_month or refresh_window(latest)
        print(f"refreshing {start}..{latest} for {selected}")

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=latest,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        paths: dict[str, str] = {}
        if fact_selected:
            paths.update(
                harvest_task(
                    tables=fact_selected,
                    first_month=start,
                    last_month=latest,
                    output_dir=work_dir,
                    schedule_c=schedule_c,
                )
            )
        if "dicionario" in selected:
            paths["dicionario"] = build_dicionario_task(
                schedule_c=schedule_c,
                schedule_d=schedule_d,
                output_dir=work_dir,
            )

        for target, bucket in (
            ("dev", "basedosdados-dev"),
            ("prod", "basedosdados"),
        ):
            if target == "prod" and not materialize_to_prod:
                return

            # Upload and RUN every table first, then TEST every table. The
            # dicionario coverage tests read sibling models, so interleaving
            # run and test per table would test a model before the sibling it
            # references exists — silent in a re-run that still has a stale
            # sibling, fatal in a clean environment.
            for table in selected:
                upload_to_gcs(
                    data_path=paths[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name=bucket,
                    # append, never overwrite: this is a partition refresh.
                    # overwrite drops the whole table and would leave only the
                    # refreshed window behind.
                    dump_mode="append",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target=target,
                )
            for table in selected:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target=target,
                )

        if update_metadata:
            for table in fact_selected:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=_COVERAGE[table],
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns (no new data, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process worker
        # reuses its filesystem — a refresh window is several GB of parquet.
        shutil.rmtree(work_dir, ignore_errors=True)


# Census publishes a reference month roughly five weeks later, on a scheduled
# release day early in the month. Poll across the first week at 16:20 BRT — a
# minute nobody else in this repo uses, so this does not pile onto another
# pipeline's BigQuery slots. The source-poll guard no-ops until a new month
# actually lands.
# pyrefly: ignore [missing-attribute]
us_census_trade_flow.deploy_schedules = [
    {"cron": "20 16 4,5,6,7,8,9 * *", "timezone": "America/Sao_Paulo"}
]
# The harvest holds one month of one table in pandas at a time, but a single
# month of HS6 x country x district is millions of rows.
#
# `memory` ALONE IS SILENTLY IGNORED: it is not a variable of the work pool's
# job template, so a flow that sets only `memory` runs on the 4Gi default no
# matter what number it names, and is later OOMKilled with no hint of why.
# `memory_limit` is the one the pod actually gets.
# pyrefly: ignore [missing-attribute]
us_census_trade_flow.job_variables = {
    "memory_limit": "16Gi",
    "memory_request": "4Gi",
}
