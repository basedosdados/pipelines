"""Flows for us_fbi_cde — Prefect 3.

FBI Crime Data Explorer. The FBI publishes one data year each autumn and revises
the two preceding years in the same release, so the refresh window is the new
year plus those two. Every table is partitioned by year and every year's content
depends only on that year's sources, which makes a windowed refresh complete for
the years it touches.

``dump_mode`` is ``"append"`` everywhere, never ``"overwrite"``: overwrite calls
``tb.delete(mode="all")``, which drops the materialised production table even
from a dev-only run. With append, each partition blob is replaced wholesale,
which is the semantics wanted here and touches nothing outside the window.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers ``us_fbi_cde_flow``;
the dev pool strips the schedule, the prod pool activates it paused.
"""

from __future__ import annotations

import tempfile

from prefect import flow

from pipelines.datasets.us_fbi_cde.constants import constants
from pipelines.datasets.us_fbi_cde.tasks import (
    clean_window,
    cleanup,
    discover_latest_year,
    download_window,
)
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

# The table the source poll is anchored on. It is the one whose coverage moves
# with every release and the one linked to the NIBRS raw data source.
POLL_TABLE = "incident"

# Every table refreshes at most once a year, so none of them falls under the BD
# Pro rolling window, which applies to tables refreshed monthly or more often.
# `dicionario` has no date column and therefore takes no coverage spec.
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in [
        "agency",
        "incident",
        "offense",
        "offender",
        "victim",
        "victim_offense",
        "victim_offender_relationship",
        "arrestee",
        "property",
        "hate_crime",
        "ucr_summary",
    ]
}

ALL_TABLES = [*list(_COVERAGE), "dicionario"]


@flow(name="us_fbi_cde", log_prints=True)
def us_fbi_cde_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    years_back: int = 2,
) -> None:
    """Refresh the FBI CDE tables for the newest data year and its revisions.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new year.
        years_back: How many prior data years to refresh alongside the newest
            one. The FBI revises the two preceding years in each release.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_fbi_cde_")
    try:
        latest_year = discover_latest_year()
        # The release covers a calendar year, so its coverage date is that
        # year's December, not the day we happened to look.
        source_max_date = f"{latest_year}-12-01"

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new_data and not force_run:
            print(f"no new data year at the source; latest is {latest_year}")
            return

        downloads = download_window(
            work_dir=work_dir, latest_year=latest_year, years_back=years_back
        )
        paths = clean_window(work_dir=work_dir, downloads=downloads)

        # Every table is built and only then tested, in both environments.
        # The dictionary-coverage and directory tests read sibling models, so
        # interleaving run and test per table fails in a clean environment where
        # the sibling does not exist yet.
        if not materialize_to_prod:
            for table in ALL_TABLES:
                if table not in paths:
                    continue
                upload_to_gcs(
                    data_path=paths[table],
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
            for table in ALL_TABLES:
                if table not in paths:
                    continue
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        for table in ALL_TABLES:
            if table not in paths:
                continue
            upload_to_gcs(
                data_path=paths[table],
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
        for table in ALL_TABLES:
            if table not in paths:
                continue
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="prod",
            )

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                if table not in paths:
                    continue
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
            # Last, and only after prod succeeded.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=POLL_TABLE,
                source_max_date=source_max_date,
                env="prod",
                date_format="%Y-%m-%d",
            )
    finally:
        cleanup(work_dir=work_dir)


# The FBI publishes the new data year in the autumn, historically between mid
# September and early October, and occasionally later. Poll every few days
# across September to November; the source-poll guard makes every run before the
# release a cheap no-op.
# pyrefly: ignore [missing-attribute]
us_fbi_cde_flow.deploy_schedules = [
    {
        "cron": "23 15 2,6,10,14,18,22,26,30 9,10,11 *",
        "timezone": "America/Sao_Paulo",
    }
]
# The clean step holds one state-year bundle in pandas at a time; Texas is the
# largest at roughly 130 MB compressed.
# pyrefly: ignore [missing-attribute]
us_fbi_cde_flow.job_variables = {"memory": "16Gi"}
