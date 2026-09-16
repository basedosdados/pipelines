"""
Flows for us_census_lodes — Prefect 3.

LODES publishes one new data year per release, roughly two years in arrears
(the December 2025 release added 2023), and is static between releases. A run
therefore rebuilds only the data years the backend does not yet cover, plus the
geography crosswalk, which LODES restates wholesale at every release.

`dump_mode="append"` with deterministic blob names is what makes a re-run safe:
each state-year lands at `year=<YYYY>/<st>.parquet`, so re-processing a year
overwrites its own blobs rather than duplicating rows. `overwrite` is not used —
it drops the prod table even from a dev run.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_census_lodes_flow`;
the dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_census_lodes.constants import DATASET_ID, YEARS
from pipelines.datasets.us_census_lodes.tasks import (
    build_years,
    get_latest_year,
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

# Order matters only for readability; the run/test split below is what keeps
# cross-table tests valid.
TABLES = [
    "residence_jobs",
    "workplace_jobs",
    "geography_crosswalk",
    "dicionario",
]

# Annual cadence, so the BD Pro rolling window does not apply -- that is for
# tables refreshed monthly or more often. Every table here is fully free.
# `geography_crosswalk` and `dicionario` have no date column and take no spec.
_COVERAGE = {
    "residence_jobs": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "workplace_jobs": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}

# The table the source poll is anchored on.
POLL_TABLE = "residence_jobs"


@flow(name="us_census_lodes", log_prints=True)
def us_census_lodes_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
    rebuild_years: str = "",
) -> None:
    """Ingest any newly published LODES data year and materialize the tables.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when LODES has published no new data year.
        rebuild_years: Comma-separated years to rebuild instead of the newly
            published one, e.g. ``"2021,2022"``. Use after a source correction:
            LODES re-releases individual files when they change, and the poll
            only notices a new *year*. Empty means "whatever is new".
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="us_census_lodes_")
    try:
        latest = get_latest_year()

        # Is the newest published year already covered? Comparing against the
        # registered coverage, not a wall clock, is what makes a scheduled run
        # between releases a cheap no-op.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=str(latest),
            env="prod",
            date_format="%Y",
            compare_against="coverage",
        )
        if not has_new_data and not force_run and not rebuild_years:
            return

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=str(latest),
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        if rebuild_years:
            years = [int(y) for y in rebuild_years.split(",")]
        else:
            # Everything the published range covers beyond the onboarded
            # history. Normally one year; more if a release was missed.
            years = [y for y in range(YEARS[-1] + 1, latest + 1)] or [latest]
        print(f"rebuilding data years: {years}")

        result = build_years(work_dir=work_dir, years=years)

        if not materialize_to_prod:
            for table in TABLES:
                if table not in result:
                    continue
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
            # Test only after every table is built: the referential tests read
            # sibling models, so interleaving run and test fails on a clean
            # environment where the sibling does not exist yet.
            for table in TABLES:
                if table not in result:
                    continue
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        for table in TABLES:
            if table not in result:
                continue
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
        for table in TABLES:
            if table not in result:
                continue
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
        # Covers both early returns (no new year, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# LODES releases annually in the last quarter (8.4 in Dec 2025, 8.3 in Nov 2024,
# 8.1 in Nov 2023). Poll weekly across October-January at 16:26 BRT; the source
# poll no-ops until a new data year actually appears.
# pyrefly: ignore [missing-attribute]
us_census_lodes_flow.deploy_schedules = [
    {"cron": "26 16 1,8,15,22 1,10,11,12 *", "timezone": "America/Sao_Paulo"}
]
# One data year across 51 states is ~600MB of gzipped CSV, cleaned a state at a
# time; the crosswalk rebuild holds ~8M rows in pandas across the run.
# pyrefly: ignore [missing-attribute]
us_census_lodes_flow.job_variables = {"memory": "8Gi"}
