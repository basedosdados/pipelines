"""
Flows for us_ed_nces_ccd — Prefect 3.

NCES Common Core of Data (school and agency directories, school enrollment,
district staff), taken from the Urban Institute Education Data Portal, which
republishes the CCD harmonized across the full 1986-2024 span.

The portal adds one school year per release, roughly 18 to 24 months after the
school year ends, so this is a light annual append: the poll compares the latest
year in the source extract against the coverage already registered, and a run
that finds nothing new returns before downloading anything large.

Only the new year is rebuilt. Every prior year is already materialized and
unchanged, so `dump_mode="append"` adds one Hive partition rather than moving
the whole 20 GB panel.

`district_finance` is not on this schedule. The F-33 stops at the 2020 school
year on the portal and is republished on its own, slower cadence; it is
refreshed by hand when a new year appears.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_ed_nces_ccd_flow`;
the dev pool strips the schedule, the prod pool activates it (paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_ed_nces_ccd.constants import constants
from pipelines.datasets.us_ed_nces_ccd.tasks import (
    clean_ccd,
    download_ccd,
    latest_source_year,
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

# Every refreshed table is annual, so none of them takes the BD Pro rolling
# window: that business rule applies to tables refreshed monthly or more often.
# `dicionario` has no date column and therefore takes no coverage spec at all.
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in constants.REFRESH_TABLES.value
}


@flow(name="us_ed_nces_ccd", log_prints=True)
def us_ed_nces_ccd_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Append the newest CCD school year and materialize the refreshed tables.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage. Has no effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the poll reports no new school year.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="school"
    )

    work_dir = tempfile.mkdtemp(prefix="us_ed_nces_ccd_")
    try:
        input_dir = download_ccd(work_dir=work_dir)
        max_year = latest_source_year(input_dir=input_dir)

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="school",
            source_max_date=f"{max_year}-01-01",
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        # Committed before the heavy work: if the run dies mid-materialization
        # the source metadata still records that a new year was published.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="school",
            source_max_date=f"{max_year}-01-01",
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        paths = clean_ccd(
            work_dir=work_dir, input_dir=input_dir, year=max_year
        )
        tables = constants.ALL_TABLES.value

        for bucket, target, enabled in (
            ("basedosdados-dev", "dev", not materialize_to_prod),
            ("basedosdados", "prod", materialize_to_prod),
        ):
            if not enabled:
                continue

            # Upload and RUN every table first, then TEST every table. The
            # dictionary coverage tests read `ref('..._dicionario')`, a sibling
            # model; interleaved run/test per table, the first table's test
            # fires before that sibling exists and fails on a clean
            # environment.
            for table in tables:
                if table not in paths:
                    continue
                upload_to_gcs(
                    data_path=paths[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name=bucket,
                    dump_mode="append",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target=target,
                )
            for table in tables:
                if table not in paths:
                    continue
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target=target,
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
        # Covers both early returns (no new year, dev-only) and any exception.
        # The k8s work pool gives each run a fresh pod, but a process worker
        # reuses its filesystem, and the download is several gigabytes.
        shutil.rmtree(work_dir, ignore_errors=True)


# The portal refreshes the CCD once a year, in the northern autumn. Poll three
# days a month across September to December; the source-poll guard makes every
# run before the release a cheap no-op. Minute 23 is not used by any other
# deployment in this repo — see the cron inventory in
# `.claude/rules/prefect-pipeline-conventions.md`.
# pyrefly: ignore [missing-attribute]
us_ed_nces_ccd_flow.deploy_schedules = [
    {"cron": "23 5 8,15,22 9,10,11,12 *", "timezone": "America/Sao_Paulo"}
]
# The enrollment extract for a single year is ~900 MB of CSV streamed through
# DuckDB into Parquet; give the worker headroom for the download plus the sort.
# pyrefly: ignore [missing-attribute]
us_ed_nces_ccd_flow.job_variables = {"memory": "8Gi"}
