"""Flows for us_epa_tri — Prefect 3.

EPA Toxics Release Inventory (TRI): annual facility-level reporting of toxic
chemical releases, transfers and waste management. EPA publishes the reporting
year twice — a preliminary dataset in mid-year and the final "National
Analysis" dataset in the autumn — and regenerates the Basic Data Files for
every year each time, because facilities may revise prior-year forms.

The run polls the Basic Data Files page first: its "processed as of" date
moves whenever EPA regenerates the files, and its year dropdown shows the
newest reporting year. Only when the date is newer than our last refresh does
the run download the **two most recent reporting years** (the newest, which
may be preliminary, and the previous one, which may have been finalised or
revised), rewrite those two ``year=`` partitions in staging, and rebuild the
tables with dbt. Older partitions stay as onboarded.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``us_epa_tri_flow``;
the dev pool ignores the schedule, the prod pool activates it (deployed paused).
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_epa_tri.constants import constants
from pipelines.datasets.us_epa_tri.tasks import (
    check_source_tri,
    clean_tri,
    download_tri,
    download_tri_facilities,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
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

# Annual data: every table stays AllFree (the BD Pro rolling window applies to
# tables refreshed monthly or more often). `chemical` and `dicionario` have no
# date column and take no coverage spec.
_ALL_FREE = AllFree(
    date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
)
_COVERAGE = {table: _ALL_FREE for table in constants.YEAR_TABLES.value}

# How many trailing reporting years each run re-downloads and rewrites.
_REFRESH_YEARS = 2


@flow(name="us_epa_tri", log_prints=True)
def us_epa_tri_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh us_epa_tri with the two most recent TRI reporting years.

    Polls the source page and short-circuits when EPA has not regenerated the
    files since our last refresh, unless ``force_run``. On new data, downloads
    the two newest national files, cleans them, overwrites those ``year=``
    partitions in staging (``dump_mode="append"`` keeps the older partitions)
    and rebuilds every table with dbt.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Download and materialize even when the source poll reports
            nothing new.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="form"
    )

    source = check_source_tri()
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id="form",
        source_max_date=source["processed_date"],
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="table_update",
    )
    if not has_new_data and not force_run:
        return

    years = source["years"][-_REFRESH_YEARS:]
    work_dir = tempfile.mkdtemp(prefix="us_epa_tri_")
    try:
        input_dir = download_tri(work_dir=work_dir, years=years)
        fips_path = download_tri_facilities(work_dir=work_dir)
        result = clean_tri(
            work_dir=work_dir,
            input_dir=input_dir,
            facility_fips_path=fips_path,
            years=years,
        )
        print(f"cleaned {years}: {result['counts']}; notes: {result['notes']}")

        tables = constants.TABLES.value

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads.
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

        # Prod: the two refreshed year= partitions overwrite their staging
        # blobs (same paths), older partitions are untouched, dbt rebuilds
        # each table from the whole staging history.
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
            # The source Update is the date EPA last regenerated the files.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id="form",
                source_max_date=source["processed_date"],
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# EPA publishes the preliminary reporting-year dataset in July and the final
# National Analysis dataset in October/November, then regenerates the files a
# few more times as revisions come in. Poll weekly on Wednesdays at 04:07 BRT
# — a minute nobody else uses; the page poll no-ops until the "processed as
# of" date moves.
# pyrefly: ignore [missing-attribute]
us_epa_tri_flow.deploy_schedules = [
    {"cron": "7 4 * * 3", "timezone": "America/Sao_Paulo"}
]
# Two ~60 MB CSVs are read whole into DuckDB and unpivoted; comfortably under
# the default, but give the dbt runs headroom.
# pyrefly: ignore [missing-attribute]
us_epa_tri_flow.job_variables = {"memory": "4Gi"}
