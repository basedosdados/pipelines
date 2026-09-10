"""
Flows for au_abs_population — Prefect 3.

ABS population estimates and projections, from three products on three
cadences: the national and state series (former catalogue 3101.0) quarterly,
the regional series (3218.0) annually, and the projections (3222.0) every few
years. Every release ships the full history, so each run is a **full replace**
(``dump_mode="overwrite"``) rather than an incremental append.

One flow rebuilds all six tables, because the three products share a parser and
the whole transform runs in seconds; the cost of a run is dominated by the
download. The run short-circuits unless one of the two polled sources has
advanced, which makes a scheduled run a cheap no-op between releases.

Deploy: ``.github/scripts/deploy_flows.py`` auto-discovers
``au_abs_population_flow``; the dev pool ignores the schedule, the prod pool
activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_abs_population.constants import constants
from pipelines.datasets.au_abs_population.tasks import (
    clean_population,
    download_population,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearOnly,
    YearQuarter,
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

# Build order: `series` first, because every other table's series_id
# relationship test references it, and dbt tests read sibling models.
TABLES = [
    "series",
    "national_state",
    "erp_age_sex",
    "projection",
    "regional_sa2",
    "regional_lga",
]

# Coverage spec per table. Every table is fully free: the fastest-refreshing
# source here is quarterly, and the BD Pro rolling window applies to tables that
# refresh monthly or more often. `series` is a dimension table with no date
# column, so it takes no coverage spec.
_COVERAGE = {
    "national_state": AllFree(
        date_column=YearQuarter(year="year", quarter="quarter"),
        date_format=DateFormat.YEAR_MONTH,
    ),
    "erp_age_sex": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "projection": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "regional_sa2": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
    "regional_lga": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="au_abs_population", log_prints=True)
def au_abs_population_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild every au_abs_population table from the current ABS releases.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source updates. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when neither source poll reports new data.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="population"
    )

    work_dir = tempfile.mkdtemp(prefix="au_abs_population_")
    try:
        downloaded = download_population(work_dir=work_dir)
        result = clean_population(
            work_dir=work_dir, input_dir=downloaded["input_dir"]
        )
        max_year_quarter = result["max_year_quarter"]
        max_year = str(result["max_year"])

        # Two sources on two cadences, so two polls: the run proceeds when
        # EITHER has advanced. Polling only the quarterly source would leave an
        # annual regional release unpublished until the next quarter landed.
        # Each poll's date_format matches its table's coverage granularity —
        # comparing a year against a month-granular coverage silently never
        # fires.
        quarterly_new = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="national_state",
            source_max_date=max_year_quarter,
            env="prod",
            date_format="%Y-%m",
            compare_against="coverage",
        )
        regional_new = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id="regional_sa2",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            compare_against="coverage",
        )
        if not (quarterly_new or regional_new) and not force_run:
            return

        # Commit the source Updates before materializing: if the flow fails
        # midway the source metadata still records that new data was published.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="national_state",
            source_max_date=max_year_quarter,
            env="prod",
            date_format="%Y-%m",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id="regional_sa2",
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        # Run every table, then test every table. The series_id relationship
        # tests read a sibling model, so an interleaved run/test would test a
        # table before the model it references exists.
        if not materialize_to_prod:
            for table in TABLES:
                upload_to_gcs(
                    data_path=result[table],
                    dataset_id=DATASET_ID,
                    table_id=table,
                    bucket_name="basedosdados-dev",
                    dump_mode="overwrite",
                    source_format="parquet",
                )
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="run",
                    target="dev",
                )
            for table in TABLES:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        for table in TABLES:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target="prod",
            )
        for table in TABLES:
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
        # Covers early returns (no new data, dev-only) and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# ABS publishes the quarterly national and state population about six months
# after the reference quarter, in the second half of the month; the regional
# release lands once a year in March. Poll on a few days around the middle of
# each month at 16:48 BRT (a minute no other flow uses) — the two source-poll
# guards no-op the run until one of them actually has new data.
# pyrefly: ignore [missing-attribute]
au_abs_population_flow.deploy_schedules = [
    {"cron": "48 16 14,17,20,23,26 * *", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
au_abs_population_flow.job_variables = {
    "memory": "8Gi",
    # `memory` alone is not in the work pool's job template and is silently
    # dropped; `memory_limit` is the key the pod actually gets.
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
