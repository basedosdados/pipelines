"""
Flows for us_bls_oes — Prefect 3.

US Occupational Employment and Wage Statistics (BLS). OEWS publishes once a year,
each release covering a May reference period, and never restates an earlier year.
Each run therefore cleans and appends **only the new year's partition**
(``dump_mode="append"``) rather than rebuilding the panel: the staging object path
carries the partition, so re-running a year overwrites that partition and leaves
every earlier one untouched.

The `dicionario` table is deliberately not refreshed here — its temporal-coverage
column is computed over the whole panel, which a single-year run does not hold.
The clean task instead asserts that the new year introduces no unlabelled code,
and fails the run if it does (see `utils.assert_dictionary_labels`).

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_bls_oes_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.us_bls_oes.constants import constants
from pipelines.datasets.us_bls_oes.tasks import (
    clean_oes,
    download_oes,
    resolve_latest_year,
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
TABLES = constants.DATA_TABLES.value
# The table the source poll is anchored on. OEWS ships both tables in one
# release, so either would do; `area` is the larger and more used of the two.
POLL_TABLE = "area"

# Coverage spec per table. OEWS is annual, so neither table takes a BD Pro
# rolling window — that applies to tables refreshed monthly or more often. Both
# are AllFree with a single free Coverage.
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in TABLES
}


@flow(name="us_bls_oes", log_prints=True)
def us_bls_oes_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Append the newest OEWS release to the `area` and `industry` tables.

    The source poll short-circuits the run when BLS has not published a new
    reference year, which makes a scheduled run a cheap no-op for the eleven
    months of the year when nothing is released.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new year.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="oes"
    )

    year = resolve_latest_year()

    # Skip the run when BLS has not published a newer reference year.
    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=str(year),
        env="prod",
        date_format="%Y",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        return

    work_dir = tempfile.mkdtemp(prefix="us_bls_oes_")
    try:
        input_dir = download_oes(work_dir=work_dir, year=year)
        result = clean_oes(work_dir=work_dir, input_dir=input_dir, year=year)

        # Commit the source Update before materializing: if the run fails
        # midway, the source metadata still records that BLS published a new
        # year, even though our tables have not caught up yet.
        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=str(year),
            env="prod",
            date_format="%Y",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        for table in TABLES:
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
        # Every table is built before any is tested: the dictionary-coverage
        # test reads the sibling `dicionario` model, so interleaving run and
        # test per table fails in a clean environment.
        for table in TABLES:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        for table in TABLES:
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
        # Covers both the dev-only early return and any exception. The k8s work
        # pool gives each run a fresh pod, but a process worker reuses its
        # filesystem, and one release is ~80 MB compressed.
        shutil.rmtree(work_dir, ignore_errors=True)


# OEWS releases once a year, in the northern spring, with the exact date varying
# between late March and May. Poll weekly across those three months; the
# source-poll guard no-ops until a new reference year actually appears.
# pyrefly: ignore [missing-attribute]
us_bls_oes_flow.deploy_schedules = [
    {"cron": "47 17 1,8,15,22,29 3,4,5 *", "timezone": "America/Sao_Paulo"}
]
# One release is ~430k rows held in pandas plus the Excel reader's own buffers.
# pyrefly: ignore [missing-attribute]
us_bls_oes_flow.job_variables = {"memory": "8Gi"}
