"""
Flows for au_ato_taxation_statistics — Prefect 3.

ATO Taxation Statistics (data.gov.au). The ATO reissues the whole collection
once a year and revises earlier years in place, so each run refetches every
in-scope release and does a **full replace** (``dump_mode="overwrite"``)
rather than appending the newest year.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers
`au_ato_taxation_statistics_flow`; the dev pool ignores the schedule, the prod
pool activates it.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.au_ato_taxation_statistics.constants import constants
from pipelines.datasets.au_ato_taxation_statistics.tasks import (
    clean_taxstats,
    download_taxstats,
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
POLL_TABLE = constants.POLL_TABLE.value

# Coverage spec per table. This dataset releases annually, and the BD Pro
# rolling window applies only to tables refreshed monthly or faster, so every
# table stays fully free — no pro Coverage and no Row Access Policies.
# `dicionario` has no date column and therefore takes no spec at all.
_COVERAGE = {
    table: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    )
    for table in constants.TABLE_SELECTORS.value
}


@flow(name="au_ato_taxation_statistics", log_prints=True)
def au_ato_taxation_statistics_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Rebuild every ATO Taxation Statistics table from the source collection.

    The source poll short-circuits the run until the ATO publishes a new
    financial year, which makes a scheduled run a cheap no-op in between.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new release.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="au_ato_taxation_statistics_")
    try:
        downloaded = download_taxstats(work_dir=work_dir)
        max_year = downloaded["max_year"]

        # Poll against coverage, not Table.Update: max_year is the newest
        # release's data year, and the polled table's coverage end year equals
        # it, so the comparison is competência against competência.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=max_year,
            env="prod",
            date_format="%Y",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            return

        result = clean_taxstats(
            work_dir=work_dir, input_dir=downloaded["input_dir"]
        )
        tables = constants.ALL_TABLES.value

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run doubled the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_to_prod:
            # Dev: upload every table, run every model, and only then test — the
            # fact tables' dictionary-coverage tests reference the dicionario
            # model, which must already exist when they run.
            for table in tables:
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
            for table in tables:
                run_dbt(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    dbt_command="test",
                    target="dev",
                )
            return

        # Prod: same two-pass shape.
        for table in tables:
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
            # Last, and only after prod succeeded: a mid-flow failure must not
            # advance the source watermark past data we did not materialize.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=POLL_TABLE,
                source_max_date=max_year,
                env="prod",
                date_format="%Y",
            )
    finally:
        # Covers both early returns (no new release, dev-only) and exceptions.
        # A process worker reuses its filesystem and the download is ~92 MB.
        shutil.rmtree(work_dir, ignore_errors=True)


# The ATO publishes Taxation Statistics once a year, historically around
# April-June. Poll monthly at 16:00 BRT on the 20th; the source-poll guard
# no-ops until a new financial year actually appears.
# pyrefly: ignore [missing-attribute]
au_ato_taxation_statistics_flow.deploy_schedules = [
    {"cron": "0 16 20 * *", "timezone": "America/Sao_Paulo"}
]
# The clean step holds ~4.5M rows in pandas before writing partitions.
# pyrefly: ignore [missing-attribute]
au_ato_taxation_statistics_flow.job_variables = {"memory": "8Gi"}
