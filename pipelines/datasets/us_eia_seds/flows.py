"""Flows for us_eia_seds — Prefect 3.

U.S. Energy Information Administration State Energy Data System (SEDS): the
complete state-by-state energy consumption, price and expenditure series across
all fuels, 1960 to the latest complete year.

**SEDS restates its whole series on each annual release** rather than extending
it, so each run rebuilds every year partition from the newest Complete_SEDS.csv.
Rebuilding everything makes a double-count structurally impossible and keeps the
dicionario computed over the whole record. The dataset is annual, so every table
is fully free — no BD Pro window.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers `us_eia_seds_flow`; the
dev pool ignores the schedule, the prod pool activates it.
"""

import shutil
import tempfile

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.datasets.us_eia_seds.constants import constants
from pipelines.datasets.us_eia_seds.tasks import clean_corpus, probe_source
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
POLL_TABLE = constants.SEDS_CONSUMPTION.value

# Annual dataset -> the single data table is fully free. dicionario has no date
# column and takes no coverage spec.
_COVERAGE = {
    constants.SEDS_CONSUMPTION.value: AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}


@flow(name="us_eia_seds", log_prints=True)
def us_eia_seds_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh SEDS from the published Complete_SEDS.csv.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. False
            exercises only the dev half — required for a safe test run.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source update. No effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports nothing new —
            needed for a release that restates history without adding a year.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
        )
    )

    work_dir = tempfile.mkdtemp(prefix="us_eia_seds_")
    try:
        probe = probe_source(work_dir=work_dir)
        print(f"SEDS max coverage date {probe['max_date']}")

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=probe["max_date"],
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data and not force_run:
            print("Não há novas atualizações na fonte original")
            return

        result = clean_corpus(work_dir=work_dir)
        print(f"row counts: {result['row_counts']}")

        commit_source_update_task(
            dataset_id=DATASET_ID,
            table_id=POLL_TABLE,
            source_max_date=probe["max_date"],
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_to_prod,
        )

        tables = constants.ALL_TABLES.value

        # Build every table before testing any (custom_dictionary_coverage reads
        # dicionario via ref()); interleaved, the first table's test runs before
        # dicionario exists and fails in a clean environment.
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
        shutil.rmtree(work_dir, ignore_errors=True)


# SEDS publishes once a year in late June. Poll across a window of days so the
# source-poll guard no-ops until the new vintage lands; :23 keeps clear of other
# crons in this repo.
# pyrefly: ignore [missing-attribute]
us_eia_seds_flow.deploy_schedules = [
    {"cron": "23 9 * * *", "timezone": "America/Sao_Paulo"}
]
# The clean step reads one ~90 MB CSV into pandas and writes 65 partitions; 8Gi
# is comfortable. `memory` alone is dropped by the work pool — the pod honours
# `memory_limit`.
# pyrefly: ignore [missing-attribute]
us_eia_seds_flow.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
