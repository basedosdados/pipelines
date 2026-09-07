"""Flows for us_fema_openfema — Prefect 3.

The four OpenFEMA sets refresh on their own cadences: the disaster
declarations every 20 minutes, the Public Assistance projects daily, and the
two NFIP files monthly. Each carries a `lastDataSetRefresh` timestamp in the
catalog, so the run polls that per set and re-materialises only the sets whose
timestamp has moved. On a typical day that means the two daily sets and
neither NFIP file; on the day FEMA cuts a new NFIP extract it means all four.

Every source is a single full-history file, so a refresh rewrites all of that
table's `year=` partitions. The staging blobs are the same object paths, so
`dump_mode="append"` overwrites them in place — `"overwrite"` would drop the
prod table even from a dev run.

The dictionary is static, generated from the source's own field dictionary and
committed to the repo. It is rewritten and rebuilt on every real run: it costs
nothing and it means a clean environment gets a complete dataset rather than
four tables and a missing legend.

Coverage is `AllFree` on every table. The house rule would ordinarily put a
table refreshed monthly or more often behind the BD Pro rolling window, but
OpenFEMA's terms restrict use to "statistical research or as a reporting
record" and forbid using the data to make determinations affecting an
individual's rights or eligibility, so paywalling it is a commercial decision
to be taken deliberately rather than a default. See the dataset's memory note.

Deploy: `.github/scripts/deploy_flows.py` auto-discovers ``us_fema_openfema_flow``;
the dev pool ignores the schedule, the prod pool activates it (deployed paused).
"""

import shutil
import tempfile

from prefect import flow  # pyrefly: ignore [missing-attribute]

from pipelines.datasets.us_fema_openfema.constants import constants
from pipelines.datasets.us_fema_openfema.tasks import (
    check_source_openfema,
    clean_openfema,
    download_openfema,
    write_dicionario_task,
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

_ALL_FREE = AllFree(
    date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
)
_COVERAGE = {table: _ALL_FREE for table in constants.SOURCES.value}


@flow(name="us_fema_openfema", log_prints=True)
def us_fema_openfema_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Refresh the OpenFEMA sets whose published data has moved.

    Args:
        materialize_to_prod: Continue past the dev materialization to write the
            prod staging bucket and run dbt against ``target="prod"``. Set False
            to exercise only the dev half — required for a safe test run, since
            the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage and commit the source updates. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Download and materialize every set even when the poll
            reports nothing new.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="nfip_claim"
    )

    refreshed_at = check_source_openfema()

    stale: list[str] = []
    for table in constants.SOURCES.value:
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=table,
            source_max_date=refreshed_at[table],
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if has_new_data or force_run:
            stale.append(table)

    if not stale:
        print("no OpenFEMA set has been republished since the last refresh")
        return
    print(f"refreshing: {stale}")

    work_dir = tempfile.mkdtemp(prefix="us_fema_openfema_")
    try:
        paths = {"dicionario": write_dicionario_task(work_dir=work_dir)}
        for table in stale:
            raw = download_openfema(work_dir=work_dir, table=table)
            paths[table] = clean_openfema(
                work_dir=work_dir, table=table, input_path=raw
            )

        bucket = (
            "basedosdados-dev" if not materialize_to_prod else "basedosdados"
        )
        target = "dev" if not materialize_to_prod else "prod"

        # Build every refreshed table first, then test them all. Interleaving
        # run and test would run a table's tests before a sibling it references
        # exists, which only bites in a clean environment.
        for table, path in paths.items():
            upload_to_gcs(
                data_path=path,
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
        for table in paths:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )

        if not materialize_to_prod or not update_metadata:
            return

        for table in stale:
            register_table_materialization_task(
                dataset_id=DATASET_ID,
                table_id=table,
                coverage=_COVERAGE[table],
                env="prod",
                bq_project="basedosdados",
            )
            # Written last, and only after prod succeeded: this is what the
            # next run's poll compares against.
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=table,
                source_max_date=refreshed_at[table],
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Daily at 05:13 BRT — a free minute. The declarations and Public Assistance
# sets move most days; the two NFIP files are monthly, so their poll no-ops
# until FEMA cuts a new extract.
# pyrefly: ignore [missing-attribute]
us_fema_openfema_flow.deploy_schedules = [
    {"cron": "13 5 * * *", "timezone": "America/Sao_Paulo"}
]
# The NFIP policy file is a 3.7 GB parquet re-partitioned into 19 year files.
# The clean streams by record batch, so peak memory is a batch rather than the
# file, but the download and the dbt rebuild want headroom.
# pyrefly: ignore [missing-attribute]
us_fema_openfema_flow.job_variables = {"memory": "8Gi"}
