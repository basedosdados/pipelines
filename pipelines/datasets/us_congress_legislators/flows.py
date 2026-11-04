"""
Flows for us_congress_legislators — Prefect 3.

unitedstates/congress-legislators republishes its full set of YAML files on
every commit, so each run is a **full replace** (dump_mode="overwrite") of all
eight us_congress_legislators tables plus the br_bd_diretorios_us.congress_member
directory, which is built from the same files.

The source has no release calendar: a commit is its only publication signal. The
poll compares the date of the repo's latest commit against the table's last
refresh (``Table.Update.latest``), so a weekly run is a cheap no-op until
upstream changes. A commit landing later on the same day as a refresh is picked
up by the next commit, not by the next run.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers
`us_congress_legislators_flow`; the dev pool ignores the schedule, the prod pool
activates it.
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.us_congress_legislators.constants import constants
from pipelines.datasets.us_congress_legislators.tasks import (
    clean_legislators,
    download_legislators,
    get_source_date,
)
from pipelines.utils.flow import flow
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
POLL_TABLE_ID = constants.POLL_TABLE_ID.value


@flow(name="us_congress_legislators", log_prints=True)
def us_congress_legislators_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the congress-legislators YAML, rebuild all nine tables, materialize.

    Every table is a current snapshot with no date column to measure coverage
    by, so each is registered as ``NonHistorical``: the run only moves
    ``Table.Update.latest`` and leaves the existing coverage ranges alone. All
    tables are free; none is high-frequency time series.

    Args:
        materialize_to_prod: Write the prod staging bucket and run dbt against
            ``target="prod"``. Set False to rebuild only in dev — required for a
            safe test run, since the default writes production.
        update_metadata: After a successful prod materialization, commit the
            source update and register each table's refresh. Has no effect when
            ``materialize_to_prod`` is False.
        force_run: Materialize even when the source has no commit newer than
            the last refresh.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=POLL_TABLE_ID
    )

    source_date = get_source_date()

    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE_ID,
        source_max_date=source_date,
        env="prod",
        date_format="%Y-%m-%d",
        compare_against="table_update",
    )
    if not has_new_data and not force_run:
        return

    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE_ID,
        source_max_date=source_date,
        env="prod",
        date_format="%Y-%m-%d",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_to_prod,
    )

    work_dir = tempfile.mkdtemp(prefix="us_congress_legislators_")
    try:
        input_dir = download_legislators(work_dir=work_dir)
        outputs = clean_legislators(work_dir=work_dir, input_dir=input_dir)
        tables = [
            (key.split(".")[0], key.split(".")[1], path)
            for key, path in outputs.items()
        ]

        bucket_name = (
            "basedosdados" if materialize_to_prod else "basedosdados-dev"
        )
        target = "prod" if materialize_to_prod else "dev"

        # Run every model before testing any: legislator carries a
        # relationships test against congress_member, and the other tables
        # against legislator.
        for dataset_id, table_id, path in tables:
            upload_to_gcs(
                data_path=path,
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name=bucket_name,
                dump_mode="overwrite",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run",
                target=target,
            )
        for dataset_id, table_id, _path in tables:
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="test",
                target=target,
            )

        if not (materialize_to_prod and update_metadata):
            return

        for dataset_id, table_id, _path in tables:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Weekly, Wednesday 06:41 BRT. Congress gains or loses members and committee
# seats at irregular times; the poll no-ops until upstream commits.
us_congress_legislators_flow.deploy_schedules = [
    Cron("41 6 * * 3", timezone="America/Sao_Paulo")
]
