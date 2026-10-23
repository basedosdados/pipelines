"""
Flows for br_ibama_fiscalizacao — Prefect 3.

Ibama's enforcement records (autos de infração, termos de embargo) from the
open-data portal. Every file ships the full history and is regenerated nightly,
so each run downloads all of them and does a full replace
(``dump_mode="overwrite"``). The flow runs weekly: daily runs would rebuild the
same ~830k rows and the municipality point-in-polygon check seven times as often
for data at most six days fresher.

BD Pro rolling window: both fact tables are refreshed more often than monthly,
so both paywall their most recent six months, keyed on the date of the act
(``data_auto``, ``data_embargo``). ``dicionario`` has no date column and takes no
coverage spec.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers
`br_ibama_fiscalizacao_flow`; the dev pool ignores the schedule, the prod pool
activates it.
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.br_ibama_fiscalizacao.constants import (
    COVERAGE,
    constants,
)
from pipelines.datasets.br_ibama_fiscalizacao.tasks import (
    clean_ibama,
    download_ibama,
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

# The table whose poll gates the run. Both files are regenerated together.
_POLL_TABLE = "auto_infracao"


@flow(name="br_ibama_fiscalizacao", log_prints=True)
def br_ibama_fiscalizacao_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Download the Ibama files, rebuild both tables and the dictionary.

    Args:
        materialize_to_prod: Write the prod staging bucket and run dbt against
            ``target="prod"``. Set False to exercise only the dev half — required
            for a safe test run, since the default writes production.
        update_metadata: After a successful prod materialization, register table
            coverage (which applies the BD Pro Row Access Policies) and commit the
            source update. Has no effect when ``materialize_to_prod`` is False.
        force_run: Materialize even when the source poll reports no new extraction.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="fiscalizacao"
    )

    work_dir = tempfile.mkdtemp(prefix="br_ibama_fiscalizacao_")
    try:
        input_dir = download_ibama(work_dir=work_dir)
        result = clean_ibama(work_dir=work_dir, input_dir=input_dir)

        # The guard is the extraction stamp Ibama prints in every row, compared
        # against Table.Update.latest (a wall clock). A weekly run normally sees a
        # newer stamp; a stale one means Ibama stopped regenerating the files, and
        # the run returns instead of re-materializing identical data.
        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=_POLL_TABLE,
            source_max_date=result["data_extracao"],
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="table_update",
        )
        if not has_new_data and not force_run:
            return

        # The source Update records what Ibama published: its latest act date.
        for table in constants.DATA_TABLES.value:
            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=table,
                source_max_date=result["max_date"][table],
                env="prod",
                date_format="%Y-%m-%d",
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

        if materialize_to_prod:
            bucket_name, target = "basedosdados", "prod"
        else:
            # The dev materialization is the pre-arm validation path. An armed run
            # skips it: prod runs the same models and tests minutes later, and
            # building dev too doubles the BigQuery bytes billed for no signal.
            bucket_name, target = "basedosdados-dev", "dev"

        for table in constants.DATA_TABLES.value:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name=bucket_name,
                dump_mode="overwrite",
                source_format="parquet",
            )

        # Run every model, then test every model. The dictionary and the
        # area_embargada -> auto_infracao relationship test both read sibling
        # models, so a test interleaved with the runs would read a table that has
        # not been rebuilt yet.
        for table in constants.ALL_TABLES.value:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run",
                target=target,
            )
        for table in constants.ALL_TABLES.value:
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="test",
                target=target,
            )

        if materialize_to_prod and update_metadata:
            for table, coverage in COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both early returns and any exception.
        shutil.rmtree(work_dir, ignore_errors=True)


# Ibama rewrites the files around 00:50 BRT. Run Wednesday mornings at a minute
# no other flow uses.
br_ibama_fiscalizacao_flow.deploy_schedules = [
    Cron("29 7 * * 3", timezone="America/Sao_Paulo")
]
# The clean step holds both full tables in memory as Python lists; it peaked at
# 1.4 GB RSS on the 2026-10-13 extraction (the embargo file is 210 MB, most of it
# polygon WKT). `memory` alone is ignored by the work pool; `memory_limit` is the
# one the pod gets.
br_ibama_fiscalizacao_flow.job_variables = {
    "memory": "4Gi",
    "memory_limit": "4Gi",
    "memory_request": "2Gi",
}
