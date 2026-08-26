"""Flow do br_fnde_fundeb — Prefect 3.

Atualiza o exercício corrente dos Indicadores do SIOPE a partir do produto 54 da
Plataforma Antonieta de Barros. Contexto da fonte, decisões de modelagem e a
divisão entre carga histórica e atualização estão no README do conjunto.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_fnde_fundeb.constants import constants
from pipelines.datasets.br_fnde_fundeb.tasks import clean_siope, download_siope
from pipelines.utils.metadata.domain import (
    DateFormat,
    FreeLag,
    PartBdpro,
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

DATE_FORMAT = constants.DATE_FORMAT.value

TABLES = [
    constants.TABLE_STATE.value,
    constants.TABLE_MUNICIPALITY.value,
]

POLL_TABLE = constants.TABLE_MUNICIPALITY.value

_COVERAGE = PartBdpro(
    date_column=YearOnly(col="ano"),
    date_format=DateFormat.YEAR,
    free_lag=FreeLag(unit="years", value=1),
)


@flow(name="br_fnde_fundeb", log_prints=True)
def br_fnde_fundeb_flow(
    dataset_id: str = constants.DATASET_ID.value,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Baixa o exercício corrente do SIOPE, remonta as tabelas e materializa."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="br_fnde_fundeb_")
    try:
        source_path = download_siope(
            work_dir=work_dir, product_id=constants.PRODUCT_CURRENT.value
        )
        result = clean_siope(work_dir=work_dir, source_path=source_path)
        max_date = result["max_date"]

        if materialize_to_prod:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=POLL_TABLE,
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
            )
            if not has_new_data and not force_run:
                return

            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=POLL_TABLE,
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
                update_metadata=update_metadata,
                materialize_after_dump=materialize_to_prod,
            )

        for table_id in TABLES:
            upload_to_gcs(
                data_path=result[table_id],
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name="basedosdados-dev",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run/test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        for table_id in TABLES:
            upload_to_gcs(
                data_path=result[table_id],
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name="basedosdados",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run/test",
                target="prod",
            )

        if not update_metadata:
            return

        for table_id in TABLES:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=_COVERAGE,
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# pyrefly: ignore [missing-attribute]
br_fnde_fundeb_flow.deploy_schedules = [
    {"cron": "17 10 5,12,19,26 * *", "timezone": "America/Sao_Paulo"}
]

# pyrefly: ignore [missing-attribute]
br_fnde_fundeb_flow.job_variables = {"memory": "4Gi"}
