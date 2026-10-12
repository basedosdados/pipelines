"""
Flows de br_bndes_desembolsos — Prefect 3.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_desembolsos.tasks import (
    clean_table,
    download_table,
    get_source_last_modified,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearMonth
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


@flow(
    name="br_bndes_desembolsos__mensal",
    log_prints=True,
)
def br_bndes_desembolsos__mensal(
    dataset_id: str = "br_bndes_desembolsos",
    table_id: str = "mensal",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Atualiza a mensal quando o BNDES republica o recurso."""
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    source_max_date = get_source_last_modified()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format=DateFormat.YEAR_MD,
            compare_against="table_update",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
        env="prod",
        date_format=DateFormat.YEAR_MD,
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    csv_path = download_table()
    filepath = clean_table(csv_path=csv_path)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="overwrite",
        source_format="parquet",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="prod",
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllFree(
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_bndes_desembolsos__mensal.deploy_schedules = [
    Cron("35 2 * * 1", timezone="America/Sao_Paulo")
]
