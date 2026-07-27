"""
Lógica compartilhada de execução para br_camara_dados_abertos — Prefect 3.
"""

from pipelines.crawler.camara_dados_abertos.constants import (
    update_metadata_variable_dictionary,
)
from pipelines.crawler.camara_dados_abertos.tasks import (
    check_if_url_is_valid,
    save_data,
)
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run_camara_dados_abertos(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    url_ok = check_if_url_is_valid(table_id)
    if not url_ok and not force_run:
        print(f"URL não disponível para {table_id}; encerrando.")
        return

    filepath = save_data(table_id=table_id)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        coverage = update_metadata_variable_dictionary(
            table_id=table_id, dataset_id=dataset_id
        )
        if coverage is not None:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=coverage,
                env="prod",
                bq_project="basedosdados",
            )
