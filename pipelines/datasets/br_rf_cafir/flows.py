"""
Flows for br_rf_cafir — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.rf_cafir.constants import (
    constants as br_rf_cafir_constants,
)
from pipelines.crawler.rf_cafir.tasks import (
    task_decide_files_to_download,
    task_download_files,
    task_parse_api_metadata,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
)
from pipelines.utils.metadata.tasks import (
    register_source_poll_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


@flow(
    name="br_rf_cafir__imoveis_rurais",
    log_prints=True,
)
def br_rf_cafir__imoveis_rurais(
    dataset_id: str = "br_rf_cafir",
    table_id: str = "imoveis_rurais",
    materialize_after_dump: bool = True,
    dbt_alias: bool = False,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    df_metadata = task_parse_api_metadata(url=br_rf_cafir_constants.URL.value)

    arquivos, data_atualizacao = task_decide_files_to_download(df=df_metadata)

    if not force_run:
        is_outdated = register_source_poll_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_atualizacao,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not is_outdated:
            return

    file_path = task_download_files(
        url=br_rf_cafir_constants.URL.value,
        file_list=arquivos,
        data_atualizacao=data_atualizacao,
    )

    upload_to_gcs(
        data_path=file_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
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
        data_path=file_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=PartBdpro(
                date_column=DateOnly(col="data_referencia"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


br_rf_cafir__imoveis_rurais.deploy_schedules = [
    {"cron": "0 0 * * *", "timezone": "America/Sao_Paulo"}
]
