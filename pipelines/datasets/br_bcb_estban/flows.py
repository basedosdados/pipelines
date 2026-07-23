"""
Flows para br_bcb_estban — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bcb_estban.tasks import (
    cleaning_data,
    download_table,
    extract_urls_list,
    get_documents_metadata,
    get_id_municipio,
    get_latest_file,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
    task_get_api_most_recent_date,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run_bcb_estban(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    documents_metadata = get_documents_metadata(table_id)
    if documents_metadata is None:
        raise RuntimeError(
            "BCB metadata was not loaded! It was not possible to determine if the dataset is up to date."
        )

    _, data_source_max_date = get_latest_file(documents_metadata)

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    print("Existem atualizações! A run será iniciada.")
    api_max_date = task_get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="%Y-%m",
        api_mode="prod",
    )

    # pyrefly: ignore [no-matching-overload]
    urls_list = extract_urls_list(
        documents_metadata,
        data_source_max_date,
        api_max_date,
        date_format="%Y-%m",
    )

    for url in urls_list:
        download_table(url=url, table_id=table_id)

    df_diretorios = get_id_municipio()
    filepath = cleaning_data(table_id, df_diretorios)

    upload_to_gcs(
        data_path=filepath,
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
        data_path=filepath,
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
                date_column=YearMonth(year="ano", month="mes"),
                date_format=DateFormat.YEAR_MONTH,
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if data_source_max_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=data_source_max_date,
                env="prod",
                date_format="%Y-%m",
            )


def _estban_flow(table_id: str, cron: str):
    @flow(
        name=f"br_bcb_estban__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_bcb_estban",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_bcb_estban(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_bcb_estban__agencia = _estban_flow(
    table_id="agencia",
    cron="0 22 25-31 * *",
)

br_bcb_estban__municipio = _estban_flow(
    table_id="municipio",
    cron="30 22 25-31 * *",
)
