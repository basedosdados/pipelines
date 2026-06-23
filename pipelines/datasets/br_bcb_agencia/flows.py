"""
Flow br_bcb_agencia__agencia — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bcb_agencia.tasks import (
    clean_data,
    download_table,
    extract_urls_list,
    get_documents_metadata,
    get_latest_file,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    register_source_poll_task,
    register_table_materialization_task,
    task_get_api_most_recent_date,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


@flow(
    name="br_bcb_agencia__agencia",
    log_prints=True,
)
def br_bcb_agencia__agencia(
    dataset_id: str = "br_bcb_agencia",
    table_id: str = "agencia",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    documents_metadata = get_documents_metadata()
    if documents_metadata is None:
        raise RuntimeError(
            "BCB metadata was not loaded! It was not possible to determine "
            "if the dataset is up to date."
        )

    _, data_source_max_date = get_latest_file(documents_metadata)

    if not force_run:
        is_outdated = register_source_poll_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m",
        )
        if not is_outdated:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    print("Existem atualizações! A run será iniciada.")
    api_max_date = task_get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="%Y-%m",
        api_mode="prod",
    )

    urls_list = extract_urls_list(
        documents_metadata,
        data_source_max_date,
        api_max_date,
        date_format="%Y-%m",
    )

    for url in urls_list:
        download_table(url=url)

    filepath = clean_data()

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


br_bcb_agencia__agencia.deploy_schedules = [
    {"cron": "0 22 25-31 * *", "timezone": "America/Sao_Paulo"}
]
