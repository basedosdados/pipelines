"""
Flows para br_denatran_frota — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.denatran_frota.constants import (
    constants as denatran_constants,
)
from pipelines.crawler.denatran_frota.tasks import (
    crawl_task,
    get_desired_file_task,
    get_latest_date_task,
    treat_municipio_tipo_task,
    treat_uf_tipo_task,
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
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run_denatran(
    *,
    dataset_id: str,
    table_id: str,
    filetype: str,
    treat_task,
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

    (
        source_available_dates,
        _source_available_dates_str,
        _source_first_available_date,
        source_first_available_date_str,
    ) = get_latest_date_task(table_id=table_id, dataset_id=dataset_id)

    print(f"First available date: {source_first_available_date_str}")

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_first_available_date_str,
            env="prod",
            date_format="%Y-%m",
        )
        if not has_new_data:
            print("No new data to be downloaded")
            return

    print("Updates found! The run will be started.")
    parquet_outputs = []
    for source_max_date in source_available_dates:
        crawl_task(
            source_max_date=source_max_date,
            table_id=table_id,
            temp_dir=denatran_constants.DOWNLOAD_PATH.value,
        )
        # pyrefly: ignore [no-matching-overload]
        desired_file = get_desired_file_task(
            source_max_date=source_max_date,
            download_directory=denatran_constants.DOWNLOAD_PATH.value,
            table_id=table_id,
            filetype=filetype,
        )
        parquet_outputs.append(treat_task(file=desired_file))

    filepath = parquet_outputs[0]

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

        if source_first_available_date_str is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_first_available_date_str,
                env="prod",
                date_format="%Y-%m",
            )


@flow(
    name="br_denatran_frota__uf_tipo",
    log_prints=True,
)
def br_denatran_frota__uf_tipo(
    dataset_id: str = "br_denatran_frota",
    table_id: str = "uf_tipo",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_denatran(
        dataset_id=dataset_id,
        table_id=table_id,
        filetype=denatran_constants.UF_TIPO_BASIC_FILENAME.value,
        treat_task=treat_uf_tipo_task,
        materialize_after_dump=materialize_after_dump,
        dbt_alias=dbt_alias,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


@flow(
    name="br_denatran_frota__municipio_tipo",
    log_prints=True,
)
def br_denatran_frota__municipio_tipo(
    dataset_id: str = "br_denatran_frota",
    table_id: str = "municipio_tipo",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_denatran(
        dataset_id=dataset_id,
        table_id=table_id,
        filetype=denatran_constants.MUNIC_TIPO_BASIC_FILENAME.value,
        treat_task=treat_municipio_tipo_task,
        materialize_after_dump=materialize_after_dump,
        dbt_alias=dbt_alias,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# pyrefly: ignore [missing-attribute]
br_denatran_frota__uf_tipo.deploy_schedules = [
    {"cron": "0 21 10-30 * *", "timezone": "America/Sao_Paulo"}
]
# pyrefly: ignore [missing-attribute]
br_denatran_frota__municipio_tipo.deploy_schedules = [
    {"cron": "20 21 10-30 * *", "timezone": "America/Sao_Paulo"}
]
