"""
Flows for br_bcb_estban
"""

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_bcb_estban.schedules import (
    every_month_agencia,
    every_month_municipio,
)
from pipelines.datasets.br_bcb_estban.tasks import (
    cleaning_data,
    download_table,
    extract_urls_list,
    get_api_max_date,
    get_documents_metadata,
    get_id_municipio,
    get_latest_file,
    raise_none_metadata_exception,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_bcb_estban.municipio",
    code_owners=[
        "Luiza",
    ],
) as br_bcb_estban_municipio:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bcb_estban", required=True
    )
    table_id = Parameter("table_id", default="municipio", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    # Materialization mode
    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    documents_metadata = get_documents_metadata(table_id)

    # Checando se os metadados foram carregados
    with case(documents_metadata is None, False):
        data_source_download_url, data_source_max_date = get_latest_file(
            documents_metadata
        )

        check_if_outdated = check_if_data_is_outdated(
            dataset_id=dataset_id,
            table_id=table_id,
            data_source_max_date=data_source_max_date,
            date_format="%Y-%m",
            upstream_tasks=[data_source_max_date],
        )
        with case(check_if_outdated, False):
            log_task(f"Não há atualizações para a tabela de {table_id}!")

        with case(check_if_outdated, True):
            log_task("Existem atualizações! A run será inciada")
            api_max_date = get_api_max_date(dataset_id, table_id)

            urls_list = extract_urls_list(
                documents_metadata,
                data_source_max_date,
                api_max_date,
                date_format="%Y-%m",
                upstream_tasks=[
                    api_max_date,
                    data_source_max_date,
                    documents_metadata,
                ],
            )

            downloaded_file_paths = download_table.map(
                url=urls_list,
                table_id=unmapped(table_id),
                upstream_tasks=[unmapped(check_if_outdated)],
            )
            df_diretorios = get_id_municipio(
                upstream_tasks=[downloaded_file_paths]
            )

            filepath = cleaning_data(
                table_id,
                df_diretorios,
                upstream_tasks=[downloaded_file_paths, df_diretorios],
            )
            wait_upload_table = create_table_dev_and_upload_to_gcs(
                data_path=filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[filepath],
            )

            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run/test",
                target=target,
                dbt_alias=dbt_alias,
                upstream_tasks=[wait_upload_table],
            )

            # municipio
            with case(materialize_after_dump, True):
                wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                    data_path=filepath,
                    dataset_id=dataset_id,
                    table_id=table_id,
                    dump_mode="append",
                    upstream_tasks=[wait_for_materialization],
                )

                with case(update_metadata, True):
                    update_django_metadata(
                        dataset_id=dataset_id,
                        table_id=table_id,
                        date_column_name={"year": "ano", "month": "mes"},
                        date_format="%Y-%m",
                        coverage_type="part_bdpro",
                        time_delta={"months": 6},
                        prefect_mode=target,
                        bq_project="basedosdados",
                        upstream_tasks=[wait_upload_prod],
                    )
    with case(documents_metadata is None, True):
        raise_none_metadata_exception(
            "BCB metadata was not loaded! It was not possible to determine if the dataset is up to date."
        )
br_bcb_estban_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_estban_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_estban_municipio.schedule = every_month_municipio


with Flow(
    name="br_bcb_estban.agencia",
    code_owners=[
        "Luiza",
    ],
) as br_bcb_estban_agencia:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bcb_estban", required=True
    )
    table_id = Parameter("table_id", default="agencia", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    # Materialization mode
    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    documents_metadata = get_documents_metadata(table_id)

    # Checando se os metadados foram carregados
    with case(documents_metadata is None, False):
        data_source_download_url, data_source_max_date = get_latest_file(
            documents_metadata
        )

        check_if_outdated = check_if_data_is_outdated(
            dataset_id=dataset_id,
            table_id=table_id,
            data_source_max_date=data_source_max_date,
            date_format="%Y-%m",
            upstream_tasks=[data_source_max_date],
        )
        with case(check_if_outdated, False):
            log_task(f"Não há atualizações para a tabela de {table_id}!")

        with case(check_if_outdated, True):
            log_task("Existem atualizações! A run será inciada")
            api_max_date = get_api_max_date(dataset_id, table_id)

            urls_list = extract_urls_list(
                documents_metadata,
                data_source_max_date,
                api_max_date,
                date_format="%Y-%m",
            )

            downloaded_file_paths = download_table.map(
                url=urls_list,
                table_id=unmapped(table_id),
                upstream_tasks=[unmapped(check_if_outdated)],
            )
            # wait(downloaded_file_paths)
            df_diretorios = get_id_municipio(
                upstream_tasks=[downloaded_file_paths]
            )

            filepath = cleaning_data(
                table_id,
                df_diretorios,
                upstream_tasks=[downloaded_file_paths, df_diretorios],
            )
            wait_upload_table = create_table_dev_and_upload_to_gcs(
                data_path=filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[filepath],
            )
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                dbt_command="run/test",
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )

            # agencia
            with case(materialize_after_dump, True):
                wait_upload_table = create_table_prod_gcs_and_run_dbt(
                    data_path=filepath,
                    dataset_id=dataset_id,
                    table_id=table_id,
                    dump_mode="append",
                    upstream_tasks=[wait_for_materialization],
                )

                with case(update_metadata, True):
                    update_django_metadata(
                        dataset_id=dataset_id,
                        table_id=table_id,
                        date_column_name={"year": "ano", "month": "mes"},
                        date_format="%Y-%m",
                        coverage_type="part_bdpro",
                        time_delta={"months": 6},
                        prefect_mode=target,
                        bq_project="basedosdados",
                        upstream_tasks=[wait_upload_table],
                    )
    with case(documents_metadata is None, True):
        raise_none_metadata_exception(
            "BCB metadata was not loaded! It was not possible to determine if the dataset is up to date."
        )
br_bcb_estban_agencia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_estban_agencia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_estban_agencia.schedule = every_month_agencia
