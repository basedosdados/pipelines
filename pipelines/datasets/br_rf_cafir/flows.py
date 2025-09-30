"""
Flows for br_rf_cafir
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_rf_cafir.constants import (
    constants as br_rf_cafir_constants,
)
from pipelines.datasets.br_rf_cafir.schedules import (
    schedule_br_rf_cafir_imoveis_rurais,
)
from pipelines.datasets.br_rf_cafir.tasks import (
    task_decide_files_to_download,
    task_download_files,
    task_parse_api_metadata,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_rf_cafir.imoveis_rurais", code_owners=["Gabriel Pisa"]
) as br_rf_cafir_imoveis_rurais:
    dataset_id = Parameter("dataset_id", default="br_rf_cafir", required=True)
    table_id = Parameter("table_id", default="imoveis_rurais", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    df_metadata = task_parse_api_metadata(
        url=br_rf_cafir_constants.URL.value[0],
        headers=br_rf_cafir_constants.HEADERS.value,
    )

    arquivos, data_atualizacao = task_decide_files_to_download(
        df=df_metadata,
        upstream_tasks=[df_metadata],
    )

    is_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_atualizacao,
        date_format="%Y-%m-%d",
        upstream_tasks=[arquivos],
    )

    with case(is_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(is_outdated, True):
        log_task("Existem atualizações! A run será inciada")

        file_path = task_download_files(
            url=br_rf_cafir_constants.URL.value[0],
            file_list=arquivos,
            headers=br_rf_cafir_constants.HEADERS.value,
            data_atualizacao=data_atualizacao,
            upstream_tasks=[arquivos, is_outdated],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=file_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=file_path,
        )

        # imoveis_rurais
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_referencia"},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )


br_rf_cafir_imoveis_rurais.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_rf_cafir_imoveis_rurais.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_rf_cafir_imoveis_rurais.schedule = schedule_br_rf_cafir_imoveis_rurais
