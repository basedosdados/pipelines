# -*- coding: utf-8 -*-
"""
Flows for br_rf_cafir
"""
# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.datasets.br_rf_cafir.constants import constants as br_rf_cafir_constants
from pipelines.datasets.br_rf_cafir.tasks import (
    parse_files_parse_date,
    parse_data,
    check_if_bq_data_is_outdated,
    convert_datetime_to_string,
)

from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
    log_task,
)

from pipelines.datasets.br_rf_cafir.schedules import schedule_br_rf_cafir_imoveis_rurais

with Flow(
    name="br_rf_cafir.imoveis_rurais", code_owners=["Gabriel Pisa"]
) as br_rf_cafir_imoveis_rurais:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_rf_cafir", required=True)
    table_id = Parameter("table_id", default="imoveis_rurais", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    info = parse_files_parse_date(url=br_rf_cafir_constants.URL.value[0])
    log_task("Checando se os dados estão desatualizados")

    is_outdated = check_if_bq_data_is_outdated(
        dataset_id=dataset_id, table_id=table_id, data=info[0], upstream_tasks=[info]
    )
    update_metadata_strig_date = convert_datetime_to_string(
        data=info[0], upstream_tasks=[info, is_outdated]
    )

    with case(is_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(is_outdated, True):
        log_task("Existem atualizações! A run será inciada")

        file_path = parse_data(
            url=br_rf_cafir_constants.URL.value[0],
            other_task_output=info,
            upstream_tasks=[info, is_outdated],
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
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )
            # TODO: Quando a nova fotografia for liberada setar is_free como True
            # is_free como true. Não setei agora pq a task update_django_metadata depende
            # de um coverage já criado na API. Como a lag entre fotográfias é de 5 meses (6 é o padrão no monento)
            # não há necessidade de atualizar o coverage agora.

            with case(update_metadata, True):
                update = update_django_metadata(
                    dataset_id,
                    table_id,
                    metadata_type="DateTimeRange",
                    _last_date=update_metadata_strig_date,
                    bq_last_update=False,
                    api_mode="prod",
                    date_format="yy-mm-dd",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )


br_rf_cafir_imoveis_rurais.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_rf_cafir_imoveis_rurais.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_rf_cafir_imoveis_rurais.schedule = schedule_br_rf_cafir_imoveis_rurais
