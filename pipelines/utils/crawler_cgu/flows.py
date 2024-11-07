# -*- coding: utf-8 -*-
"""
Flows for br_cgu_cartao_pagamento
"""
from calendar import month
from datetime import timedelta
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Parameter, case
from pipelines.constants import constants
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.crawler_cgu.tasks import (
    partition_data,
    get_current_date_and_download_file,
    verify_all_url_exists_to_download,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata, check_if_data_is_outdated
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
    log_task
)


with Flow(
    name="CGU - Cartão de Pagamento"
) as flow_cgu_cartao_pagamento:

    dataset_id = Parameter("dataset_id", default='br_cgu_cartao_pagamento',  required=True)
    table_id = Parameter("table_id", default ="microdados_governo_federal", required=True)
    ####
    # Relative_month =  1 means that the data will be downloaded for the current month
    ####
    relative_month = Parameter("relative_month", default=-1, required=False)
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_after_dump = Parameter("materialize_after_dump", default=True, required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    rename_flow_run = rename_current_flow_run_dataset_table(prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id)

    data_source_max_date = get_current_date_and_download_file(
        table_id,
        dataset_id,
        relative_month,
    )

    dados_desatualizados = check_if_data_is_outdated(
    dataset_id=dataset_id,
    table_id=table_id,
    data_source_max_date=data_source_max_date,
    date_format="%Y-%m",
    upstream_tasks=[data_source_max_date]
    )

    with case(dados_desatualizados, True):

        filepath = partition_data(
            table_id=table_id,
            dataset_id=dataset_id,

            upstream_tasks=[dados_desatualizados],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[filepath],
        )

        with case(materialize_after_dump, True):

            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                    "dbt_command": "run/test",
                    "disable_elementary": False,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
                upstream_tasks=[wait_upload_table],
            )

            wait_for_materialization = wait_for_flow_run(
                materialization_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
                upstream_tasks=[materialization_flow],
            )
            wait_for_materialization.max_retries = (
                dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
            )
            wait_for_materialization.retry_delay = timedelta(
                seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
            )
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano_extrato", "month": "mes_extrato"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

flow_cgu_cartao_pagamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cgu_cartao_pagamento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

# ! ============================================== CGU - Servidores Públicos do Executivo Federal ==============================================

with Flow(
    name="CGU - Servidores Públicos do Executivo Federal", code_owners=["trick"]
) as flow_cgu_servidores_publicos:

    dataset_id = Parameter("dataset_id", default="br_cgu_servidores_executivo_federal", required=True)
    table_id = Parameter("table_id", required=True)
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_after_dump = Parameter("materialize_after_dump", default=True, required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    rename_flow_run = rename_current_flow_run_dataset_table(prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id)

    ####
    # Relative_month =  1 means that the data will be downloaded for the current month
    ####
    relative_month = Parameter("relative_month", default=1, required=False)

    with case(verify_all_url_exists_to_download(dataset_id, table_id, relative_month), True):
        data_source_max_date = get_current_date_and_download_file(
            table_id,
            dataset_id,
            relative_month,
        )

        dados_desatualizados = check_if_data_is_outdated(
            dataset_id=dataset_id,
            table_id=table_id,
            data_source_max_date=data_source_max_date,
            date_format="%Y-%m",
            upstream_tasks=[data_source_max_date],
        )

        with case(dados_desatualizados, True):
            filepath = partition_data(
                table_id=table_id, dataset_id=dataset_id, upstream_tasks=[data_source_max_date]
            )
            wait_upload_table = create_table_and_upload_to_gcs(
                data_path=filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                wait=filepath,
                upstream_tasks=[filepath],
            )

            with case(materialize_after_dump, True):

                current_flow_labels = get_current_flow_labels()
                materialization_flow = create_flow_run(
                    flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                    project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                    parameters={
                        "dataset_id": dataset_id,
                        "table_id": table_id,
                        "mode": materialization_mode,
                        "dbt_alias": dbt_alias,
                        "dbt_command": "run/test",
                        "disable_elementary": False,
                    },
                    labels=current_flow_labels,
                    run_name=f"Materialize {dataset_id}.{table_id}",
                    upstream_tasks=[wait_upload_table],
                )

                wait_for_materialization = wait_for_flow_run(
                    materialization_flow,
                    stream_states=True,
                    stream_logs=True,
                    raise_final_state=True,
                    upstream_tasks=[materialization_flow],
                )
                wait_for_materialization.max_retries = (
                    dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
                )
                wait_for_materialization.retry_delay = timedelta(
                    seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
                )
                with case(update_metadata, True):
                    update_django_metadata(
                        dataset_id=dataset_id,
                        table_id=table_id,
                        date_column_name={"year": "ano", "month": "mes"},
                        date_format="%Y-%m",
                        coverage_type="part_bdpro",
                        time_delta={"months": 6},
                        prefect_mode=materialization_mode,
                        bq_project="basedosdados",
                        upstream_tasks=[wait_for_materialization],
                    )

flow_cgu_servidores_publicos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cgu_servidores_publicos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
