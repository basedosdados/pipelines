# -*- coding: utf-8 -*-
"""
Flows for br_bcb_estban
"""
# TODO: UPLOAD MUNICIPIOS; UPLOAD AGENCIA 03
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_bcb_estban.constants import (
    constants as br_bcb_estban_constants,
)
from pipelines.datasets.br_bcb_estban.schedules import (
    every_month_agencia,
    every_month_municipio,
)
from pipelines.datasets.br_bcb_estban.tasks import (
    cleaning_agencias_data,
    cleaning_municipios_data,
    download_estban_selenium,
    extract_last_date,
    get_id_municipio,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_bcb_estban.municipio",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_estban_municipio:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_bcb_estban", required=True)
    table_id = Parameter("table_id", default="municipio", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )

    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    extract_last_date = extract_last_date(table_id=table_id)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=extract_last_date[0],
        date_format="%Y-%m",
        upstream_tasks=[extract_last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será inciada")

        donwload_files = download_estban_selenium(
            save_path=br_bcb_estban_constants.ZIPFILE_PATH_MUNICIPIO.value,
            table_id=table_id,
            date=extract_last_date[1],
        )

        municipio = get_id_municipio(upstream_tasks=[donwload_files])

        filepath = cleaning_municipios_data(
            municipio=municipio,
            upstream_tasks=[donwload_files, municipio],
        )

        # wait_upload_table = create_table_and_upload_to_gcs(
        #     data_path=filepath,
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     dump_mode="append",
        #     wait=filepath,
        # )

        # municipio
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

br_bcb_estban_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_estban_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_bcb_estban_municipio.schedule = every_month_municipio


with Flow(
    name="br_bcb_estban.agencia",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_estban_agencia:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_bcb_estban", required=True)
    table_id = Parameter("table_id", default="agencia", required=True)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )

    materialize_after_dump = Parameter(
        "materialize after dump", default=False, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    extract_last_date = extract_last_date(table_id=table_id)

    # task check if is outdated
    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=extract_last_date[0],
        date_format="%Y-%m",
        upstream_tasks=[extract_last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será inciada")

        donwload_files = download_estban_selenium(
            save_path=br_bcb_estban_constants.ZIPFILE_PATH_AGENCIA.value,
            table_id=table_id,
            date=extract_last_date[1],
        )

        # read_file
        municipio = get_id_municipio()

        filepath = cleaning_agencias_data(
            municipio=municipio,
            upstream_tasks=[donwload_files, municipio],
        )

        # 15/16/19/20 sao files problematicos
        # wait_upload_table = create_table_and_upload_to_gcs(
        #    data_path=filepath,
        #    dataset_id=dataset_id,
        #    table_id=table_id,
        #    dump_mode="append",
        #    wait=filepath,
        # )

        # agencia
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

br_bcb_estban_agencia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_estban_agencia.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_bcb_estban_agencia.schedule = every_month_agencia
