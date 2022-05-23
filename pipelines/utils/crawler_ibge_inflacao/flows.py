# -*- coding: utf-8 -*-
"""
Flows for ibge inflacao
"""
# pylint: disable=C0103, E1123, invalid-name, duplicate-code, R0801
from datetime import datetime, timedelta

from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.constants import constants
from pipelines.utils.crawler_ibge_inflacao.tasks import (
    crawler,
    clean_mes_brasil,
    clean_mes_rm,
    clean_mes_municipio,
    clean_mes_geral,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_metadata,
    get_temporal_coverage,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

with Flow(
    name="BD Template - IBGE Inflação: mes_brasil"
) as flow_ibge_inflacao_mes_brasil:

    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_brasil(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[wait_upload_table],
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            {"temporal_coverage": [temporal_coverage]},
        ],
        upstream_tasks=[temporal_coverage],
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": f'{dataset_id}_{table_id}',
                "mode": materialization_mode,
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

flow_ibge_inflacao_mes_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_brasil.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow("BD Template - IBGE Inflação: mes_rm") as flow_ibge_inflacao_mes_rm:
    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_rm(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[wait_upload_table],
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            {"temporal_coverage": [temporal_coverage]},
        ],
        upstream_tasks=[temporal_coverage],
    )

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

flow_ibge_inflacao_mes_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_rm.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow(
    "BD Template - IBGE Inflação: mes_municipio"
) as flow_ibge_inflacao_mes_municipio:

    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_municipio(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[wait_upload_table],
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            {"temporal_coverage": [temporal_coverage]},
        ],
        upstream_tasks=[temporal_coverage],
    )

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


flow_ibge_inflacao_mes_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow("BD Template - IBGE Inflação: mes_geral") as flow_ibge_inflacao_mes_geral:

    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    wait_crawler = crawler(indice=INDICE, folder=FOLDER)
    # pylint: disable=E1123
    filepath = clean_mes_geral(indice=INDICE, upstream_tasks=[wait_crawler])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[wait_upload_table],
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            {"temporal_coverage": [temporal_coverage]},
        ],
        upstream_tasks=[temporal_coverage],
    )

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

flow_ibge_inflacao_mes_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_geral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
