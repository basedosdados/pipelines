# -*- coding: utf-8 -*-
"""
Flows for ibge inflacao
"""
# pylint: disable=C0103, E1123, invalid-name, duplicate-code, R0801
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.crawler_ibge_inflacao.tasks import (
    check_for_updates,
    clean_mes_brasil,
    clean_mes_geral,
    clean_mes_municipio,
    clean_mes_rm,
    crawler,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    get_temporal_coverage,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="BD Template - IBGE Inflação: mes_brasil"
) as flow_ibge_inflacao_mes_brasil:
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
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    # USA A FUNÇAO EXTRACT_LAST_DATE ENTAO QUEBRA SE A BSE NAO EXISTIR

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )
    # pylint: disable=E1123

    with case(was_downloaded, True):
        filepath = clean_mes_brasil(indice=INDICE, upstream_tasks=[was_downloaded])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
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
                    dataset_id,
                    table_id,
                    metadata_type="DateTimeRange",
                    # needs_to_update[1] é a data (Y%-m%) mais recente
                    _last_date=needs_to_update[1],
                    bq_last_update=False,
                    api_mode="prod",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
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
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )
    # pylint: disable=E1123

    with case(was_downloaded, True):
        # pylint: disable=E1123
        filepath = clean_mes_rm(indice=INDICE, upstream_tasks=[was_downloaded])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
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
                    dataset_id,
                    table_id,
                    metadata_type="DateTimeRange",
                    # needs_to_update[1] é a data (Y%-m%) mais recente
                    _last_date=needs_to_update[1],
                    bq_last_update=False,
                    api_mode="prod",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
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
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )
    # pylint: disable=E1123

    with case(was_downloaded, True):
        # pylint: disable=E1123
        filepath = clean_mes_municipio(indice=INDICE, upstream_tasks=[was_downloaded])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
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
                    dataset_id,
                    table_id,
                    metadata_type="DateTimeRange",
                    # needs_to_update[1] é a data (Y%-m%) mais recente
                    _last_date=needs_to_update[1],
                    bq_last_update=False,
                    api_mode="prod",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
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

    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )
    # pylint: disable=E1123

    with case(was_downloaded, True):
        # pylint: disable=E1123
        filepath = clean_mes_geral(indice=INDICE, upstream_tasks=[was_downloaded])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

        temporal_coverage = get_temporal_coverage(
            filepath=filepath,
            date_cols=["ano", "mes"],
            time_unit="month",
            interval="1",
            upstream_tasks=[wait_upload_table],
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
                    dataset_id,
                    table_id,
                    metadata_type="DateTimeRange",
                    # needs_to_update[1] é a data (Y%-m%) mais recente
                    _last_date=needs_to_update[1],
                    bq_last_update=False,
                    api_mode="prod",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )

flow_ibge_inflacao_mes_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_geral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
