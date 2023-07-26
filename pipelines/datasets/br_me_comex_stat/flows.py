# -*- coding: utf-8 -*-
"""
Flows for  br_me_comex_stat
"""
# pylint: disable=invalid-name
from datetime import datetime, timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_me_comex_stat.tasks import (
    download_br_me_comex_stat,
    clean_br_me_comex_stat,
)
from pipelines.datasets.br_me_comex_stat.constants import constants as comex_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    update_django_metadata,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

from pipelines.datasets.br_me_comex_stat.schedules import (
    schedule_municipio_exportacao,
    schedule_municipio_importacao,
    schedule_ncm_exportacao,
    schedule_ncm_importacao,
)


with Flow(
    name="br_me_comex_stat.municipio_exportacao", code_owners=["Gabriel Pisa"]
) as dataset_br_me_comex_municipio_exportacao_flow:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="municipio_exportacao", required=True)
    start = Parameter("start", default=2023, required=True)  # confirmar depois
    update_metadata = Parameter("update_metadata", default=False, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[1],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[1],
        upstream_tasks=[download_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    # municipio_exportacao

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

    # municipio_exportacao_atualizado
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id + "_atualizado",
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
            update = update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
            )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id,
                table_id + "_atualizado",
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
                upstream_tasks=[update],
            )

dataset_br_me_comex_municipio_exportacao_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
dataset_br_me_comex_municipio_exportacao_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
dataset_br_me_comex_municipio_exportacao_flow.schedule = schedule_municipio_exportacao


with Flow(
    name="br_me_comex_stat.municipio_importacao", code_owners=["Gabriel Pisa"]
) as br_comex_municipio_importacao:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="municipio_importacao", required=True)
    start = Parameter("start", default=2023, required=True)  # confirmar depois
    update_metadata = Parameter("update_metadata", default=False, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[0],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[0],
        upstream_tasks=[download_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    # municipio_importacao

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

        # municipio_importacao_atualizado
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id + "_atualizado",
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
            update = update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
            )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id,
                table_id + "_atualizado",
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
                upstream_tasks=[update],
            )

br_comex_municipio_importacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_municipio_importacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_comex_municipio_importacao.schedule = schedule_municipio_importacao


with Flow(
    name="br_me_comex_stat.ncm_exportacao", code_owners=["Gabriel Pisa"]
) as br_comex_ncm_exportacao:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="ncm_exportacao", required=True)
    start = Parameter("start", default=2023, required=True)  # confirmar depois
    update_metadata = Parameter("update_metadata", default=False, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[3],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[3],
        upstream_tasks=[download_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    # ncm_exportacao

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

    # ncm_exportacao_atualizado
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id + "_atualizado",
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
            update = update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
            )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id,
                table_id + "_atualizado",
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
                upstream_tasks=[update],
            )

br_comex_ncm_exportacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_ncm_exportacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_comex_ncm_exportacao.schedule = schedule_ncm_exportacao


with Flow(
    name="br_me_comex_stat.ncm_importacao", code_owners=["Gabriel Pisa"]
) as br_comex_ncm_importacao:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="ncm_importacao", required=True)
    start = Parameter("start", default=2023, required=True)  # confirmar depois
    update_metadata = Parameter("update_metadata", default=False, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[2],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[2],
        upstream_tasks=[download_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )
    # ncm_importacao
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

    # ncm_importacao_atualizado
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id + "_atualizado",
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
            update = update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
            )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id,
                table_id + "_atualizado",
                metadata_type="DateTimeRange",
                bq_last_update=True,
                api_mode="prod",
                date_format="yy-mm",
                upstream_tasks=[update],
            )

br_comex_ncm_importacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_ncm_importacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_comex_ncm_importacao.schedule = schedule_ncm_importacao
