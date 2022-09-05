# -*- coding: utf-8 -*-
"""
Flows for br_me_comex_stat
"""

from datetime import datetime, timedelta

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow

from pipelines.datasets.br_me_comex_stat.tasks import (
    clean_br_me_comex_stat,                                                                         
)

from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_metadata,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

from pipelines.datasets.br_me_comex_stat.schedules import (
    schedule_municipio_exportacao,
    schedule_municipio_importacao,
    schedule_ncm_importacao,
    schedule_ncm_exportacao,
)

from pipelines.datasets.br_me_comex_stat.constants import constants as comex_constants

with Flow(
    name="br_me_comex_stat.municipio_exportacao", code_owners=["crislanealves"]
) as br_comex_mx:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="municipio_exportacao", required=True)
    start = Parameter("start", default=1997, required=True)                          ## confirmar depois
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_after_dump = Parameter("materialize after dump", default=True, required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id)

    filepath = clean_br_me_comex_stat()                                                   
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
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

br_comex_mx.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_mx.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_comex_mx.schedule = schedule_municipio_exportacao


with Flow(
    name="br_me_comex_stat.municipio_importacao", code_owners=["crislanealves"]
) as br_comex_mi:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="municipio_importacao", required=True)
    start = Parameter("start", default=1997, required=True)                          ## confirmar depois
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_after_dump = Parameter("materialize after dump", default=True, required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id)

    filepath = clean_br_me_comex_stat()                                                   
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
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

br_comex_mi.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_mi.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_comex_mi.schedule = schedule_municipio_importacao


with Flow(
    name="br_me_comex_stat.ncm_exportacao", code_owners=["crislanealves"]
) as br_comex_ncm_export:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="ncm_exportacao", required=True)
    start = Parameter("start", default=1997, required=True)                          ## confirmar depois
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_after_dump = Parameter("materialize after dump", default=True, required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id)

    filepath = clean_br_me_comex_stat()                                                   
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
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

br_comex_ncm_export.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_ncm_export.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_comex_ncm_export.schedule = schedule_ncm_exportacao


with Flow(
    name="br_me_comex_stat.ncm_importacao", code_owners=["crislanealves"]
) as br_comex_ncm_import:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_me_comex_stat", required=True)
    table_id = Parameter("table_id", default="ncm_importacao", required=True)
    start = Parameter("start", default=1997, required=True)                          ## confirmar depois
    materialization_mode = Parameter("materialization_mode", default="dev", required=False)
    materialize_after_dump = Parameter("materialize after dump", default=True, required=False)
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id)

    filepath = clean_br_me_comex_stat()                                                   
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
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

br_comex_ncm_import.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_ncm_import.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_comex_ncm_import.schedule = schedule_ncm_importacao