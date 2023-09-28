# -*- coding: utf-8 -*-
"""
Flows for br_bcb_estban
"""

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
    download_estban_files,
    get_id_municipio,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
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

    donwload_files = download_estban_files(
        xpath=br_bcb_estban_constants.MUNICIPIO_XPATH.value,
        save_path=br_bcb_estban_constants.DOWNLOAD_PATH_MUNICIPIO.value,
    )

    municipio = get_id_municipio(table="municipio")

    filepath = cleaning_municipios_data(
        path=br_bcb_estban_constants.DOWNLOAD_PATH_MUNICIPIO.value,
        municipio=municipio,
        upstream_tasks=[donwload_files, municipio],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

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
            update = update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                api_mode="prod",
                billing_project_id="basedosdados",
                date_format="yy-mm",
                is_bd_pro=True,
                is_free=True,
                time_delta=6,
                time_unit="months",
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

    donwload_files = download_estban_files(
        xpath=br_bcb_estban_constants.AGENCIA_XPATH.value,
        save_path=br_bcb_estban_constants.DOWNLOAD_PATH_AGENCIA.value,
    )
    # read_file

    municipio = get_id_municipio(table="municipio")

    filepath = cleaning_agencias_data(
        path=br_bcb_estban_constants.DOWNLOAD_PATH_AGENCIA.value,
        municipio=municipio,
        upstream_tasks=[donwload_files, municipio],
    )
    # 15/16/19/20 sao files problematicos
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

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
            update = update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                api_mode="prod",
                billing_project_id="basedosdados",
                date_format="yy-mm",
                is_bd_pro=True,
                is_free=True,
                time_delta=6,
                time_unit="months",
                upstream_tasks=[wait_for_materialization],
            )

br_bcb_estban_agencia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_estban_agencia.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_bcb_estban_agencia.schedule = every_month_agencia
