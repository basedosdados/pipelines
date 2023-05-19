# -*- coding: utf-8 -*-
"""
Flows for br_cvm_fi
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from datetime import timedelta
from pipelines.datasets.br_cvm_fi.tasks import (
    extract_links_and_dates,
    check_for_updates,
    is_empty,
    download_unzip_csv,
    clean_data_and_make_partitions,
    clean_data_make_partitions_cda,
)
from pipelines.datasets.br_cvm_fi.schedules import every_day_cvm
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.utils.decorators import Flow
from prefect import Parameter, case
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.datasets.br_cvm_fi.constants import constants as cvm_constants
from pipelines.constants import constants
from pipelines.utils.utils import log
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

with Flow(
    name="br_cvm_fi_documentos_informe_diario",
    code_owners=[
        "arthurfg",
    ],
) as br_cvm_fi_documentos_informe_diario:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=False)
    table_id = Parameter(
        "table_id", default="documentos_informe_diario", required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    url = Parameter(
        "url",
        default=cvm_constants.INFORME_DIARIO_URL.value,
        required=True,
    )

    df = extract_links_and_dates(url)
    arquivos = check_for_updates(df, upstream_tasks=[df])

    with case(is_empty(arquivos), True):
        log(f"Não houveram atualizações em {url.default}!")

    with case(is_empty(arquivos), False):
        input_filepath = download_unzip_csv(arquivos, upstream_tasks=[arquivos])
        output_filepath = clean_data_and_make_partitions(
            input_filepath, upstream_tasks=[input_filepath]
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
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


br_cvm_fi_documentos_informe_diario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_fi_documentos_informe_diario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_cvm_fi_documentos_informe_diario.schedule = every_day_cvm


with Flow(
    name="br_cvm_fi_documentos_carteiras_fundos_investimento",
    code_owners=[
        "arthurfg",
    ],
) as br_cvm_fi_documentos_carteiras_fundos_investimento:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=False)
    table_id = Parameter(
        "table_id", default="documentos_carteiras_fundos_investimento", required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    url = Parameter(
        "url",
        default=cvm_constants.CDA_URL.value,
        required=True,
    )

    df = extract_links_and_dates(url)
    arquivos = check_for_updates(df, upstream_tasks=[df])

    with case(is_empty(arquivos), True):
        log(f"Não houveram atualizações em {url.default}!")

    with case(is_empty(arquivos), False):
        input_filepath = download_unzip_csv(
            url=url, files=arquivos, upstream_tasks=[arquivos]
        )
        output_filepath = clean_data_make_partitions_cda(
            input_filepath, upstream_tasks=[input_filepath]
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
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


br_cvm_fi_documentos_informe_diario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_fi_documentos_informe_diario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_cvm_fi_documentos_informe_diario.schedule = every_day_cvm
