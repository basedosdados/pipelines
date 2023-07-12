# -*- coding: utf-8 -*-
"""
Flows for br_anatel_telefonia_movel
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from datetime import timedelta
from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.constants import constants
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_telefonia_movel.tasks import (
    clean_csv_microdados,
    clean_csv_brasil,
    download,
    # clean_csv_uf,
    # clean_csv_municipio,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

from pipelines.datasets.br_anatel_telefonia_movel.schedules import (
    every_month_anatel,
    # every_month_anatel_municipio,
    # every_month_anatel_uf,
    # every_month_anatel_brasil,
)

with Flow(name="br_anatel_telefonia_movel", code_owners=["tricktx"]) as br_anatel:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_anatel_telefonia_movel", required=True
    )
    table_id = Parameter(
        "table_id", default=["microdados", "brasil"], required=True
    )  # Table_id Microdados

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id[0],
        wait=table_id[0],
    )

    # ! as variáveis ano, mes_um, mes_dois é criada aqui e cria um objeto 'Parameter' no Prefect Cloud chamado 'ano', 'mes_um', 'mes_dois'
    # Importante salientar que o mes_um sempre será 01 ou 06 e o mes_dois será 07 ou 12
    anos = Parameter("anos", default=2023, required=True)
    mes_um = Parameter("mes_um", default="01", required=True)
    mes_dois = Parameter("mes_dois", default="06", required=True)
    download_file = download()
    filepath_microdados = clean_csv_microdados(
        anos=anos,
        mes_um=mes_um,
        mes_dois=mes_dois,
        input_path=download_file
        upstream_tasks=[rename_flow_run],
    )

    # MICRODADOS
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath_microdados,
        dataset_id=dataset_id,
        table_id=table_id[0],
        dump_mode="append",
        wait=filepath_microdados,
    )
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id[0],
                "mode": materialization_mode,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id[0]}",
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
    download_file = download()
    filepath_brasil = clean_csv_brasil(input_path=download_file,
                                       upstream_tasks=[filepath_microdados])

    # BRASIL
    wait_upload_table_BRASIL = create_table_and_upload_to_gcs(
        data_path=filepath_brasil,
        dataset_id=dataset_id,
        table_id=table_id[1],
        dump_mode="append",
        wait=filepath_brasil,
    )
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id[1],
                "mode": materialization_mode,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id[1]}",
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

br_anatel.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_anatel.schedule = every_month_anatel

"""
with Flow(
    name="br_anatel_telefonia_movel", code_owners=["tricktx"]
) as br_anatel_brasil:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_anatel_telefonia_movel", required=True
    )
    table_id = Parameter("table_id", default="densidade_brasil", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    filepath = clean_csv_brasil()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
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

br_anatel_brasil.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_brasil.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_brasil.schedule = every_month_anatel_brasil


with Flow(name="br_anatel_telefonia_movel", code_owners=["tricktx"]) as br_anatel_uf:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_anatel_telefonia_movel", required=True
    )
    table_id = Parameter("table_id", default="densidade_uf", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    filepath = clean_csv_uf()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
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

br_anatel_uf.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_uf.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_uf.schedule = every_month_anatel_uf


with Flow(
    name="br_anatel_telefonia_movel", code_owners=["tricktx"]
) as br_anatel_municipio:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_anatel_telefonia_movel", required=True
    )
    table_id = Parameter("table_id", default="densidade_municipio", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    filepath = clean_csv_municipio()

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

br_anatel_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel_municipio.schedule = every_month_anatel_municipio"""
