# -*- coding: utf-8 -*-
"""
Flows for br_ons_avaliacao_operacao
"""
# pylint: disable=invalid-name
from datetime import datetime, timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_ons_avaliacao_operacao.tasks import (
    download_data,
    wrang_data,
)
from pipelines.datasets.br_ons_avaliacao_operacao.constants import (
    constants as ons_constants,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    update_metadata,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
    create_table_and_upload_to_gcs,
)

# from pipelines.datasets.br_ons_avaliacao_operacao.schedules import (
#    schedule_municipio_exportacao,
#    schedule_municipio_importacao,
#    schedule_ncm_exportacao,
#    schedule_ncm_importacao,
# )


with Flow(
    name="br_ons_avaliacao_operacao.reservatorio", code_owners=["Gabriel Pisa"]
) as br_ons_avaliacao_operacao_reservatorio:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter("table_id", default="reservatorio", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[0],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[0],
        upstream_tasks=[dow_data],
    )

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

br_ons_avaliacao_operacao_reservatorio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ons_avaliacao_operacao_reservatorio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    name="br_ons_avaliacao_operacao.geracao_usina", code_owners=["Gabriel Pisa"]
) as br_ons_avaliacao_operacao_geracao_usina:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter("table_id", default="geracao_usina", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[1],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[1],
        upstream_tasks=[dow_data],
    )

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

br_ons_avaliacao_operacao_geracao_usina.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ons_avaliacao_operacao_geracao_usina.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    name="br_ons_avaliacao_operacao.geracao_termica_motivo_despacho",
    code_owners=["Gabriel Pisa"],
) as br_ons_avaliacao_operacao_geracao_termica_motivo_despacho:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter(
        "table_id", default="geracao_termica_motivo_despacho", required=True
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[2],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[2],
        upstream_tasks=[dow_data],
    )

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

br_ons_avaliacao_operacao_geracao_termica_motivo_despacho.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_avaliacao_operacao_geracao_termica_motivo_despacho.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    name="br_ons_avaliacao_operacao.energia_natural_afluente",
    code_owners=["Gabriel Pisa"],
) as br_ons_avaliacao_operacao_energia_natural_afluente:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter("table_id", default="energia_natural_afluente", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[3],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[3],
        upstream_tasks=[dow_data],
    )

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

br_ons_avaliacao_operacao_energia_natural_afluente.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_avaliacao_operacao_energia_natural_afluente.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

with Flow(
    name="br_ons_avaliacao_operacao.energia_armazenada_reservatorio_dia",
    code_owners=["Gabriel Pisa"],
) as br_ons_energia_armazenada_reservatorio_dia:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter(
        "table_id", default="energia_armazenada_reservatorio_dia", required=True
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[4],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[4],
        upstream_tasks=[dow_data],
    )

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

br_ons_energia_armazenada_reservatorio_dia.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_energia_armazenada_reservatorio_dia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
