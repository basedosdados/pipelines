# -*- coding: utf-8 -*-
"""
Flows for br_jota
"""
from datetime import timedelta

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.datasets.br_jota.schedules import (
    schedule_candidatos,
    schedule_contas_candidato,
    schedule_contas_candidato_origem,
)
from pipelines.utils.tasks import get_current_flow_labels


with Flow(
    name="br_jota.eleicao_perfil_candidato_2022", code_owners=["lauris"]
) as eleicao_perfil_candidato_2022:
    dataset_id = Parameter("dataset_id", default="br_jota", required=True)
    table_id = Parameter(
        "table_id", default="eleicao_perfil_candidato_2022", required=True
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

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

eleicao_perfil_candidato_2022.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
eleicao_perfil_candidato_2022.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
eleicao_perfil_candidato_2022.schedule = schedule_candidatos



with Flow(
    name="br_jota.eleicao_prestacao_contas_candidato_2022", code_owners=["lauris"]
) as eleicao_prestacao_contas_candidato_2022:
    dataset_id = Parameter("dataset_id", default="br_jota", required=True)
    table_id = Parameter(
        "table_id", default="eleicao_prestacao_contas_candidato_2022", required=True
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

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

eleicao_prestacao_contas_candidato_2022.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
eleicao_prestacao_contas_candidato_2022.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
eleicao_prestacao_contas_candidato_2022.schedule = schedule_contas_candidato


with Flow(
    name="br_jota.eleicao_prestacao_contas_candidato_origem_2022", code_owners=["lauris"]
) as eleicao_prestacao_contas_candidato_origem_2022:
    dataset_id = Parameter("dataset_id", default="br_jota", required=True)
    table_id = Parameter(
        "table_id", default="eleicao_prestacao_contas_candidato_origem_2022", required=True
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

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

eleicao_prestacao_contas_candidato_origem_2022.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
eleicao_prestacao_contas_candidato_origem_2022.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
eleicao_prestacao_contas_candidato_origem_2022.schedule = schedule_contas_candidato_origem
