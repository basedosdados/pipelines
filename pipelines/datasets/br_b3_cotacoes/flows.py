# -*- coding: utf-8 -*- dataset
"""
Flows for br_b3_cotacoes dataset
"""

from datetime import timedelta
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.utils.tasks import update_django_metadata
from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.datasets.br_b3_cotacoes.tasks import tratamento, get_today_date

from pipelines.utils.utils import (
    log,
)

from pipelines.datasets.br_b3_cotacoes.schedules import (
    all_day_cotacoes,
)

from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

with Flow(name="br_b3_cotacoes.cotacoes", code_owners=["trick"]) as cotacoes:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_b3_cotacoes", required=True)
    table_id = Parameter("table_id", default="cotacoes", required=True)

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    # ! a variável delta_day é criada aqui e cria um objeto 'Parameter' no Prefect Cloud chamado delta_day

    delta_day = Parameter("delta_day", default=1, required=False)
    # ! a variável filepath é criada aqui e é passado o parâmetro 'delta_day', sendo ele mesmo o valor.

    # ! upstream_tasks=[rename_flow_run] significa que o task 'rename_flow_run' será executado antes do 'tratamento'

    # ? Importante para o Prefect saber a ordem de execução dos tasks

    filepath = tratamento(delta_day=delta_day, upstream_tasks=[rename_flow_run])

    # pylint: disable=C0103
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )

cotacoes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cotacoes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
cotacoes.schedule = all_day_cotacoes
