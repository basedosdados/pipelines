# -*- coding: utf-8 -*-
"""
Flows for mundo_transfermarkt_competicoes
"""

###############################################################################
#
# Aqui é onde devem ser definidos os flows do projeto.
# Cada flow representa uma sequência de passos que serão executados
# em ordem.
#
# Mais informações sobre flows podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/flows.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# Existem diversas maneiras de declarar flows. No entanto, a maneira mais
# conveniente e recomendada pela documentação é usar a API funcional.
# Em essência, isso implica simplesmente na chamada de funções, passando
# os parâmetros necessários para a execução em cada uma delas.
#
# Também, após a definição de um flow, para o adequado funcionamento, é
# mandatório configurar alguns parâmetros dele, os quais são:
# - storage: onde esse flow está armazenado. No caso, o storage é o
#   próprio módulo Python que contém o flow. Sendo assim, deve-se
#   configurar o storage como o pipelines.datasets
# - run_config: para o caso de execução em cluster Kubernetes, que é
#   provavelmente o caso, é necessário configurar o run_config com a
#   imagem Docker que será usada para executar o flow. Assim sendo,
#   basta usar constants.DOCKER_IMAGE.value, que é automaticamente
#   gerado.
# - schedule (opcional): para o caso de execução em intervalos regulares,
#   deve-se utilizar algum dos schedules definidos em schedules.py
#
# Um exemplo de flow, considerando todos os pontos acima, é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
# from prefect import Flow
# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
# from pipelines.constants import constants
# from my_tasks import my_task, another_task
# from my_schedules import some_schedule
#
# with Flow("my_flow") as flow:
#     a = my_task(param1=1, param2=2)
#     b = another_task(a, param3=3)
#
# flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow.schedule = some_schedule
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
from pipelines.datasets.mundo_transfermarkt_competicoes.constants import (
    constants as mundo_constants,
)
from pipelines.datasets.mundo_transfermarkt_competicoes.tasks import (
    make_partitions,
    get_max_data,
    execucao_coleta_sync,
)
from pipelines.datasets.mundo_transfermarkt_competicoes.utils import execucao_coleta
from pipelines.datasets.mundo_transfermarkt_competicoes.schedules import every_week
from pipelines.pipelines.utils.metadata.tasks import get_today_date
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
    update_django_metadata,
)
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.decorators import Flow
from prefect import Parameter, case
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run,
)
from pipelines.utils.constants import constants as utils_constants
from datetime import timedelta
import asyncio

with Flow(
    name="mundo_transfermarkt_competicoes.brasileirao_serie_a",
    code_owners=[
        "Gabs",
    ],
) as transfermarkt_brasileirao_flow:
    dataset_id = Parameter(
        "dataset_id", default="mundo_transfermarkt_competicoes", required=True
    )
    table_id = Parameter("table_id", default="brasileirao_serie_a", required=True)
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
    df = execucao_coleta_sync(execucao_coleta)
    output_filepath = make_partitions(df, upstream_tasks=[df])
    data_maxima = get_max_data()

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
            run_name=r"Materialize {dataset_id}.{table_id}",
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

        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            _last_date=data_maxima,
            bq_table_last_year_month=False,
            bq_last_update=False,
            is_bd_pro=True,
            is_free=True,
            time_delta=6,
            time_unit="weeks",
            date_format="yy-mm-dd",
            api_mode="prod",
        )

transfermarkt_brasileirao_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
transfermarkt_brasileirao_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
transfermarkt_brasileirao_flow.schedule = every_week
