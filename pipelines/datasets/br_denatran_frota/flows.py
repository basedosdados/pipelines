# -*- coding: utf-8 -*-
"""
Flows for br_denatran_frota
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

from datetime import datetime, timedelta
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Parameter, case, Flow
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run,
)

from pipelines.constants import constants as pipelines_constants
from pipelines.utils.constants import constants as utils_constants

# from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)
from pipelines.datasets.br_denatran_frota.tasks import crawl_task, treat_uf_tipo_task
from pipelines.datasets.br_denatran_frota.constants import constants

download_path = constants.DOWNLOAD_PATH.value
# from pipelines.datasets.br_denatran_frota.schedules import every_two_weeks

with Flow(
    name="br_denatran_frota.uf_tipo",
    # code_owners=[
    #     "Tamir",
    # ],
) as br_denatran_frota_uf_tipo:
    # dataset_id = Parameter("dataset_id", default="br_denatran_frota")
    # table_id = Parameter("table_id", default="uf_tipo")

    # # Materialization mode
    # materialization_mode = Parameter(
    #     "materialization_mode", default="dev", required=False
    # )

    # materialize_after_dump = Parameter(
    #     "materialize after dump", default=False, required=False
    # )

    # dbt_alias = Parameter("dbt_alias", default=True, required=False)

    # rename_flow_run = rename_current_flow_run_dataset_table(
    #     prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    # )
    crawled = crawl_task(
        month=2, year=2021, temp_dir=download_path
    )  # Download the desired files.
    uf_tipo_file = "/home/tamir/basedosdados/pipelines/pipelines/datasets/br_denatran_frota/tmp/input/files/2021/frota_por_uf_e_tipo_de_veículo_2-2021.xls"
    df = treat_uf_tipo_task(file=uf_tipo_file, upstream_tasks=[crawled])

    print("foi?")

br_denatran_frota_uf_tipo.storage = GCS(pipelines_constants.GCS_FLOWS_BUCKET.value)
br_denatran_frota_uf_tipo.run_config = KubernetesRun(
    image=pipelines_constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks
