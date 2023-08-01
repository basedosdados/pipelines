# -*- coding: utf-8 -*-
"""
Flows for br_mp_pep_cargos_funcoes
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

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)
from pipelines.utils.utils import run_local, log
from pipelines.utils.decorators import Flow

# from pipelines.datasets.br_mp_pep_cargos_funcoes.schedules import every_two_weeks
from pipelines.datasets.br_mp_pep_cargos_funcoes.tasks import (
    setup_web_driver,
    scraper,
    clean_data,
)

with Flow(
    name="br_mp_pep.cargos_funcoes",
    code_owners=[
        "aspeddro",
    ],
) as datasets_br_mp_pep_cargos_funcoes_flow:
    dataset_id = Parameter("dataset_id", default="br_mp_pep", required=True)
    table_id = Parameter("table_id", default="cargos_funcoes", required=True)

    setup_web_driver()

    scraper(year_start=2022, year_end=2023)

    data_path = clean_data()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=data_path,
    )


run_local(datasets_br_mp_pep_cargos_funcoes_flow)
# datasets_br_mp_pep_cargos_funcoes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# datasets_br_mp_pep_cargos_funcoes_flow.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value
# )
# flow.schedule = every_two_weeks
