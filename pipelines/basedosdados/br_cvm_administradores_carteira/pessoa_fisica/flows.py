"""
Flows for basedosdados
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
#   configurar o storage como o pipelines.basedosdados
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


from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.basedosdados.br_cvm_administradores_carteira.pessoa_fisica.tasks import (
    download_url,
    unzip,
    treat,
    dataframe_to_csv,
    upload_to_gcs,
)
from pipelines.basedosdados.br_cvm_administradores_carteira.pessoa_fisica.schedules import (
    every_day,
)

with Flow("br_cvm_oferta_publica_distribuicao__pessoa_fisica") as flow:

    path = "temp"
    out_path = "temp_folder"
    final_path = "output"
    raw_table_name = "cad_adm_cart_pf.csv"

    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    url = Parameter("url")

    path = download_url(url, path)
    out_path = unzip(path, out_path)

    data = treat(out_path, raw_table_name)

    path = dataframe_to_csv(df=data, path=final_path, filename=table_id)

    upload_to_gcs(path=path, dataset_id=dataset_id, table_id=table_id)

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
