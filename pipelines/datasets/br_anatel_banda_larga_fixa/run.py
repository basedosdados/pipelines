from pipelines.utils.utils import run_cloud
from pipelines.datasets.br_anatel_banda_larga_fixa.tasks import microdados

run_cloud(
    microdados,               # O flow que você deseja executar
    labels=[
        "basedosdados-dev",      # Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters = {
        "materialize_after_dump": "True", # Parâmetros que serão passados para a pipeline (opcional)
    }
)