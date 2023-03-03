from pipelines.utils.utils import run_cloud
from pipelines.datasets.test_lauris.flows import bd_available_options # Complete com as infos da sua pipeline

run_cloud(
    bd_available_options,               # O flow que você deseja executar
    labels=[
        "basedosdados-dev",      # Label para identificar o agente que irá executar a pipeline (ex: basedosdados-dev)
    ],
    parameters = dict(
        materialize_after_dump = True
    )
)