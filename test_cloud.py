from pipelines.datasets.br_bcb_estban.flows import (
    br_bcb_estban_agencia,
    br_bcb_estban_municipio,
)
from pipelines.utils.utils import run_cloud

run_cloud(
    br_bcb_estban_municipio,
    labels=[
        "basedosdados-dev",
    ],
    parameters={"dataset_id": "br_bcb_estban", "table_id": "municipio"},
)

run_cloud(
    br_bcb_estban_agencia,
    labels=[
        "basedosdados-dev",
    ],
    parameters={"dataset_id": "br_bcb_estban", "table_id": "agencia"},
)
