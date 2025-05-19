from pipelines.datasets.new_arch_pipelines.flows import (
    new_arch_pipeline,
)
from pipelines.utils.utils import run_local

run_local(
    flow=new_arch_pipeline,
    parameters={
        "dataset_id": "new_arch_pipeline",
        "table_id": "new_arch_pipeline",
        "materialization_mode": "dev",
    },
    # labels = ["basedosdados"]
)
