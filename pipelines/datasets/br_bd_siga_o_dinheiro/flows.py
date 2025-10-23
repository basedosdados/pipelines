"""
Flows for br_jota
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_bd_siga_o_dinheiro.tasks import get_table_ids
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import download_data_to_gcs, run_dbt

with Flow(
    name="BD template - br_bd_siga_o_dinheiro", code_owners=["luiz"]
) as flow:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_siga_o_dinheiro", required=True
    )

    target = Parameter("target", default="prod", required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    table_ids = get_table_ids()

    for _, table_id in enumerate(table_ids):
        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[table_ids],
        )
        wait_for_dowload_data_to_gcs = download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[wait_for_materialization],
        )


flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_bd_siga_o_dinheiro.schedule = schedule_br_bd_siga_o_dinheiro
