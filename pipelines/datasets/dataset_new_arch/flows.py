# from datetime import timedelta

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.dataset_new_arch.tasks import (
    create_dataframe,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    # create_table_and_upload_to_gcs_prod,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="dataset_new_arch.tabela_new_arch",
    code_owners=[
        "aspeddro",
    ],
) as flow:
    dataset_id = Parameter(
        "dataset_id",
        default="dataset_new_arch",
        required=False,
    )

    table_id = Parameter("table_id", default="tabela_new_arch", required=False)

    target = Parameter("target", default="prod", required=False)

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    data_path = create_dataframe(upstream_tasks=[rename_flow_run])

    wait_upload_table_dev = create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=[data_path],
    )

    wait_upload_table_prod = create_table_and_upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=[wait_upload_table_dev],
    )

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
