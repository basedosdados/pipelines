"""
Flows for br_me_novo_caged
"""
# pylint: disable=invalid-name
from prefect.storage import GCS
from prefect import Parameter
from prefect.run_configs import KubernetesRun

from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.datasets.br_me_caged.tasks import build_partitions, get_caged_data
from pipelines.utils.tasks import create_table_and_upload_to_gcs
from pipelines.datasets.br_me_caged.schedules import every_month

with Flow("br_me_caged.microdados_mov", code_owners=["lucas_cr"]
) as cagedmov:
    dataset_id = Parameter("dataset_id", default="br_me_caged", required=True)
    table_id = Parameter("table_id", default="microdados_movimentacao", required=True)
    year = Parameter("year", default=2022, required=True)

    get_data=get_caged_data(table_id=table_id, year=year)

    filepath = build_partitions(table_id=table_id, upstream_tasks=[get_data])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

cagedmov.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cagedmov.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
cagedmov.schedule = every_month
