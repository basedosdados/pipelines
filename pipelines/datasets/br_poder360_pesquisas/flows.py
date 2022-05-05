"""
Flows for br_poder360_pesquisas
"""

from datetime import datetime

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_poder360_pesquisas.tasks import crawler
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_metadata,
    get_temporal_coverage,
    publish_table,
)
from pipelines.datasets.br_poder360_pesquisas.schedules import every_monday_thursday

# pylint: disable=C0103
with Flow("br_poder360_pesquisas.microdados") as br_poder360:
    dataset_id = "br_poder360_pesquisas"
    table_id = "microdados"

    filepath = crawler()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["data"],
        time_unit="year",
        interval="1",
        upstream_tasks=[wait_upload_table],
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            {"temporal_coverage": [temporal_coverage]},
        ],
        upstream_tasks=[temporal_coverage],
    )

    publish_table(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        wait=wait_update_metadata,
    )

br_poder360.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_poder360.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_poder360.schedule = every_monday_thursday
