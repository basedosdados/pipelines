"""
Flows for br_sp_saopaulo_dieese_icv
"""
# pylint: disable=C0103, E1123

from datetime import datetime

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_sp_saopaulo_dieese_icv.tasks import (
    clean_dieese_icv,
)

from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_metadata,
    publish_table,
)
from pipelines.datasets.br_sp_saopaulo_dieese_icv.schedules import every_month

with Flow("br_sp_saopaulo_dieese_icv.mes") as br_sp_dieese:
    dataset_id = "br_sp_saopaulo_dieese_icv"
    table_id = "mes"
    filepath = clean_dieese_icv()  # igual minha funcao de tratamento

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"metadata": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )

    publish_table(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        wait=wait_update_metadata,
    )

br_sp_dieese.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_sp_dieese.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_sp_dieese.schedule = every_month
