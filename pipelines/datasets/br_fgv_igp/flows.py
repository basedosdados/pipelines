"""
Flows for br_fgv_igp
"""

from datetime import datetime

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from schedules import every_month
from pipelines.datasets.br_fgv_igp.tasks import (
    IGP_DI_anual,
    IGP_DI_mensal,
    IGP_10_mensal,
    IGP_OG_mensal,
    IGP_OG_anual,
    IGP_M_anual,
    IGP_M_mensal

)
from pipelines.constants import constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_metadata,
)
from pipelines.datasets.br_fgv_igp.schedules import every_year

with Flow("br_fgv_igp.igp_di_ano") as br_fgv_igp_di_ano:
    filepath =IGP_DI_anual()
    dataset_id = "br_fgv_igp"
    table_id = "igp_di_ano"

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # no generate temporal coverage since there is no date variable
    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )

br_fgv_igp_di_ano.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_fgv_igp_di_ano.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_fgv_igp_di_ano.schedule = every_month

with Flow("br_fgv_igp.igp_di_mes") as br_fgv_igp_di_mes:
    filepath =IGP_DI_mensal()
    dataset_id = "br_fgv_igp"
    table_id = "igp_di_mes"

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # no generate temporal coverage since there is no date variable
    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )

br_fgv_igp_di_mes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_fgv_igp_di_mes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_fgv_igp_di_mes.schedule = every_month

with Flow("br_fgv_igp.igp_10_mes") as br_fgv_igp_10_mes:
    filepath =IGP_10_mensal()
    dataset_id = "br_fgv_igp"
    table_id = "igp_10_mes"

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # no generate temporal coverage since there is no date variable
    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )
br_fgv_igp_10_mes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_fgv_igp_10_mes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_fgv_igp_10_mes.schedule = every_month

with Flow("br_fgv_igp.igp_m_mes") as br_fgv_igp_m_mes:
    filepath =IGP_M_mensal()
    dataset_id = "br_fgv_igp"
    table_id = "igp_m_mes"

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # no generate temporal coverage since there is no date variable
    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )
br_fgv_igp_m_mes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_fgv_igp_m_mes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_fgv_igp_m_mes.schedule = every_month

with Flow("br_fgv_igp.igp_m_ano") as br_fgv_igp_m_ano:
    filepath =IGP_M_anual()
    dataset_id = "br_fgv_igp"
    table_id = "igp_m_ano"

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # no generate temporal coverage since there is no date variable
    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )
br_fgv_igp_m_ano.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_fgv_igp_m_ano.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_fgv_igp_m_ano.schedule = every_month

with Flow("br_fgv_igp.igp_og_mes") as br_fgv_igp_og_mes:
    filepath =IGP_OG_mensal()
    dataset_id = "br_fgv_igp"
    table_id = "igp_og_mes"

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # no generate temporal coverage since there is no date variable
    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )

br_fgv_igp_og_mes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_fgv_igp_og_mes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_fgv_igp_og_mes.schedule = every_month

with Flow("br_fgv_igp.igp_og_ano") as br_fgv_igp_og_ano:
    filepath =IGP_OG_anual()
    dataset_id = "br_fgv_igp"
    table_id = "igp_og_ano"

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # no generate temporal coverage since there is no date variable
    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y/%m/%d")}}
        ],
        upstream_tasks=[wait_upload_table],
    )
br_fgv_igp_og_ano.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_fgv_igp_og_ano.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_fgv_igp_og_ano.schedule = every_month


