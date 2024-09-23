# -*- coding: utf-8 -*-
"""
Flows for br_sfb_car
"""

# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_sfb_car.constants import Constants as car_constants
#rom pipelines.datasets.br_sfb_car.schedules import schedule_br_sfb_car_imoveis_rurais
from pipelines.datasets.br_sfb_car.tasks import download_car, unzip_to_parquet
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)
inputpath = car_constants.INPUT_PATH.value
outputpath = car_constants.OUTPUT_PATH.value

with Flow(
    name="br_sfb_car.area_imovel", code_owners=["Gabriel Pisa"]
) as br_sfb_car_area_imovel:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_sfb_car", required=True)
    table_id = Parameter("table_id", default="area_imovel", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )


    download_polygons = download_car(
        inputpath=inputpath,
        outputpath=outputpath
    )

    unzip_from_shp_to_parquet_wkt = unzip_to_parquet(
        inputpath=inputpath,
        outputpath=outputpath,
        upstream_tasks=[download_polygons]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=outputpath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=unzip_from_shp_to_parquet_wkt,
    )


br_sfb_car_area_imovel.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_sfb_car_area_imovel.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
#br_sfb_car_area_imovel.schedule = schedule_br_sfb_car_area_imovel
