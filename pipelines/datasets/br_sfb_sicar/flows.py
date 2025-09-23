# -*- coding: utf-8 -*-
"""
Flows for br_sfb_sicar
"""

# pylint: disable=invalid-name

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_sfb_sicar.constants import (
    Constants as car_constants,
)
from pipelines.datasets.br_sfb_sicar.schedules import (
    schedule_br_sfb_sicar_area_imovel,
)
from pipelines.datasets.br_sfb_sicar.tasks import (
    download_car,
    get_each_uf_release_date,
    unzip_to_parquet,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

INPUTPATH = car_constants.INPUT_PATH.value
OUTPUTPATH = car_constants.OUTPUT_PATH.value
SIGLAS_UF = car_constants.UF_SIGLAS.value


with Flow(
    name="br_sfb_sicar.area_imovel", code_owners=["Gabriel Pisa"]
) as br_sfb_sicar_area_imovel:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_sfb_sicar", required=True)
    table_id = Parameter("table_id", default="area_imovel", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    download_polygons = download_car.map(
        inputpath=unmapped(INPUTPATH),
        outputpath=unmapped(OUTPUTPATH),
        sigla_uf=SIGLAS_UF,
        polygon=unmapped("AREA_IMOVEL"),
    )

    ufs_release_dates = get_each_uf_release_date(
        upstream_tasks=[download_polygons]
    )

    unzip_from_shp_to_parquet_wkt = unzip_to_parquet(
        inputpath=INPUTPATH,
        outputpath=OUTPUTPATH,
        uf_relase_dates=ufs_release_dates,
        upstream_tasks=[download_polygons, ufs_release_dates],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=OUTPUTPATH,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        source_format="parquet",
        wait=unzip_from_shp_to_parquet_wkt,
    )

    with case(materialize_after_dump, True):
        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_alias=dbt_alias,
            dbt_command="run/test",
            disable_elementary=False,
            upstream_tasks=[wait_upload_table],
        )

        wait_for_dowload_data_to_gcs = download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[wait_for_materialization],
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={
                    "date": "data_extracao",
                },
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )


br_sfb_sicar_area_imovel.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_sfb_sicar_area_imovel.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_sfb_sicar_area_imovel.schedule = schedule_br_sfb_sicar_area_imovel
