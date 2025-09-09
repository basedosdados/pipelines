"""
Flows for br_sfb_sicar
"""

from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_sfb_sicar.constants import (
    Constants,
)
from pipelines.datasets.br_sfb_sicar.schedules import (
    schedule_br_sfb_sicar_area_imovel,
)
from pipelines.datasets.br_sfb_sicar.tasks import (
    download_car,
    get_each_uf_release_date,
    unzip_to_parquet,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import (
    constants as dump_db_constants,
)
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

INPUTPATH = Constants.INPUT_PATH.value
OUTPUTPATH = Constants.OUTPUT_PATH.value
SIGLAS_UF = Constants.UF_SIGLAS.value


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
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "target": target,
                "dbt_alias": dbt_alias,
                "dbt_command": "run/test",
                "disable_elementary": False,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
            upstream_tasks=[wait_upload_table],
        )

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
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
                upstream_tasks=[wait_for_materialization],
            )


br_sfb_sicar_area_imovel.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_sfb_sicar_area_imovel.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_sfb_sicar_area_imovel.schedule = schedule_br_sfb_sicar_area_imovel
