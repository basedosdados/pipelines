# -*- coding: utf-8 -*-


from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants as pipelines_constants
from pipelines.datasets.br_denatran_frota.constants import constants
from pipelines.datasets.br_denatran_frota.tasks import (
    crawl_task,
    get_desired_file_task,
    get_latest_data_task,
    output_file_to_parquet_task,
    treat_municipio_tipo_task,
    treat_uf_tipo_task,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_denatran_frota.uf_tipo",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_denatran_frota_uf_tipo:
    dataset_id = Parameter("dataset_id", default="br_denatran_frota")
    table_id = Parameter("table_id", default="uf_tipo")

    # Materialization mode
    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    year_to_fetch = get_latest_data_task(
        table_id="uf_tipo", dataset_id=dataset_id
    )
    # search for most recent year in the API

    crawled = crawl_task(
        month=year_to_fetch[1],
        year=year_to_fetch[0],
        temp_dir=constants.DOWNLOAD_PATH.value,
        upstream_tasks=[year_to_fetch],
    )

    with case(crawled, False):
        log_task("No new data to be downloaded")

    with case(crawled, True):
        # Now get the downloaded file:
        # Used primarly to backfill data
        desired_file = get_desired_file_task(
            year=year_to_fetch[0],
            download_directory=constants.DOWNLOAD_PATH.value,
            filetype=constants.UF_TIPO_BASIC_FILENAME.value,
            upstream_tasks=[crawled],
        )

        df = treat_uf_tipo_task(
            file=desired_file, upstream_tasks=[desired_file]
        )

        parquet_output = output_file_to_parquet_task(
            df, constants.UF_TIPO_BASIC_FILENAME.value, upstream_tasks=[df]
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=parquet_output,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=parquet_output,
        )

        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
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
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="all_free",
                    # time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )


br_denatran_frota_uf_tipo.storage = GCS(
    pipelines_constants.GCS_FLOWS_BUCKET.value
)
br_denatran_frota_uf_tipo.run_config = KubernetesRun(
    image=pipelines_constants.DOCKER_IMAGE.value
)
# br_denatran_frota_uf_tipo.schedule = every_month_uf


with Flow(
    name="br_denatran_frota.municipio_tipo",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_denatran_frota_municipio_tipo:
    dataset_id = Parameter("dataset_id", default="br_denatran_frota")
    table_id = Parameter("table_id", default="municipio_tipo")

    # Materialization mode
    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    year_to_fetch = get_latest_data_task(
        table_id="municipio_tipo", dataset_id=dataset_id
    )

    crawled = crawl_task(
        month=year_to_fetch[1],
        year=year_to_fetch[0],
        temp_dir=constants.DOWNLOAD_PATH.value,
        upstream_tasks=[year_to_fetch],
    )

    with case(crawled, False):
        log_task("There's no new data to be downloaded")

    with case(crawled, True):
        # Now get the downloaded file:
        desired_file = get_desired_file_task(
            year=year_to_fetch[0],
            download_directory=constants.DOWNLOAD_PATH.value,
            filetype=constants.MUNIC_TIPO_BASIC_FILENAME.value,
            upstream_tasks=[crawled],
        )

        df = treat_municipio_tipo_task(
            file=desired_file, upstream_tasks=[desired_file]
        )

        parquet_output = output_file_to_parquet_task(
            df, constants.MUNIC_TIPO_BASIC_FILENAME.value, upstream_tasks=[df]
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=parquet_output,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=parquet_output,
        )

        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
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
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="all_free",
                    # time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )


br_denatran_frota_municipio_tipo.storage = GCS(
    pipelines_constants.GCS_FLOWS_BUCKET.value
)
br_denatran_frota_municipio_tipo.run_config = KubernetesRun(
    image=pipelines_constants.DOCKER_IMAGE.value
)
# br_denatran_frota_municipio_tipo.schedule = every_month_municipio
