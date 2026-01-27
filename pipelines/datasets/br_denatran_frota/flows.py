from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants as pipelines_constants
from pipelines.datasets.br_denatran_frota.constants import (
    constants as denatran_constants,
)
from pipelines.datasets.br_denatran_frota.schedules import (
    every_month_municipio,
    every_month_uf,
)
from pipelines.datasets.br_denatran_frota.tasks import (
    crawl_task,
    get_desired_file_task,
    get_latest_date_task,
    treat_municipio_tipo_task,
    treat_uf_tipo_task,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_denatran_frota.uf_tipo",
    code_owners=["Gabriel Pisa", "Luiza"],
) as br_denatran_frota_uf_tipo:
    dataset_id = Parameter(
        "dataset_id", default="br_denatran_frota", required=True
    )
    table_id = Parameter("table_id", default="uf_tipo", required=True)

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    (
        source_available_dates,
        source_available_dates_str,
        source_first_available_date,
        source_first_available_date_str,
    ) = get_latest_date_task(table_id=table_id, dataset_id=dataset_id)

    log_task(source_first_available_date_str)
    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_first_available_date_str,
        date_format="%Y-%m",
        upstream_tasks=[source_first_available_date_str],
    )

    with case(check_if_outdated, False):
        log_task("No new data to be downloaded")

    with case(check_if_outdated, True):
        log_task("Updates found! The run will be started.")
        crawled = crawl_task.map(
            source_max_date=source_available_dates,
            table_id=unmapped(table_id),
            temp_dir=unmapped(denatran_constants.DOWNLOAD_PATH.value),
            upstream_tasks=[unmapped(check_if_outdated)],
        )
        # Used primarly to backfill data
        desired_file = get_desired_file_task.map(
            source_max_date=source_available_dates,
            download_directory=unmapped(
                denatran_constants.DOWNLOAD_PATH.value
            ),
            table_id=unmapped(table_id),
            filetype=unmapped(denatran_constants.UF_TIPO_BASIC_FILENAME.value),
            upstream_tasks=[unmapped(crawled)],
        )

        parquet_output = treat_uf_tipo_task.map(
            file=desired_file, upstream_tasks=[unmapped(desired_file)]
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=parquet_output[0],
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[parquet_output],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=parquet_output[0],
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )

br_denatran_frota_uf_tipo.storage = GCS(
    pipelines_constants.GCS_FLOWS_BUCKET.value
)
br_denatran_frota_uf_tipo.run_config = KubernetesRun(
    image=pipelines_constants.DOCKER_IMAGE.value
)
br_denatran_frota_uf_tipo.schedule = every_month_uf


with Flow(
    name="br_denatran_frota.municipio_tipo",
    code_owners=["Gabriel Pisa", "Luiza"],
) as br_denatran_frota_municipio_tipo:
    dataset_id = Parameter(
        "dataset_id", default="br_denatran_frota", required=True
    )
    table_id = Parameter("table_id", default="municipio_tipo", required=True)

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    (
        source_available_dates,
        source_available_dates_str,
        source_first_available_date,
        source_first_available_date_str,
    ) = get_latest_date_task(table_id=table_id, dataset_id=dataset_id)

    log_task(source_first_available_date_str)
    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_first_available_date_str,
        date_format="%Y-%m",
        upstream_tasks=[source_first_available_date_str],
    )

    with case(check_if_outdated, False):
        log_task("No new data to be downloaded")

    with case(check_if_outdated, True):
        log_task("Updates found! The run will be started.")
        crawled = crawl_task.map(
            source_max_date=source_available_dates,
            table_id=unmapped(table_id),
            temp_dir=unmapped(denatran_constants.DOWNLOAD_PATH.value),
            upstream_tasks=[unmapped(check_if_outdated)],
        )
        # Used primarly to backfill data
        desired_file = get_desired_file_task.map(
            source_max_date=source_available_dates,
            download_directory=unmapped(
                denatran_constants.DOWNLOAD_PATH.value
            ),
            table_id=unmapped(table_id),
            filetype=unmapped(
                denatran_constants.MUNIC_TIPO_BASIC_FILENAME.value
            ),
            upstream_tasks=[unmapped(crawled)],
        )

        parquet_output = treat_municipio_tipo_task.map(
            file=desired_file, upstream_tasks=[unmapped(desired_file)]
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=parquet_output[0],
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[unmapped(parquet_output)],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=parquet_output[0],
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[unmapped(wait_for_materialization)],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )


br_denatran_frota_municipio_tipo.storage = GCS(
    pipelines_constants.GCS_FLOWS_BUCKET.value
)
br_denatran_frota_municipio_tipo.run_config = KubernetesRun(
    image=pipelines_constants.DOCKER_IMAGE.value
)
br_denatran_frota_municipio_tipo.schedule = every_month_municipio
