from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_bcb_agencia.constants import (
    constants as agencia_constants,
)
from pipelines.datasets.br_bcb_agencia.schedules import every_month_agencia
from pipelines.datasets.br_bcb_agencia.tasks import (
    clean_data,
    download_table,
    extract_urls_list,
    get_api_max_date,
    get_documents_metadata,
    get_latest_file,
    raise_none_metadata_exception,
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
    name="br_bcb_agencia.agencia",
    code_owners=[
        "Luiza",
    ],
) as br_bcb_agencia_agencia:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bcb_agencia", required=True
    )
    table_id = Parameter("table_id", default="agencia", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    documents_metadata = get_documents_metadata()

    # Checking if metadata was loaded
    with case(documents_metadata is None, False):
        data_source_download_url, data_source_max_date = get_latest_file(
            documents_metadata
        )

        # Checking if production data is up to date
        check_if_outdated = check_if_data_is_outdated(
            dataset_id=dataset_id,
            table_id=table_id,
            data_source_max_date=data_source_max_date,
            date_format="%Y-%m",
            upstream_tasks=[data_source_max_date],
        )

        with case(check_if_outdated, False):
            log_task(f"No updates for table {table_id}!")

        with case(check_if_outdated, True):
            log_task("Updates found! The run will be started.")
            api_max_date = get_api_max_date(dataset_id, table_id)

            urls_list = extract_urls_list(
                documents_metadata,
                data_source_max_date,
                api_max_date,
                date_format="%Y-%m",
            )
            downloaded_file_paths = download_table.map(
                url=urls_list,
                download_dir=unmapped(
                    agencia_constants.ZIPFILE_PATH_AGENCIA.value
                ),
                upstream_tasks=[unmapped(check_if_outdated)],
            )

            filepath = clean_data(
                upstream_tasks=[downloaded_file_paths],
            )

            wait_upload_table = create_table_dev_and_upload_to_gcs(
                data_path=filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[filepath],
            )

            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_alias=dbt_alias,
                dbt_command="run/test",
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )

            # agencia
            with case(materialize_after_dump, True):
                wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                    data_path=filepath,
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
    with case(documents_metadata is None, True):
        raise_none_metadata_exception(
            "BCB metadata was not loaded! It was not possible to determine if the dataset is up to date."
        )
br_bcb_agencia_agencia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_agencia_agencia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_agencia_agencia.schedule = every_month_agencia
