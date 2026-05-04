from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.bcb.tasks import (
    create_load_dictionary,
    download_table,
    get_sicor_table_size,
    search_sicor_links,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated_by_size,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="BD template - BR_BCB_SICOR",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_sicor_template:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_bcb_sicor", required=True)
    table_id = Parameter("table_id", default="operacao", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    append_overwrite = Parameter("append_overwrite", default="overwrite")
    source_format = Parameter("source_format", default="parquet")

    download_all_files = Parameter(
        "download_all_files", default=False, required=False
    )
    local_redis_execution = Parameter(
        "local_redis_execution", default=False, required=False
    )
    historical_database = Parameter(
        "historical_database", default=True, required=False
    )
    coverage_type = Parameter(
        "coverage_type", default="part_bdpro", required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    download_links = search_sicor_links()

    table_size = get_sicor_table_size(
        download_links,
        table_id=table_id,
        download_all_files=download_all_files,
        upstream_tasks=[download_links],
    )

    is_outdated = check_if_data_is_outdated_by_size(
        dataset_id=dataset_id,
        table_id=table_id,
        byte_length=table_size,
        local_execution=local_redis_execution,
        upstream_tasks=[table_size],
    )

    with case(is_outdated, True):
        download_table_task = download_table(
            download_links,
            table_id=table_id,
            download_all_files=download_all_files,
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=download_table_task,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=append_overwrite,
            upstream_tasks=[download_table_task],
            source_format=source_format,
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
                data_path=download_table_task,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode=append_overwrite,
                source_format=source_format,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={
                        "year": "ano_emissao",
                        "month": "mes_emissao",
                    },
                    date_format="%Y-%m",
                    coverage_type=coverage_type,
                    time_delta={"months": 6},
                    bq_project="basedosdados",
                    historical_database=historical_database,
                    upstream_tasks=[wait_upload_prod],
                )


br_bcb_sicor_template.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor_template.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    name="br_bcb_sicor.dicionario",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_sicor_dicionario:
    dataset_id = Parameter("dataset_id", default="br_bcb_sicor", required=True)
    table_id = Parameter("table_id", default="dicionario", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    dicionario_filepath = create_load_dictionary()

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=dicionario_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        upstream_tasks=[dicionario_filepath],
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
            data_path=dicionario_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
            upstream_tasks=[wait_for_materialization],
        )


br_bcb_sicor_template.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor_template.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
