"""
Flows for br_rf_cno
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.rf.tasks import (
    check_need_for_update,
    crawl,
    process_file,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(name="BD Template - Receita Federal") as flow_rf:
    # Common parameters
    dataset_id = Parameter("dataset_id", default="br_rf_cno", required=True)
    table_id = Parameter("table_id", required=True)
    chunksize = Parameter("chunksize", default=100000, required=False)
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    last_update_original_source = check_need_for_update(
        dataset_id,
        upstream_tasks=[dataset_id],
    )

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=last_update_original_source,
        date_format="%Y-%m-%d",
        upstream_tasks=[last_update_original_source],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela {table_id}!!")

    with case(check_if_outdated, True):
        log_task(f"Existem atualizações para {table_id}! A run será iniciada")

        # Data processing pipeline
        data = crawl(
            dataset_id=dataset_id,
            input_dir="input",
            upstream_tasks=[
                check_if_outdated,
                last_update_original_source,
            ],
        )

        path = process_file(
            dataset_id=dataset_id,
            table_id=table_id,
            input_dir="input",
            output_dir="output",
            partition_date=last_update_original_source,
            chunksize=chunksize,
            upstream_tasks=[data],
        )

        # Upload to GCS
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            source_format="parquet",
            upstream_tasks=[path],
        )

        # Materialization and metadata
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_download_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_extracao"},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_download_data_to_gcs],
                )

# Common flow configuration
flow_rf.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_rf.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
