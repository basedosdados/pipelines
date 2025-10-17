"""
Flows for br_rf_cno
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_rf_cno.constants import (
    constants as br_rf_cno_constants,
)
from pipelines.datasets.br_rf_cno.schedules import schedule_br_rf_cno
from pipelines.datasets.br_rf_cno.tasks import (
    check_need_for_update,
    crawl_cno,
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


def create_cno_flow(table_id: str, filename: str):
    """Factory function to create CNO flows with common structure."""

    flow_name = f"br_rf_cno.{table_id}"

    with Flow(name=flow_name, code_owners=["Gabriel Pisa"]) as flow:
        # Common parameters
        dataset_id = Parameter(
            "dataset_id", default="br_rf_cno", required=True
        )
        table_id_param = Parameter("table_id", default=table_id, required=True)
        update_metadata = Parameter(
            "update_metadata", default=False, required=False
        )
        target = Parameter("target", default="prod", required=False)
        materialize_after_dump = Parameter(
            "materialize_after_dump", default=True, required=False
        )
        dbt_alias = Parameter("dbt_alias", default=True, required=False)

        # Common flow setup
        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id_param,
            wait=table_id_param,
        )

        last_update_original_source = check_need_for_update(
            url=br_rf_cno_constants.URL_FTP.value,
            upstream_tasks=[rename_flow_run],
        )

        check_if_outdated = check_if_data_is_outdated(
            dataset_id=dataset_id,
            table_id=table_id_param,
            data_source_max_date=last_update_original_source,
            date_format="%Y-%m-%d",
            upstream_tasks=[last_update_original_source],
        )

        with case(check_if_outdated, False):
            log_task(f"Não há atualizações para a tabela {table_id}!!")

        with case(check_if_outdated, True):
            log_task(
                f"Existem atualizações para {table_id}! A run será iniciada"
            )

            # Data processing pipeline
            data = crawl_cno(
                root="input",
                url=br_rf_cno_constants.URL.value,
                upstream_tasks=[
                    check_if_outdated,
                    last_update_original_source,
                ],
            )

            path = process_file(
                filename,
                input_dir="input",
                output_dir="output",
                partition_date=last_update_original_source,
                chunksize=100000,
                upstream_tasks=[data],
            )

            # Upload to GCS
            wait_upload_table = create_table_and_upload_to_gcs(
                data_path=path,
                dataset_id=dataset_id,
                table_id=table_id_param,
                dump_mode="append",
                source_format="parquet",
                upstream_tasks=[path],
            )

            # Materialization and metadata
            with case(materialize_after_dump, True):
                wait_for_materialization = run_dbt(
                    dataset_id=dataset_id,
                    table_id=table_id_param,
                    target=target,
                    dbt_alias=dbt_alias,
                    upstream_tasks=[wait_upload_table],
                )

                wait_for_download_data_to_gcs = download_data_to_gcs(
                    dataset_id=dataset_id,
                    table_id=table_id_param,
                    upstream_tasks=[wait_for_materialization],
                )

                with case(update_metadata, True):
                    update_django_metadata(
                        dataset_id=dataset_id,
                        table_id=table_id_param,
                        date_column_name={"date": "data_extracao"},
                        date_format="%Y-%m-%d",
                        coverage_type="part_bdpro",
                        time_delta={"months": 6},
                        prefect_mode=target,
                        bq_project="basedosdados",
                        upstream_tasks=[wait_for_download_data_to_gcs],
                    )

    # Common flow configuration
    flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
    flow.run_config = KubernetesRun(
        image=constants.DOCKER_IMAGE.value,
        memory_limit="4Gi",
        memory_request="1Gi",
        cpu_limit=1,
    )
    flow.schedule = schedule_br_rf_cno

    return flow


# Create all flows using the factory function
br_rf_cno_microdados = create_cno_flow("microdados", "cno.csv")
br_rf_cno_vinculos = create_cno_flow("vinculos", "cno_vinculos.csv")
br_rf_cno_areas = create_cno_flow("areas", "cno_areas.csv")
br_rf_cno_cnaes = create_cno_flow("cnaes", "cno_cnaes.csv")
