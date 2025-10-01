"""
Flows for ibge inflacao
"""

# pylint: disable=C0103, E1123, invalid-name, duplicate-code, R0801

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.ibge_inflacao.tasks import (
    check_for_updates_task,
    collect_data_utils,
    json_to_csv,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(name="BD Template - IBGE Inflação") as flow_ibge:
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    periodo = Parameter("periodo", default=None, required=False)
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

    needs_to_update = check_for_updates_task(
        dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update, True):
        download_data = collect_data_utils(
            dataset_id=dataset_id,
            table_id=table_id,
            periodo=periodo,
            upstream_tasks=[rename_flow_run],
        )

        filepath = json_to_csv(
            table_id=table_id,
            dataset_id=dataset_id,
            periodo=periodo,
            upstream_tasks=[download_data],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[filepath],
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
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )


flow_ibge.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
