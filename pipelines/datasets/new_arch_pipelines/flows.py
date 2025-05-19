# -*- coding: utf-8 -*-
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run

from pipelines.constants import constants
from pipelines.datasets.new_arch_pipelines.tasks import criar_dataframe
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs_teste,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="new_arch_pipeline",
    code_owners=[
        "trick",
    ],
) as new_arch_pipeline:
    dataset_id = Parameter(
        "dataset_id",
        default="new_arch_pipeline",
        required=False,
    )
    table_id = Parameter(
        "table_id", default="new_arch_pipeline", required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    dataframe = criar_dataframe()

    wait_upload_table_dev = create_table_and_upload_to_gcs_teste(
        data_path=dataframe,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
        wait=dataframe,
        upstream_tasks=[dataframe],
    )

    # print_ids(upstream_tasks=[wait_upload_table])
    # current_flow_labels = get_current_flow_labels()

    materialization_flow = create_flow_run(
        flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": "dev",
            "dbt_command": "run/test",
            "disable_elementary": False,
        },
        labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
        run_name=r"Materialize {dataset_id}.{table_id}",
        upstream_tasks=[wait_upload_table_dev],
    )

    with case(materialization_mode, "prod"):
        wait_upload_table_prod = create_table_and_upload_to_gcs_teste(
            data_path=dataframe,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=dataframe,
            bucket_name="basedosdados",
            upstream_tasks=[wait_upload_table_dev],
        )

        # current_flow_labels = get_current_flow_labels()

        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": "prod",
                "dbt_command": "run/test",
                "disable_elementary": False,
            },
            labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            run_name=r"Materialize {dataset_id}.{table_id}",
            upstream_tasks=[wait_upload_table_prod],
        )

        update_django_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            date_column_name={"date": "data"},
            date_format="%Y-%m-%d",
            coverage_type="part_bdpro",
            time_delta={"months": 6},
            prefect_mode="materialization_mode",
            bq_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            upstream_tasks=[wait_upload_table_prod],
        )

new_arch_pipeline.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
new_arch_pipeline.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
