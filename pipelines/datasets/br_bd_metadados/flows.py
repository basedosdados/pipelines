"""
Flows for br_bd_metadados
"""

# pylint: disable=invalid-name

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_bd_metadados.schedules import (
    every_day_prefect_flow_runs,
    every_day_prefect_flows,
)
from pipelines.datasets.br_bd_metadados.tasks import (
    crawler_flow_runs,
    crawler_flows,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_bd_metadados.prefect_flow_runs",
    code_owners=[
        "lauris",
    ],
) as bd_prefect_flow_runs:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bd_metadados", required=True
    )
    table_id = Parameter(
        "table_id", default="prefect_flow_runs", required=True
    )

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    filepath = crawler_flow_runs()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
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

bd_prefect_flow_runs.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_prefect_flow_runs.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
bd_prefect_flow_runs.schedule = every_day_prefect_flow_runs


with Flow(
    name="br_bd_metadados.prefect_flows",
    code_owners=[
        "lauris",
    ],
) as bd_prefect_flows:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bd_metadados", required=True
    )
    table_id = Parameter("table_id", default="prefect_flows", required=True)

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    filepath = crawler_flows()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
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

bd_prefect_flows.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_prefect_flows.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
bd_prefect_flows.schedule = every_day_prefect_flows
