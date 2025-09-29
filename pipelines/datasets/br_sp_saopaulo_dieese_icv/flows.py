"""
Flows for br_sp_saopaulo_dieese_icv
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_sp_saopaulo_dieese_icv.tasks import clean_dieese_icv
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_sp_saopaulo_dieese_icv.mes", code_owners=["crislanealves"]
) as br_sp_dieese:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_sp_saopaulo_dieese_icv", required=True
    )
    table_id = Parameter("table_id", default="mes", required=True)
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    filepath = clean_dieese_icv()  # igual minha funcao de tratamento

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        wait=filepath,
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )

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

br_sp_dieese.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_sp_dieese.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_sp_dieese.schedule = every_month
