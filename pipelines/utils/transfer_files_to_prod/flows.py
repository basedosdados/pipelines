"""
Flows for transfer_files_to_prod
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)
from pipelines.utils.transfer_files_to_prod.tasks import (
    download_files_from_bucket_folders,
)

with Flow(
    name="BD Utils: Transfere arquivos do bucket basedosdados-dev para basedosdados",
    code_owners=[
        "equipe_pipelines",
    ],
) as transfer_files_to_prod_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )
    table_id = Parameter(
        "table_id", default="novo_bolsa_familia", required=False
    )
    folders = Parameter(
        "folders",
        default=["mes_competencia=202306", "mes_competencia=202305"],
        required=False,
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

    output_filepath = download_files_from_bucket_folders(
        dataset_id=dataset_id, table_id=table_id, folders=folders
    )
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=output_filepath,
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


transfer_files_to_prod_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
transfer_files_to_prod_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
