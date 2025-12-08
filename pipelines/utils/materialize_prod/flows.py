"""
Flows for transfer_files_to_prod
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.materialize_prod.tasks import (
    download_files_from_bucket_folders,
)
from pipelines.utils.tasks import (
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
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

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
        )


transfer_files_to_prod_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
transfer_files_to_prod_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
