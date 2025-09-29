# ! register flow
"""
Flows for dumping data directly from BigQuery to GCS.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_to_gcs.tasks import (
    get_project_id,
)

# from pipelines.datasets.cross_update.tasks import get_all_eligible_in_selected_year
from pipelines.utils.tasks import (
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name=constants.FLOW_DUMP_TO_GCS_NAME.value,
    code_owners=["lauris"],
) as dump_to_gcs_flow:
    project_id = Parameter("project_id", required=False)  # basedosdados
    dataset_id = Parameter(
        "dataset_id", required=False
    )  # dataset_id or dataset_id_staging
    table_id = Parameter("table_id", required=False)
    query = Parameter("query", required=False)
    # year = Parameter("year", required=False)
    # mode = Parameter("mode", required=False)
    bd_project_mode = Parameter(
        "bd_project_mode", required=False, default="prod"
    )  # prod or staging
    billing_project_id = Parameter("billing_project_id", required=False)
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump to GCS: ", dataset_id=dataset_id, table_id=table_id
    )
    project_id = get_project_id(
        project_id=project_id, bd_project_mode=bd_project_mode
    )
    """
    Caso queiram subir todas as tabelas para o bucket novamente,
    1. Descomentem os parametros year e mode
    2. Descomente também a linha 62 até a linha 72
    3. Retire o decorador da linha 20 na task de cross_update

    dataset_ids, table_ids = get_all_eligible_in_selected_year(year, mode)

    # with case(trigger_download, True):
    download_task = download_data_to_gcs.map(
        project_id=unmapped(project_id),
        dataset_id=dataset_ids,
        table_id=table_ids,
        query=unmapped(query),
        bd_project_mode=unmapped(bd_project_mode),
        billing_project_id=unmapped(billing_project_id),
    )
    """

    download_task = download_data_to_gcs(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        query=query,
        bd_project_mode=bd_project_mode,
        billing_project_id=billing_project_id,
    )

dump_to_gcs_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_to_gcs_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
