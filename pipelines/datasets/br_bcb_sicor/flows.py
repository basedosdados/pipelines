from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_bcb_sicor.tasks import (
    download_table,
    search_sicor_links,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="BD template - BR_BCB_SICOR",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_sicor_template:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_bcb_sicor", required=True)
    table_id = Parameter(
        "table_id", default="microdados_operacao", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    download_links = search_sicor_links()
    download_table = download_table(download_links, id_tabela=table_id)

br_bcb_sicor_template.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor_template.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
