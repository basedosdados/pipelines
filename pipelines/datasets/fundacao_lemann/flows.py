"""
Flows for fundacao_lemann
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.fundacao_lemann.schedules import every_year
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import download_data_to_gcs, run_dbt

with Flow(
    name="fundacao_lemann.ano_escola_serie_educacao_aprendizagem_adequada",
    code_owners=["crislanealves"],
) as ano_escola_serie_educacao_aprendizagem_adequada:
    dataset_id = Parameter(
        "dataset_id", default="fundacao_lemann", required=True
    )
    table_id = Parameter(
        "table_id",
        default="ano_escola_serie_educacao_aprendizagem_adequada",
        required=True,
    )

    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
    )
    wait_for_dowload_data_to_gcs = download_data_to_gcs(
        dataset_id=dataset_id,
        table_id=table_id,
        upstream_tasks=[wait_for_materialization],
    )

ano_escola_serie_educacao_aprendizagem_adequada.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
ano_escola_serie_educacao_aprendizagem_adequada.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
ano_escola_serie_educacao_aprendizagem_adequada.schedule = every_year
