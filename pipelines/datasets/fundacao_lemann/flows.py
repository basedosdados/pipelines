"""
Flows for fundacao_lemann
"""

from datetime import timedelta

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants

from pipelines.utils.tasks import get_current_flow_labels

from pipelines.datasets.fundacao_lemann.schedules import every_year

with Flow(
    name="fundacao_lemann.ano_escola_serie_educacao_aprendizagem_adequada",
    code_owners=["crislanealves"],
) as ano_escola_serie_educacao_aprendizagem_adequada:
    dataset_id = Parameter("dataset_id", default="fundacao_lemann", required=True)
    table_id = Parameter(
        "table_id",
        default="ano_escola_serie_educacao_aprendizagem_adequada",
        required=True,
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    current_flow_labels = get_current_flow_labels()
    materialization_flow = create_flow_run(
        flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        parameters={
            "dataset_id": dataset_id,
            "table_id": table_id,
            "mode": materialization_mode,
            "dbt_alias": dbt_alias,
        },
        labels=current_flow_labels,
        run_name=f"Materialize {dataset_id}.{table_id}",
    )

    wait_for_materialization = wait_for_flow_run(
        materialization_flow,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
    )
    wait_for_materialization.max_retries = (
        dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
    )
    wait_for_materialization.retry_delay = timedelta(
        seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
    )

ano_escola_serie_educacao_aprendizagem_adequada.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
ano_escola_serie_educacao_aprendizagem_adequada.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
ano_escola_serie_educacao_aprendizagem_adequada.schedule = every_year
