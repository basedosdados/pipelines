# register flow

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.constants import constants
from pipelines.datasets.dataset_new_arch.tasks import criar_dataframe

# from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow

# from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)
from pipelines.utils.utils import log_task

with Flow(
    name="dataset_new_arch.table_new_arch",
    code_owners=[
        "trick",
    ],
) as dataset_new_arch_flow:
    dataset_id = Parameter(
        "dataset_id",
        default="dataset_new_arch",
        required=False,
    )

    table_id = Parameter("table_id", default="tabela_new_arch", required=False)

    target = Parameter("target", default="dev", required=False)

    dbt_alias = Parameter(
        "dbt_alias",
        default=True,
        required=False,
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    dataframe = criar_dataframe(upstream_tasks=[rename_flow_run])
    log_task(f"DATAFRAME -> {dataframe}", upstream_tasks=[dataframe])
    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=dataframe,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[dataframe],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=dataframe,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

        # update_django_metadata(
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     date_column_name={"date": "data"},
        #     date_format="%Y-%m-%d",
        #     coverage_type="part_bdpro",
        #     time_delta={"months": 6},
        #     prefect_mode="materialization_mode",
        #     bq_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
        #     upstream_tasks=[wait_upload_table_prod],
        # )

dataset_new_arch_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dataset_new_arch_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
