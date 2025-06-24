from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.constants import (
    update_metadata_variable_dictionary,
)
from pipelines.utils.crawler_camara_dados_abertos.tasks import (
    check_if_url_is_valid,
    get_output,
    save_data,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

# ------------------------------ TABLES UNIVERSAL -------------------------------------

with Flow(
    name="BD template - Camara Dados Abertos", code_owners=["trick"]
) as flow_camara_dados_abertos:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_camara_dados_abertos", required=True
    )
    table_id = Parameter(
        "table_id",
        required=True,
    )
    target = Parameter("target", default="prod", required=True)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=True
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=True)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    with case(check_if_url_is_valid(table_id), True):
        # Save data to GCS
        filepath = save_data(
            table_id=table_id,
            upstream_tasks=[rename_flow_run],
        )
        get_output = get_output(table_id=table_id, upstream_tasks=[filepath])
        upload_and_materialization_dev = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="dev",
                bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[get_output],
            )
        )

        with case(target, "prod"):
            upload_and_materialization_prod = template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )

        get_table_id_in_update_metadata_variable_dictionary = (
            update_metadata_variable_dictionary(table_id=table_id)
        )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=get_table_id_in_update_metadata_variable_dictionary[
                    "dataset_id"
                ],
                table_id=get_table_id_in_update_metadata_variable_dictionary[
                    "table_id"
                ],
                date_column_name=get_table_id_in_update_metadata_variable_dictionary[
                    "date_column_name"
                ],
                date_format=get_table_id_in_update_metadata_variable_dictionary[
                    "date_format"
                ],
                coverage_type=get_table_id_in_update_metadata_variable_dictionary[
                    "coverage_type"
                ],
                time_delta=get_table_id_in_update_metadata_variable_dictionary[
                    "time_delta"
                ],
                prefect_mode=get_table_id_in_update_metadata_variable_dictionary[
                    "prefect_mode"
                ],
                bq_project=get_table_id_in_update_metadata_variable_dictionary[
                    "bq_project"
                ],
                historical_database=get_table_id_in_update_metadata_variable_dictionary[
                    "historical_database"
                ],
                upstream_tasks=[
                    get_table_id_in_update_metadata_variable_dictionary
                ],
            )
flow_camara_dados_abertos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_camara_dados_abertos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
