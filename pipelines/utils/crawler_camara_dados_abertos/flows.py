# -*- coding: utf-8 -*-

# register flow in prefect now


from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.crawler_camara_dados_abertos.constants import (
    update_metadata_variable_dictionary,
)
from pipelines.utils.crawler_camara_dados_abertos.tasks import (
    check_if_url_is_valid,
    save_data,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
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

    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    with case(check_if_url_is_valid(table_id), True):
        # Save data to GCS
        filepath = save_data(
            table_id=table_id,
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[filepath],
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

            get_table_id_in_update_metadata_variable_dictionary = (
                update_metadata_variable_dictionary(
                    table_id=table_id,
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )
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
