from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_cgu_emendas_parlamentares.schedules import (
    every_day_emendas_parlamentares,
)
from pipelines.datasets.br_cgu_emendas_parlamentares.tasks import (
    convert_str_to_float,
    get_last_modified_time,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (  # update_django_metadata,
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_cgu_emendas_parlamentares.microdados",
    code_owners=[
        "trick",
    ],
) as br_cgu_emendas_parlamentares_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_emendas_parlamentares", required=False
    )
    table_id = Parameter("table_id", default="microdados", required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    max_modified_time = get_last_modified_time()

    outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        date_type="last_update_date",
        data_source_max_date=max_modified_time,
        upstream_tasks=[max_modified_time],
    )

    with case(outdated, True):
        output_path = convert_str_to_float()
        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_path],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_alias=dbt_alias,
            dbt_command="run/test",
            disable_elementary=False,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=output_path,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_format="%Y",
                    date_column_name={"year": "ano_emenda"},
                    coverage_type="part_bdpro",
                    time_delta={"years": 1},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )

br_cgu_emendas_parlamentares_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cgu_emendas_parlamentares_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cgu_emendas_parlamentares_flow.schedule = every_day_emendas_parlamentares
