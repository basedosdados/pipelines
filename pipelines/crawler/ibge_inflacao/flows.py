# register flow

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.ibge_inflacao.tasks import (
    check_for_updates,
    collect_data_utils,
    json_to_csv,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(name="BD Template - IBGE Inflação") as flow_ibge:
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    periodo = Parameter("periodo", default=None, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    download_data_periods = collect_data_utils(
        dataset_id=dataset_id,
        table_id=table_id,
        periodo=periodo,
        upstream_tasks=[rename_flow_run],
    )

    needs_to_update = check_for_updates(
        dataset_id=dataset_id,
        table_id=table_id,
        upstream_tasks=[download_data_periods],
    )

    outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=needs_to_update,
        date_format="%Y-%m",
        upstream_tasks=[needs_to_update],
    )

    with case(outdated, True):
        filepath = json_to_csv(
            table_id=table_id,
            dataset_id=dataset_id,
            upstream_tasks=[outdated],
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[filepath],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )


flow_ibge.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
