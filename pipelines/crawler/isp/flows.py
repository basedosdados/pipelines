from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.isp.tasks import (
    clean_data,
    get_count_lines,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="BD Template - Estatísticas de Segurança",
    code_owners=[
        "trick",
    ],
) as flow_isp:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter("table_id", required=True)

    # Materialization mode

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

    compararison_between_lines = get_count_lines(file_name=table_id)

    with case(compararison_between_lines, True):
        filepath = clean_data(
            file_name=table_id,
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
                    coverage_type="all_free",
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )

flow_isp.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_isp.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
