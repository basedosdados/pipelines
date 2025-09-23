# -*- coding: utf-8 -*-
"""
Flows for br_rj_isp_estatisticas_seguranca.
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.crawler_isp.tasks import (
    clean_data,
    get_count_lines,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
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

    compararison_between_lines = get_count_lines(file_name=table_id)

    with case(compararison_between_lines, True):
        filepath = clean_data(
            file_name=table_id,
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

flow_isp.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_isp.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
