# -*- coding: utf-8 -*-
"""
Flows for br_me_caged
"""

from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants

# pylint: disable=invalid-name
from pipelines.datasets.br_me_caged.tasks import (
    build_partitions,
    build_table_paths,
    crawl_novo_caged_ftp,
    generate_yearmonth_range,
    get_source_last_date,
    get_table_last_date,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import (
    constants as dump_db_constants,
)
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

with Flow(
    "br_me_caged.microdados_movimentacao", code_owners=["Luiza"]
) as br_me_caged__microdados_movimentacao:
    dataset_id = Parameter("dataset_id", default="br_me_caged", required=True)

    table_id = Parameter(
        "table_id", default="microdados_movimentacao", required=True
    )

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    source_last_date = get_source_last_date()
    table_last_date = get_table_last_date(
        dataset_id, table_id, upstream_tasks=[source_last_date]
    )

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_last_date,
        date_format="%Y-%m",
        upstream_tasks=[source_last_date],
    )

    with case(table_last_date < source_last_date, False):
        log_task(f"No updates for table {table_id}!")

    with case(table_last_date < source_last_date, True):
        input_path, output_path = build_table_paths(
            table_id, upstream_tasks=[check_if_outdated]
        )

        input_dir, output_dir = build_table_paths(
            table_id, upstream_tasks=[source_last_date]
        )

        yearmonths = generate_yearmonth_range(
            table_last_date, source_last_date, upstream_tasks=[table_last_date]
        )

        crawl_caged = crawl_novo_caged_ftp.map(
            yearmonths,
            unmapped(table_id),
            upstream_tasks=[yearmonths],
        )

        filepath = build_partitions(
            table_id=table_id,
            table_output_dir=output_dir,
            upstream_tasks=[crawl_caged],
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
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "target": target,
                    "dbt_alias": dbt_alias,
                    "dbt_command": "run/test",
                    "disable_elementary": False,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
                upstream_tasks=[wait_upload_table],
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

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_me_caged__microdados_movimentacao.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_me_caged__microdados_movimentacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
