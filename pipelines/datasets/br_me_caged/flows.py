# -*- coding: utf-8 -*-
"""
Flows for br_me_novo_caged
"""

from prefect import Parameter

# pylint: disable=invalid-name
from pipelines.datasets.br_me_caged.tasks import (
    build_partitions,
    get_caged_data,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
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

    # check_if_outdated = check_if_data_is_outdated(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     data_source_max_date=last_date,
    #     date_format="%Y-%m",
    #     upstream_tasks=[last_date],
    # )
    year = Parameter("year", default=2022, required=True)

    get_data = get_caged_data(table_id=table_id, year=year)

    filepath = build_partitions(table_id=table_id, upstream_tasks=[get_data])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

# cagedmov.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# cagedmov.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# cagedmov.schedule = every_month
