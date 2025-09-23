# -*- coding: utf-8 -*-

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.anatel.banda_larga_fixa.tasks import (
    get_max_date_in_table_microdados,
    get_year_and_unzip,
    join_tables_in_function,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="BD template - Anatel Banda Larga Fixa", code_owners=["trick"]
) as flow_anatel_banda_larga_fixa:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_anatel_banda_larga_fixa", required=True
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

    ano = Parameter("ano", default=None, required=False)

    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    #####
    # Function dynamic parameters
    # https://discourse.prefect.io/t/my-parameter-value-shows-the-same-date-every-day-how-can-i-set-parameter-value-dynamically/99
    #####
    new_ano = get_year_and_unzip(day=ano, upstream_tasks=[rename_flow_run])

    update_tables = get_max_date_in_table_microdados(
        ano=new_ano, table_id=table_id, upstream_tasks=[new_ano]
    )

    get_max_date = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=update_tables,
        date_format="%Y-%m",
        upstream_tasks=[update_tables],
    )

    with case(get_max_date, True):
        filepath = join_tables_in_function(
            table_id=table_id, ano=new_ano, upstream_tasks=[get_max_date]
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
            upstream_tasks=[
                filepath
            ],  # Fix: Wrap filepath in a list to make it iterable
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
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

flow_anatel_banda_larga_fixa.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_anatel_banda_larga_fixa.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
