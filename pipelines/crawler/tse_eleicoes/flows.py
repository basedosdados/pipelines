"""
Flows for br_tse_eleicoes
"""

# pylint: disable=invalid-name,line-too-long

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.tse_eleicoes.tasks import (
    flows_control,
    get_data_source_max_date,
    preparing_data,
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
    name="BD template - BR_TSE_ELEICOES", code_owners=["luiz"]
) as flow_br_tse_eleicoes:
    # Parameters

    dataset_id = Parameter(
        "dataset_id", default="br_tse_eleicoes", required=True
    )

    table_id = Parameter("table_id", required=True)

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    # Inicio das tarefas

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    flow = flows_control(
        table_id=table_id,
        mode=target,
        upstream_tasks=[rename_flow_run],
    )

    data_source_max_date = get_data_source_max_date(
        flow_class=flow, upstream_tasks=[flow]
    )

    outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        date_type="last_update_date",
        data_source_max_date=data_source_max_date,
        upstream_tasks=[data_source_max_date],
    )

    with case(outdated, True):
        ready_data_path = preparing_data(
            flow_class=flow, upstream_tasks=[outdated]
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=ready_data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[ready_data_path],
        )

        # materialize municipio_exportacao
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

            # coverage updater

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_eleicao"},
                    date_format="%Y",
                    prefect_mode=target,
                    coverage_type="all_free",
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

flow_br_tse_eleicoes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_br_tse_eleicoes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
