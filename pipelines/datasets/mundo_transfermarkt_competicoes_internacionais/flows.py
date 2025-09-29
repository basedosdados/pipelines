"""
Flows for mundo_transfermarkt_competicoes_internacionais
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.mundo_transfermarkt_competicoes_internacionais.tasks import (
    execucao_coleta_sync,
    get_data_source_transfermarkt_max_date,
    make_partitions,
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

###############################################################################


with Flow(
    name="mundo_transfermarkt_competicoes_internacionais.champions_league",
    code_owners=[
        "equipe_pipelines",
    ],
) as transfermarkt_flow:
    dataset_id = Parameter(
        "dataset_id",
        default="mundo_transfermarkt_competicoes_internacionais",
        required=False,
    )
    table_id = Parameter(
        "table_id", default="champions_league", required=False
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
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    data_source_max_date = get_data_source_transfermarkt_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, True):
        df = execucao_coleta_sync()
        output_filepath = make_partitions(df, upstream_tasks=[df])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )


transfermarkt_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
transfermarkt_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# transfermarkt_flow.schedule = every_day_champions_league
