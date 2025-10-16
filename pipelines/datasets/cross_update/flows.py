"""
Flows for br_tse_eleicoes
"""

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.cross_update.schedules import (
    update_metadata_table_schedule,
)
from pipelines.datasets.cross_update.tasks import (
    filter_eligible_download_tables,
    get_metadata_data,
    query_tables,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="cross_update.update_nrows", code_owners=["lauris"]
) as crossupdate_nrows:
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    update_metadata_table = Parameter(
        "update_metadata_table", default=False, required=False
    )
    year = Parameter("year", default=2024, required=False)
    target = Parameter("target", default="prod", required=False)
    current_flow_labels = get_current_flow_labels()

    # Atualiza a tabela que contem os metadados do BQ
    with case(update_metadata_table, True):
        update_metadata_table_flow = create_flow_run(
            flow_name="cross_update.update_metadata_table",
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={"materialization_mode": target},
            labels=current_flow_labels,
        )

        wait_for_create_table = wait_for_flow_run(
            update_metadata_table_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

    # Consulta e  seleciona apenas as tabelas que atendem os critérios de tamanho e abertura(bdpro)

    eligible_to_zip_tables = query_tables(year=year, mode=target)
    tables_to_zip = filter_eligible_download_tables(
        eligible_to_zip_tables, upstream_tasks=[eligible_to_zip_tables]
    )

    # Para cada tabela selecionada cria um flow de dump para gcs
    with case(dump_to_gcs, True):
        dump_to_gcs_flow = create_flow_run.map(
            flow_name=unmapped(constants.FLOW_DUMP_TO_GCS_NAME.value),
            project_name=unmapped(constants.PREFECT_DEFAULT_PROJECT.value),
            parameters=tables_to_zip,
            labels=unmapped(current_flow_labels),
            run_name=unmapped("Dump to GCS"),
        )

        wait_for_dump_to_gcs = wait_for_flow_run.map(
            dump_to_gcs_flow,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
        )


crossupdate_nrows.storage = GCS(str(constants.GCS_FLOWS_BUCKET.value))
crossupdate_nrows.run_config = KubernetesRun(
    image=str(constants.DOCKER_IMAGE.value)
)
# crossupdate_nrows.schedule = schedule_nrows

with Flow(
    name="cross_update.update_metadata_table", code_owners=["lauris"]
) as crossupdate_update_metadata_table:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_metadados", required=False
    )
    table_id = Parameter("table_id", default="bigquery_tables", required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
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

    file_path = get_metadata_data(
        mode=target,
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=file_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[file_path],
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
            coverage_type="all_free",
            prefect_mode=target,
            bq_project="basedosdados",
            historical_database=False,
        )


crossupdate_update_metadata_table.storage = GCS(
    str(constants.GCS_FLOWS_BUCKET.value)
)
crossupdate_update_metadata_table.run_config = KubernetesRun(
    image=str(constants.DOCKER_IMAGE.value)
)
crossupdate_update_metadata_table.schedule = update_metadata_table_schedule
