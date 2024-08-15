# -*- coding: utf-8 -*-
"""
Flows for br_rf_cno
"""
# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


from pipelines.constants import constants
from pipelines.datasets.br_rf_cno.constants import constants as br_rf_cno_constants
from pipelines.datasets.br_rf_cno.schedules import schedule_br_rf_cno
from pipelines.datasets.br_rf_cno.tasks import (
    wrangling,
    check_need_for_update,
    crawl_cno,
    create_parameters_list,
    )

from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
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
    name="br_rf_cno.tables", code_owners=["Gabriel Pisa"]
) as br_rf_cno_tables:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_rf_cno", required=True)
    table_id = Parameter("table_id", default="microdados", required=True)
    table_ids = Parameter("table_ids", default=['microdados', 'areas', 'cnaes', 'vinculos'], required=False)
    paths = Parameter("paths", default=['output/microdados', 'output/areas', 'output/cnaes', 'output/vinculos'], required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    #url = Parameter("url", default=br_rf_cno_constants.URL.value, required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    last_update_original_source = check_need_for_update(
        url=br_rf_cno_constants.URL_FTP.value
    )


    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=last_update_original_source,
        date_format="%Y-%m-%d",
        upstream_tasks=[last_update_original_source],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será inciada")

        data = crawl_cno(
            root='input',
            url=br_rf_cno_constants.URL.value
            )

        files = wrangling(
            input_dir='input',
            output_dir='output',
            partition_date=last_update_original_source,
            upstream_tasks=[data])


        #3. subir tabelas para o Storage e materilizar no BQ usando map
        wait_upload_table = create_table_and_upload_to_gcs.map(
            data_path=paths,
            dataset_id=unmapped(dataset_id),
            table_id=table_ids,
            dump_mode=unmapped("append"),
            source_format=unmapped('parquet'),
            upstream_tasks=[unmapped(files)] # https://github.com/PrefectHQ/prefect/issues/2752
        )

        dbt_parameters = create_parameters_list(
             dataset_id = dataset_id,
             table_ids = table_ids,
             materialization_mode = materialization_mode,
             dbt_alias = dbt_alias,
             download_csv_file = True,
             dbt_command = 'run',
             disable_elementary = True,
             upstream_tasks=[unmapped(wait_upload_table)]
        )

        with case(materialize_after_dump, True):
                # Trigger DBT flow run
                current_flow_labels = get_current_flow_labels()
                materialization_flow = create_flow_run.map(
                    flow_name=unmapped(utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value),
                    project_name=unmapped(constants.PREFECT_DEFAULT_PROJECT.value),
                    parameters=dbt_parameters,
                    labels=unmapped(current_flow_labels),
                    run_name=f"Materialize {dataset_id}.{table_ids}",
                    upstream_tasks=[unmapped(dbt_parameters)]
                )

                wait_for_materialization = wait_for_flow_run.map(
                    materialization_flow,
                    stream_states=unmapped(True),
                    stream_logs=unmapped(True),
                    raise_final_state=unmapped(True),
                    upstream_tasks=[unmapped(materialization_flow)]
                )
                wait_for_materialization.max_retries = (
                    dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
                )
                wait_for_materialization.retry_delay = timedelta(
                    seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
                )

                with case(update_metadata, True):

                    update_django_metadata.map(
                        dataset_id=unmapped(dataset_id),
                        table_id=table_ids,
                        date_column_name=unmapped({"date": "data_extracao"}), #register_date
                        date_format=unmapped("%Y-%m-%d"),
                        coverage_type=unmapped("all_bdpro"),
                        #time_delta=unmapped({"months": 6}),
                        prefect_mode=unmapped(materialization_mode),
                        bq_project=unmapped("basedosdados"),
                        upstream_tasks=[unmapped(wait_for_materialization)],
                    )


br_rf_cno_tables.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_rf_cno_tables.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_rf_cno_tables.schedule = schedule_br_rf_cno