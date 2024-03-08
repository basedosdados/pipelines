# -*- coding: utf-8 -*-
"""
Flows for br_ans_beneficiario
"""


from datetime import timedelta
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.tasks.control_flow.conditional import ifelse

from pipelines.constants import constants
from pipelines.datasets.br_ans_beneficiario.schedules import every_day_ans
from pipelines.datasets.br_ans_beneficiario.tasks import (
    check_for_updates,
    crawler_ans,
    extract_links_and_dates,
    is_empty,
    update_files_list,
    check_last_date,
    get_file_max_date,
    check_condition,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.metadata.tasks import check_if_data_is_outdated
from pipelines.utils.tasks import (  # update_django_metadata,
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_ans_beneficiario.informacao_consolidada",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_ans_beneficiario_flow:
    dataset_id = Parameter("dataset_id", default="br_ans_beneficiario", required=False)
    table_id = Parameter("table_id", default="informacao_consolidada", required=False)
    url = Parameter(
        "url",
        default="https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/",
        required=False,
    )
    update_metadata = Parameter("update_metadata", default=False, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    force_update = Parameter("update", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    # Monta dataframe com as colunas: ["arquivo", "ultima_atualizacao", "data_hoje", "desatualizado"]
    df = extract_links_and_dates(url=url)

    # Se update == True, força o update dos dados.
    with case(force_update, True):
        files = update_files_list(df, upstream_tasks=[df])


    with case(force_update, False):
        # Se update == False, verifica se a data de hoje == data de atualização | data da cobertura temporal < data do último arquivo (ex: arquivo 202401 -> (2023-12-01 < 2024-01-01) )
        # Essa condição é verificada na task `check_condition()` abaixo.

        file_last_date = get_file_max_date(df, upstream_tasks=[df])
        last_date_check = check_last_date(df, upstream_tasks=[df])
        coverage_check = check_if_data_is_outdated(dataset_id,
            table_id,
            data_source_max_date=file_last_date,
            date_format= "%Y-%m-%d", upstream_tasks=[df, file_last_date])
        with case(check_condition(last_date_check, coverage_check), True):
            # Se check_condition = True, cria a lista com os arquivos para o download.
            files = update_files_list(df, upstream_tasks=[df])

    with case(is_empty(files), False):
        output_filepath = crawler_ans(files, upstream_tasks=[files])
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            source_format="parquet",
            wait=output_filepath,
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
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
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
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

datasets_br_ans_beneficiario_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_ans_beneficiario_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_ans_beneficiario_flow.schedule = every_day_ans
