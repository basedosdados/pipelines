# -*- coding: utf-8 -*-
"""
Flows for br_ans_beneficiario
"""


from datetime import datetime, timedelta
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_ans_beneficiario.tasks import (
    extract_links_and_dates,
    check_for_updates,
    crawler_ans,
    is_empty,
    get_today_date,
)

from pipelines.datasets.br_ans_beneficiario.schedules import every_day_ans
from pipelines.utils.decorators import Flow
from prefect import Parameter, case
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
    log_task,
    # update_django_metadata,
)
from pipelines.utils.metadata.flows import update_django_metadata
from prefect.tasks.prefect import (
    create_flow_run,
    wait_for_flow_run,
)


with Flow(
    name="br_ans_beneficiario.informacao_consolidada",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_ans_beneficiario_flow:
    dataset_id = Parameter("dataset_id", default="br_ans_beneficiario", required=True)
    table_id = Parameter("table_id", default="teste_beneficiario", required=True)
    url = Parameter(
        "url",
        default="https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios/",
        required=False,
    )
    update_metadata = Parameter("update_metadata", default=True, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    # files = Parameter("files", default=["202306", "202307"], required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    hrefs = extract_links_and_dates(url=url)

    files = check_for_updates(hrefs)

    with case(is_empty(files), True):
        log_task(f"Não houveram atualizações em {url.default}!")

    with case(is_empty(files), False):
        output_filepath = crawler_ans(files)
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
                date = get_today_date()
                update_django_metadata(
                    dataset_id,
                    table_id,
                    metadata_type="DateTimeRange",
                    _last_date=date,
                    bq_table_last_year_month=False,
                    bq_last_update=False,
                    is_bd_pro=True,
                    is_free=True,
                    date_format="yy-mm",
                    api_mode="prod",
                    time_delta=6,
                    time_unit="months",
                )

datasets_br_ans_beneficiario_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_ans_beneficiario_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_ans_beneficiario_flow.schedule = every_day_ans
