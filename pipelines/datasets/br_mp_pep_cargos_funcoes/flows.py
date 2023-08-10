# -*- coding: utf-8 -*-
"""
Flows for br_mp_pep_cargos_funcoes
"""

import datetime

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


from pipelines.constants import constants
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    update_django_metadata,
)
from pipelines.utils.utils import log
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow

from pipelines.datasets.br_mp_pep_cargos_funcoes.schedules import every_month
from pipelines.datasets.br_mp_pep_cargos_funcoes.tasks import (
    setup_web_driver,
    scraper,
    clean_data,
    make_partitions,
    is_up_to_date,
)

with Flow(
    name="br_mp_pep.cargos_funcoes",
    code_owners=[
        "aspeddro",
    ],
) as datasets_br_mp_pep_cargos_funcoes_flow:
    dataset_id = Parameter("dataset_id", default="br_mp_pep", required=True)
    table_id = Parameter("table_id", default="cargos_funcoes", required=True)

    setup = setup_web_driver()

    data_is_up_to_date = is_up_to_date(upstream_tasks=[setup])

    # if data_is_up_to_date:
    #     log(f"Sem atualizações: {data_is_up_to_date}")
    #     exit(0)

    current_date = datetime.datetime(
        year=datetime.datetime.now().year, month=datetime.datetime.now().month, day=1
    )
    year_delta = current_date - datetime.timedelta(days=1)
    year_end = year_delta.year

    scrapper = scraper(
        year_start=2019, year_end=year_end, upstream_tasks=[data_is_up_to_date]
    )

    df = clean_data(upstream_tasks=[scrapper])

    log("Clean data Finished")

    output_filepath = make_partitions(df, upstream_tasks=[df])

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    update_metadata = Parameter("update_metadata", default=True, required=False)

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
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
            run_name=r"Materialize {dataset_id}.{table_id}",
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
        wait_for_materialization.retry_delay = datetime.timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm",
                upstream_tasks=[wait_for_materialization],
            )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": f"{table_id}_atualizado",
                "mode": materialization_mode,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}_atualizado",
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
        wait_for_materialization.retry_delay = datetime.timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id,
                f"{table_id}_atualizado",
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm",
                upstream_tasks=[wait_for_materialization],
            )


datasets_br_mp_pep_cargos_funcoes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_mp_pep_cargos_funcoes_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_mp_pep_cargos_funcoes_flow.schedule = every_month
