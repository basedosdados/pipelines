# -*- coding: utf-8 -*-
"""
Flows for br_cgu_servidores_executivo_federal
"""

import datetime

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_cgu_servidores_executivo_federal.constants import (
    constants as cgu_constants,
)
from pipelines.datasets.br_cgu_servidores_executivo_federal.schedules import every_month
from pipelines.datasets.br_cgu_servidores_executivo_federal.tasks import (
    download_files,
    get_next_date,
    is_up_to_date,
    make_partitions,
    merge_and_clean_data,
    table_is_available,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.utils import log_task

with Flow(
    name="br_cgu_servidores_executivo_federal",
    code_owners=[
        "aspeddro",
    ],
) as datasets_br_cgu_servidores_executivo_federal_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_servidores_executivo_federal", required=True
    )

    tables_ids = list(cgu_constants.TABLES.value.keys())

    table_id = Parameter("table_id", default=tables_ids, required=True)

    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    next_date = get_next_date()
    log_task(f"Next date: {next_date=}")

    data_is_up_to_date = is_up_to_date(next_date, upstream_tasks=[next_date])

    with case(data_is_up_to_date, True):
        log_task("Tabelas estão atualizadas")

    with case(data_is_up_to_date, False):
        log_task(f"Starting download, {next_date}, {next_date}")
        sheets_info = download_files(
            date_start=next_date, date_end=next_date, upstream_tasks=[next_date]
        )
        log_task("Files downloaded")

        data_clean_by_table = merge_and_clean_data(
            sheets_info, upstream_tasks=[sheets_info]
        )
        log_task("Data clean finished")

        outputs_path_by_table = make_partitions(
            data_clean_by_table, upstream_tasks=[data_clean_by_table]
        )
        log_task("Partitions done")

        with case(
            table_is_available(outputs_path_by_table, "aposentados_cadastro"), True
        ):
            create_table_and_upload_to_gcs(
                data_path=outputs_path_by_table["aposentados_cadastro"],
                dataset_id=dataset_id,
                table_id="aposentados_cadastro",
                dump_mode="append",
                wait=outputs_path_by_table,
            )

        with case(
            table_is_available(outputs_path_by_table, "pensionistas_cadastro"), True
        ):
            create_table_and_upload_to_gcs(
                data_path=outputs_path_by_table["pensionistas_cadastro"],
                dataset_id=dataset_id,
                table_id="pensionistas_cadastro",
                dump_mode="append",
                wait=outputs_path_by_table,
            )

        with case(
            table_is_available(outputs_path_by_table, "servidores_cadastro"), True
        ):
            create_table_and_upload_to_gcs(
                data_path=outputs_path_by_table["servidores_cadastro"],
                dataset_id=dataset_id,
                table_id="servidores_cadastro",
                dump_mode="append",
                wait=outputs_path_by_table,
            )

        with case(
            table_is_available(
                outputs_path_by_table, "reserva_reforma_militares_cadastro"
            ),
            True,
        ):
            create_table_and_upload_to_gcs(
                data_path=outputs_path_by_table["reserva_reforma_militares_cadastro"],
                dataset_id=dataset_id,
                table_id="reserva_reforma_militares_cadastro",
                dump_mode="append",
                wait=outputs_path_by_table,
            )

        with case(table_is_available(outputs_path_by_table, "remuneracao"), True):
            create_table_and_upload_to_gcs(
                data_path=outputs_path_by_table["remuneracao"],
                dataset_id=dataset_id,
                table_id="remuneracao",
                dump_mode="append",
                wait=outputs_path_by_table,
            )

        with case(table_is_available(outputs_path_by_table, "afastamentos"), True):
            create_table_and_upload_to_gcs(
                data_path=outputs_path_by_table["afastamentos"],
                dataset_id=dataset_id,
                table_id="afastamentos",
                dump_mode="append",
                wait=outputs_path_by_table,
            )

        with case(table_is_available(outputs_path_by_table, "observacoes"), True):
            create_table_and_upload_to_gcs(
                data_path=outputs_path_by_table["observacoes"],
                dataset_id=dataset_id,
                table_id="observacoes",
                dump_mode="append",
                wait=outputs_path_by_table,
            )

        # aposentados_cadastro
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": "aposentados_cadastro",
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.aposentados_cadastro",
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
                    table_id="aposentados_cadastro",
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=True,
                    billing_project_id="basedosdados",
                    api_mode="dev",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )

        # pensionistas_cadastro
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": "pensionistas_cadastro",
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.pensionistas_cadastro",
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
                    table_id="pensionistas_cadastro",
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=True,
                    billing_project_id="basedosdados",
                    api_mode="dev",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )

        # servidores_cadastro
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": "servidores_cadastro",
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.servidores_cadastro",
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
                    table_id="servidores_cadastro",
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=True,
                    billing_project_id="basedosdados",
                    api_mode="dev",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )

        # reserva_reforma_militares_cadastro
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": "reserva_reforma_militares_cadastro",
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.reserva_reforma_militares_cadastro",
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
                    table_id="reserva_reforma_militares_cadastro",
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=True,
                    billing_project_id="basedosdados",
                    api_mode="dev",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )

        # remuneracao
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": "remuneracao",
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.remuneracao",
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
                    table_id="remuneracao",
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=True,
                    billing_project_id="basedosdados",
                    api_mode="dev",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )

        # afastamentos
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": "afastamentos",
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.afastamentos",
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
                    table_id="afastamentos",
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=True,
                    billing_project_id="basedosdados",
                    api_mode="dev",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )

        # observacoes
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": "observacoes",
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=r"Materialize {dataset_id}.observacoes",
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
                    table_id="observacoes",
                    metadata_type="DateTimeRange",
                    bq_last_update=False,
                    bq_table_last_year_month=True,
                    billing_project_id="basedosdados",
                    api_mode="dev",
                    date_format="yy-mm",
                    is_bd_pro=True,
                    is_free=True,
                    time_delta=6,
                    time_unit="months",
                    upstream_tasks=[wait_for_materialization],
                )


datasets_br_cgu_servidores_executivo_federal_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
datasets_br_cgu_servidores_executivo_federal_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_cgu_servidores_executivo_federal_flow.schedule = every_month
