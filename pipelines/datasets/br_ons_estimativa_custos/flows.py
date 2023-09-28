# -*- coding: utf-8 -*-
"""
Flows for br_ons_estimativa_custos
"""
# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_ons_estimativa_custos.constants import (
    constants as ons_constants,
)
from pipelines.datasets.br_ons_estimativa_custos.schedules import (
    schedule_br_ons_estimativa_custos_balanco_energia_subsistemas,
    schedule_br_ons_estimativa_custos_balanco_energia_subsistemas_dessem,
    schedule_br_ons_estimativa_custos_custo_marginal_operacao_semanal,
    schedule_br_ons_estimativa_custos_custo_marginal_operacao_semi_horario,
)
from pipelines.datasets.br_ons_estimativa_custos.tasks import download_data, wrang_data
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
    update_django_metadata,
)

with Flow(
    name="br_ons_estimativa_custos.custo_marginal_operacao_semi_horario",
    code_owners=["Gabriel Pisa"],
) as br_ons_estimativa_custos_custo_marginal_operacao_semi_horario:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_estimativa_custos", required=True
    )
    table_id = Parameter(
        "table_id", default="custo_marginal_operacao_semi_horario", required=True
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[0],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[0],
        upstream_tasks=[dow_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
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
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm-dd",
                upstream_tasks=[wait_for_materialization],
            )

br_ons_estimativa_custos_custo_marginal_operacao_semi_horario.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_estimativa_custos_custo_marginal_operacao_semi_horario.run_config = (
    KubernetesRun(image=constants.DOCKER_IMAGE.value)
)
br_ons_estimativa_custos_custo_marginal_operacao_semi_horario.schedule = (
    schedule_br_ons_estimativa_custos_custo_marginal_operacao_semi_horario
)


with Flow(
    name="br_ons_estimativa_custos.custo_marginal_operacao_semanal",
    code_owners=["Gabriel Pisa"],
) as br_ons_estimativa_custos_custo_marginal_operacao_semanal:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_estimativa_custos", required=True
    )
    table_id = Parameter(
        "table_id", default="custo_marginal_operacao_semanal", required=True
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[1],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[1],
        upstream_tasks=[dow_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
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
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm-dd",
                upstream_tasks=[wait_for_materialization],
            )

br_ons_estimativa_custos_custo_marginal_operacao_semanal.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_estimativa_custos_custo_marginal_operacao_semanal.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ons_estimativa_custos_custo_marginal_operacao_semanal.schedule = (
    schedule_br_ons_estimativa_custos_custo_marginal_operacao_semanal
)


with Flow(
    name="br_ons_estimativa_custos.balanco_energia_subsistemas",
    code_owners=["Gabriel Pisa"],
) as br_ons_estimativa_custos_balanco_energia_subsistemas:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_estimativa_custos", required=True
    )
    table_id = Parameter(
        "table_id", default="balanco_energia_subsistemas", required=True
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[2],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[2],
        upstream_tasks=[dow_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
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
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm-dd",
                upstream_tasks=[wait_for_materialization],
            )

br_ons_estimativa_custos_balanco_energia_subsistemas.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_estimativa_custos_balanco_energia_subsistemas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ons_estimativa_custos_balanco_energia_subsistemas.schedule = (
    schedule_br_ons_estimativa_custos_balanco_energia_subsistemas
)


with Flow(
    name="br_ons_estimativa_custos.balanco_energia_subsistemas_dessem",
    code_owners=["Gabriel Pisa"],
) as br_ons_estimativa_custos_balanco_energia_subsistemas_dessem:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_estimativa_custos", required=True
    )
    table_id = Parameter(
        "table_id", default="balanco_energia_subsistemas_dessem", required=True
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[3],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[3],
        upstream_tasks=[dow_data],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
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
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                bq_last_update=False,
                bq_table_last_year_month=True,
                billing_project_id="basedosdados",
                api_mode="prod",
                date_format="yy-mm-dd",
                upstream_tasks=[wait_for_materialization],
            )

br_ons_estimativa_custos_balanco_energia_subsistemas_dessem.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_estimativa_custos_balanco_energia_subsistemas_dessem.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ons_estimativa_custos_balanco_energia_subsistemas_dessem.schedule = (
    schedule_br_ons_estimativa_custos_balanco_energia_subsistemas_dessem
)
