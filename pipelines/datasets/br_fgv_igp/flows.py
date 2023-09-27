# -*- coding: utf-8 -*-
"""
Flows for br_fgv_igp
"""

# pylint: disable=invalid-name
from datetime import timedelta
from pathlib import Path

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_fgv_igp.schedules import (
    igp_10_mes,
    igp_di_ano,
    igp_di_mes,
    igp_m_ano,
    igp_m_mes,
    igp_og_ano,
    igp_og_mes,
)
from pipelines.datasets.br_fgv_igp.tasks import clean_fgv_df, crawler_fgv
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    get_temporal_coverage,
    rename_current_flow_run_dataset_table,
    update_metadata,
)

ROOT = Path("tmp/data")

with Flow(
    name="IGP-DI mensal",
    code_owners=[],
) as fgv_igpdi_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPDI", required=True)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp", required=True)
    table_id = Parameter("table_id", default="igp_di_mes", required=True)
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
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )


fgv_igpdi_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpdi_mes_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igpdi_mes_flow.schedule = igp_di_mes


with Flow(
    name="IGP-DI anual",
    code_owners=[],
) as fgv_igpdi_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPDI", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_di_ano")
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
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpdi_ano_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpdi_ano_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igpdi_ano_flow.schedule = igp_di_ano


with Flow(
    name="IGP-M mensal",
    code_owners=[],
) as fgv_igpm_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPM", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_m_mes")
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
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpm_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpm_mes_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igpm_mes_flow.schedule = igp_m_mes


with Flow(
    name="IGP-M anual",
    code_owners=[],
) as fgv_igpm_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPM", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_m_ano")
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
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpm_ano_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpm_ano_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igpm_ano_flow.schedule = igp_m_ano


with Flow(
    name="IGP-OG mensal",
    code_owners=[],
) as fgv_igpog_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPOG", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_og_mes")
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
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpog_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpog_mes_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igpog_mes_flow.schedule = igp_og_mes


with Flow(
    name="IGP-OG anual",
    code_owners=[],
) as fgv_igpog_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPOG", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_og_ano")
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
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpog_ano_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpog_ano_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igpog_ano_flow.schedule = igp_og_ano


with Flow(
    name="IGP-10 mensal",
    code_owners=[],
) as fgv_igp10_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGP10", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_10_mes")
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
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
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

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igp10_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igp10_mes_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
fgv_igp10_mes_flow.schedule = igp_10_mes
