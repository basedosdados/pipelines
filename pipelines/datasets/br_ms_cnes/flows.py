# -*- coding: utf-8 -*-
"""
Flows for br_ms_cnes
"""
# pylint: disable=invalid-name
from datetime import datetime, timedelta

from prefect import Parameter, case
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.datasets.br_ms_cnes.constants import constants as br_ms_cnes_constants
from pipelines.datasets.br_ms_cnes.tasks import (
    parse_latest_cnes_dbc_files,
    access_ftp_donwload_files,
    read_dbc_save_csv,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)

from pipelines.datasets.br_ms_cnes.schedules import (
    schedule_br_ms_cnes_estabelecimento,
    schedule_br_ms_cnes_profissional,
)

with Flow(
    name="br_ms_cnes.estabelecimento", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_estabelecimento:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="estabelecimento", required=True)

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

    files_path = parse_latest_cnes_dbc_files(
        database="CNES",
        cnes_group=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][0],
    )

    dbc_files = access_ftp_donwload_files(
        file_list=files_path,
        path=br_ms_cnes_constants.PATH.value[0],
        table=br_ms_cnes_constants.TABLE.value[0],
        upstream_tasks=[files_path],
    )

    filepath = read_dbc_save_csv(
        file_list=dbc_files,
        path=br_ms_cnes_constants.PATH.value[1],
        table=br_ms_cnes_constants.TABLE.value[0],
        upstream_tasks=[files_path, dbc_files],
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

br_ms_cnes_estabelecimento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_estabelecimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ms_cnes_estabelecimento.schedule = schedule_br_ms_cnes_estabelecimento


# profissional
with Flow(
    name="br_ms_cnes.profissional", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_profissional:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="profissional", required=True)

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

    files_path = parse_latest_cnes_dbc_files(
        database="CNES",
        cnes_group=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][1],
    )

    dbc_files = access_ftp_donwload_files(
        file_list=files_path,
        path=br_ms_cnes_constants.PATH.value[0],
        table=br_ms_cnes_constants.TABLE.value[1],
        upstream_tasks=[files_path],
    )

    filepath = read_dbc_save_csv(
        file_list=dbc_files,
        path=br_ms_cnes_constants.PATH.value[1],
        table=br_ms_cnes_constants.TABLE.value[1],
        upstream_tasks=[files_path, dbc_files],
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

br_ms_cnes_profissional.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_profissional.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_ms_cnes_profissional.schedule = schedule_br_ms_cnes_profissional

# leito
with Flow(name="br_ms_cnes.leito", code_owners=["Gabriel Pisa"]) as br_ms_cnes_leito:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="leito", required=True)

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

    files_path = parse_latest_cnes_dbc_files(
        database="CNES",
        cnes_group=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][3],
    )

    dbc_files = access_ftp_donwload_files(
        file_list=files_path,
        path=br_ms_cnes_constants.PATH.value[0],
        table=br_ms_cnes_constants.TABLE.value[3],
        upstream_tasks=[files_path],
    )

    filepath = read_dbc_save_csv(
        file_list=dbc_files,
        path=br_ms_cnes_constants.PATH.value[1],
        table=br_ms_cnes_constants.TABLE.value[3],
        upstream_tasks=[files_path, dbc_files],
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
br_ms_cnes_leito.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_leito.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_leito.schedule = none

# equipamento
with Flow(
    name="br_ms_cnes.equipamento", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_equipamento:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="equipamento", required=True)

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

    files_path = parse_latest_cnes_dbc_files(
        database="CNES",
        cnes_group=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][2],
    )

    dbc_files = access_ftp_donwload_files(
        file_list=files_path,
        path=br_ms_cnes_constants.PATH.value[0],
        table=br_ms_cnes_constants.TABLE.value[2],
        upstream_tasks=[files_path],
    )

    filepath = read_dbc_save_csv(
        file_list=dbc_files,
        path=br_ms_cnes_constants.PATH.value[1],
        table=br_ms_cnes_constants.TABLE.value[2],
        upstream_tasks=[files_path, dbc_files],
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
br_ms_cnes_equipamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_equipamento.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

# equipe
with Flow(name="br_ms_cnes.equipe", code_owners=["Gabriel Pisa"]) as br_ms_cnes_equipe:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="equipe", required=True)

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

    files_path = parse_latest_cnes_dbc_files(
        database="CNES",
        cnes_group=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][4],
    )

    dbc_files = access_ftp_donwload_files(
        file_list=files_path,
        path=br_ms_cnes_constants.PATH.value[0],
        table=br_ms_cnes_constants.TABLE.value[4],
        upstream_tasks=[files_path],
    )

    filepath = read_dbc_save_csv(
        file_list=dbc_files,
        path=br_ms_cnes_constants.PATH.value[1],
        table=br_ms_cnes_constants.TABLE.value[4],
        upstream_tasks=[files_path, dbc_files],
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
br_ms_cnes_equipe.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_equipe.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
