# -*- coding: utf-8 -*-
"""
Flows for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long
from datetime import datetime, timedelta

from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.datasets.br_tse_eleicoes.tasks import (
    download_before22,
    get_csv_files,
    build_candidatos,
    clean_candidatos22,
    build_bens_candidato,
    clean_bens22,
    clean_despesa22,
    clean_receita22,
)
from pipelines.datasets.br_tse_eleicoes.schedules import (
    schedule_bens,
    schedule_candidatos,
    schedule_despesa,
    schedule_receita,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
    update_metadata,
    get_current_flow_labels,
)

from pipelines.datasets.br_tse_eleicoes.constants import constants as tse_constants

with Flow(
    name="br_tse_eleicoes.candidatos", code_owners=["lucas_cr"]
) as br_tse_candidatos:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_tse_eleicoes", required=True)
    table_id = Parameter("table_id", default="candidatos", required=True)
    start = Parameter("start", default=2018, required=True)
    id_candidato_bd = Parameter("id_candidato_bd", default=False, required=True)

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

    with case(id_candidato_bd, True):
        d22_task = download_before22(
            table_id=table_id, start=start, upstream_tasks=[rename_flow_run]
        )

        gfiles_task = get_csv_files(
            url=tse_constants.CANDIDATOS22_ZIP.value,
            save_path="/tmp/data/",
            upstream_tasks=[d22_task],
        )

        c22_task = clean_candidatos22("/tmp/data/input", upstream_tasks=[gfiles_task])

        filepath = build_candidatos(
            "/tmp/data/raw/br_tse_eleicoes/candidatos",
            start=start,
            end=2022,
            id_candidato_bd=id_candidato_bd,
            upstream_tasks=[c22_task],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
            wait=filepath,
        )

    with case(id_candidato_bd, False):
        gfiles_task = get_csv_files(
            url=tse_constants.CANDIDATOS22_ZIP.value,
            save_path="/tmp/data/",
            mkdir=True,
            upstream_tasks=[rename_flow_run],
        )

        c22_task = clean_candidatos22("/tmp/data/input", upstream_tasks=[gfiles_task])

        filepath = build_candidatos(
            "/tmp/data/raw/br_tse_eleicoes/candidatos",
            start=start,
            end=2022,
            id_candidato_bd=id_candidato_bd,
            upstream_tasks=[c22_task],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
        upstream_tasks=[wait_upload_table],
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

br_tse_candidatos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_candidatos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_candidatos.schedule = schedule_candidatos


with Flow(
    name="br_tse_eleicoes.bens_candidato", code_owners=["lucas_cr"]
) as br_tse_bens:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_tse_eleicoes", required=True)
    table_id = Parameter("table_id", default="bens_candidato", required=True)
    start = Parameter("start", default=2018, required=True)
    id_candidato_bd = Parameter("id_candidato_bd", default=False, required=True)

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

    with case(id_candidato_bd, True):
        d22_task = download_before22(table_id=table_id, start=start)

        gfiles_task = get_csv_files(
            url=tse_constants.BENS22_ZIP.value,
            save_path="/tmp/data/",
            upstream_tasks=[d22_task],
        )

        c22_task = clean_bens22("/tmp/data/input", upstream_tasks=[gfiles_task])

        filepath = build_bens_candidato(
            "/tmp/data/raw/br_tse_eleicoes/bens_candidato",
            start=start,
            end=2022,
            id_candidato_bd=id_candidato_bd,
            upstream_tasks=[c22_task],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
            wait=filepath,
        )

    with case(id_candidato_bd, False):
        gfiles_task = get_csv_files(
            url=tse_constants.BENS22_ZIP.value, save_path="/tmp/data/", mkdir=True
        )

        c22_task = clean_bens22("/tmp/data/input", upstream_tasks=[gfiles_task])

        filepath = build_bens_candidato(
            "/tmp/data/raw/br_tse_eleicoes/bens_candidato",
            start=start,
            end=2022,
            id_candidato_bd=id_candidato_bd,
            upstream_tasks=[c22_task],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
        upstream_tasks=[wait_upload_table],
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

br_tse_bens.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_bens.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_bens.schedule = schedule_bens


with Flow(
    name="br_tse_eleicoes.despesas_candidato", code_owners=["lucas_cr"]
) as br_tse_despesa:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_tse_eleicoes", required=True)
    table_id = Parameter("table_id", default="despesas_candidato", required=True)
    id_candidato_bd = Parameter("id_candidato_bd", default=False, required=True)

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

    with case(id_candidato_bd, False):
        gfiles_task = get_csv_files(
            url=tse_constants.CONTAS22_ZIP.value,
            save_path="/tmp/data/",
            mkdir=True,
            upstream_tasks=[rename_flow_run],
        )

        filepath = clean_despesa22("/tmp/data/input", upstream_tasks=[gfiles_task])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
        upstream_tasks=[wait_upload_table],
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

br_tse_despesa.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_despesa.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_despesa.schedule = schedule_despesa


with Flow(
    name="br_tse_eleicoes.receitas_candidato", code_owners=["lucas_cr"]
) as br_tse_receita:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_tse_eleicoes", required=True)
    table_id = Parameter("table_id", default="receitas_candidato", required=True)
    id_candidato_bd = Parameter("id_candidato_bd", default=False, required=True)

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

    with case(id_candidato_bd, False):
        gfiles_task = get_csv_files(
            url=tse_constants.CONTAS22_ZIP.value,
            save_path="/tmp/data/",
            mkdir=True,
            upstream_tasks=[rename_flow_run],
        )

        filepath = clean_receita22("/tmp/data/input", upstream_tasks=[gfiles_task])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
        ],
        upstream_tasks=[wait_upload_table],
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

br_tse_receita.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_tse_receita.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_tse_receita.schedule = schedule_receita
