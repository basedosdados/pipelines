# -*- coding: utf-8 -*-
"""
Flows for br_ms_cnes
"""
# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_ms_cnes.constants import constants as br_ms_cnes_constants
from pipelines.datasets.br_ms_cnes.schedules import (
    schedule_br_ms_cnes_dados_complementares,
    schedule_br_ms_cnes_equipamento,
    schedule_br_ms_cnes_equipe,
    schedule_br_ms_cnes_estabelecimento,
    schedule_br_ms_cnes_estabelecimento_ensino,
    schedule_br_ms_cnes_estabelecimento_filantropico,
    schedule_br_ms_cnes_gestao_metas,
    schedule_br_ms_cnes_habilitacao,
    schedule_br_ms_cnes_incentivos,
    schedule_br_ms_cnes_leito,
    schedule_br_ms_cnes_profissional,
    schedule_br_ms_cnes_regra_contratual,
    schedule_br_ms_cnes_servico_especializado,
)
from pipelines.datasets.br_ms_cnes.tasks import (
    access_ftp_donwload_files,
    check_files_to_parse,
    decompress_dbc,
    decompress_dbf,
    is_empty,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_ms_cnes.estabelecimento", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_estabelecimento:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="estabelecimento", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][0],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[0],
        )

        dbf_files = decompress_dbc(file_list=dbc_files, upstream_tasks=[dbc_files])

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[0],
            upstream_tasks=[dbf_files, dbc_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

        # estabelecimento
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
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_ms_cnes_estabelecimento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_estabelecimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_estabelecimento.schedule = schedule_br_ms_cnes_estabelecimento


# profissional
with Flow(
    name="br_ms_cnes.profissional", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_profissional:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="profissional", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)

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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][1],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[1],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[1],
            upstream_tasks=[dbf_files, dbc_files],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=filepath,
        )

        # estabelecimento
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
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_ms_cnes_profissional.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_profissional.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_profissional.schedule = schedule_br_ms_cnes_profissional

# equipe
with Flow(name="br_ms_cnes.equipe", code_owners=["Gabriel Pisa"]) as br_ms_cnes_equipe:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="equipe", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)

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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][4],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[4],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[4],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_ms_cnes_equipe.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_equipe.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_equipe.schedule = schedule_br_ms_cnes_equipe

# leito
with Flow(name="br_ms_cnes.leito", code_owners=["Gabriel Pisa"]) as br_ms_cnes_leito:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="leito", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)

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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][3],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[3],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[3],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_ms_cnes_leito.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_leito.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_leito.schedule = schedule_br_ms_cnes_leito

# equipamento
with Flow(
    name="br_ms_cnes.equipamento", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_equipamento:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="equipamento", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)

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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        # mudar no aqui pra 1
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][2],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        # task para veficiar se for nula
        # e deliberar
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[2],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[2],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_ms_cnes_equipamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_equipamento.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_equipamento.schedule = schedule_br_ms_cnes_equipamento


with Flow(
    name="br_ms_cnes.estabelecimento_ensino", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_estabelecimento_ensino:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="estabelecimento_ensino", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][5],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[5],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[5],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_ms_cnes_estabelecimento_ensino.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_estabelecimento_ensino.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_estabelecimento_ensino.schedule = schedule_br_ms_cnes_estabelecimento_ensino

with Flow(
    name="br_ms_cnes.dados_complementares", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_dados_complementares:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="dados_complementares", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][6],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[6],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[6],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_ms_cnes_dados_complementares.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_dados_complementares.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_dados_complementares.schedule = schedule_br_ms_cnes_dados_complementares

with Flow(
    name="br_ms_cnes.estabelecimento_filantropico", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_estabelecimento_filantropico:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter(
        "table_id", default="estabelecimento_filantropico", required=True
    )
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][7],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[7],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[7],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_ms_cnes_estabelecimento_filantropico.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_estabelecimento_filantropico.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_estabelecimento_filantropico.schedule = (
#    schedule_br_ms_cnes_estabelecimento_filantropico
# )

# gestao_metas
with Flow(
    name="br_ms_cnes.gestao_metas", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_gestao_metas:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="gestao_metas", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][8],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[8],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[8],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_ms_cnes_gestao_metas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_gestao_metas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_gestao_metas.schedule = schedule_br_ms_cnes_gestao_metas

# habilitacao
with Flow(
    name="br_ms_cnes.habilitacao", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_habilitacao:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="habilitacao", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][9],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[9],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[9],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_ms_cnes_habilitacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_habilitacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_habilitacao.schedule = schedule_br_ms_cnes_habilitacao


# incentivos
with Flow(
    name="br_ms_cnes.incentivos", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_incentivos:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="incentivos", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][10],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[10],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[10],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_ms_cnes_incentivos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_incentivos.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_ms_cnes_incentivos.schedule = schedule_br_ms_cnes_incentivos

# regra_contratual
with Flow(
    name="br_ms_cnes.regra_contratual", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_regra_contratual:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="regra_contratual", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][11],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[12],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[12],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )


br_ms_cnes_regra_contratual.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_regra_contratual.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_regra_contratual.schedule = schedule_br_ms_cnes_regra_contratual

# regra_contratual
with Flow(
    name="br_ms_cnes.servico_especializado", code_owners=["Gabriel Pisa"]
) as br_ms_cnes_servico_especializado:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_ms_cnes", required=True)
    table_id = Parameter("table_id", default="servico_especializado", required=True)
    update_metadata = Parameter("update_metadata", default=False, required=False)
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

    files_path = check_files_to_parse(
        dataset_id=dataset_id,
        table_id=table_id,
        cnes_database="CNES",
        cnes_group_file=br_ms_cnes_constants.DATABASE_GROUPS.value["CNES"][12],
    )

    with case(is_empty(files_path), True):
        log_task(
            "Os dados do FTP CNES-ST ainda não foram atualizados para o ano/mes mais recente"
        )

    with case(is_empty(files_path), False):
        dbc_files = access_ftp_donwload_files(
            file_list=files_path,
            path=br_ms_cnes_constants.PATH.value[0],
            table=br_ms_cnes_constants.TABLE.value[12],
        )

        dbf_files = decompress_dbc(file_list=dbc_files)

        filepath = decompress_dbf(
            file_list=dbc_files,
            path=br_ms_cnes_constants.PATH.value[1],
            table=br_ms_cnes_constants.TABLE.value[12],
            upstream_tasks=[dbf_files, dbc_files],
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
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_ms_cnes_servico_especializado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_cnes_servico_especializado.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ms_cnes_servico_especializado.schedule = schedule_br_ms_cnes_servico_especializado
