# -*- coding: utf-8 -*-

# register flow in prefect

from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_camara_dados_abertos.constants import (
    constants as constants_camara,
)
from pipelines.datasets.br_camara_dados_abertos.schedules import (
    every_day_camara_dados_abertos,
    every_day_camara_dados_abertos_deputados,
)
from pipelines.datasets.br_camara_dados_abertos.tasks import (
    download_files_and_get_max_date,
    download_files_and_get_max_date_deputados,
    make_partitions,
    output_path_list,
    save_data_proposicao,
    treat_and_save_table,
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

with Flow(name="br_camara_dados_abertos.votacao", code_owners=["trick"]) as br_camara:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_camara_dados_abertos", required=True
    )
    table_id = Parameter(
        "table_id",
        default=[
            "votacao_microdados",
            "votacao_objeto",
            "votacao_orientacao_bancada",
            "voto_parlamentar",
            "votacao_proposicao_afetada",
        ],
        required=True,
    )

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id[0],
        wait=table_id[0],
    )

    update_metadata = Parameter("update_metadata", default=True, required=False)
    data_source_max_date = download_files_and_get_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id[0],
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, False):
        log_task("Não há atualizações!")

    with case(dados_desatualizados, True):
        # ! ---------------------------------------- >   Votacao - Microdados

        filepath_microdados = make_partitions(
            table_id="votacao_microdados",
            date_column="data",
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_microdados,
            dataset_id=dataset_id,
            table_id=table_id[0],
            dump_mode="append",
            wait=filepath_microdados,
        )
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[0],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[0]}",
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
                table_id[0],
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        # ! ---------------------------------------- >   Votacao - objeto

        filepath_objeto = make_partitions(
            table_id="votacao_objeto",
            date_column="data",
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_objeto,
            dataset_id=dataset_id,
            table_id=table_id[1],
            dump_mode="append",
            wait=filepath_objeto,
        )
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[1],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[1]}",
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
                table_id[1],
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        # ! ---------------------------------------- >   Votacao - Parlamentar

        filepath_parlamentar = make_partitions(
            table_id="voto_parlamentar",
            date_column="dataHoraVoto",
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_parlamentar,
            dataset_id=dataset_id,
            table_id=table_id[3],
            dump_mode="append",
            wait=filepath_parlamentar,
        )
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[3],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[3]}",
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
                table_id[3],
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        # ! ---------------------------------------- >   Votacao - Proposicao

        filepath_proposicao = make_partitions(
            table_id="votacao_proposicao_afetada",
            date_column="data",
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_proposicao,
            dataset_id=dataset_id,
            table_id=table_id[4],
            dump_mode="append",
            wait=filepath_proposicao,
        )
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[4],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[4]}",
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
                table_id[4],
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        # ! ---------------------------------------- >   Votacao - Orientacao -- Não tem data

        filepath_orientacao = make_partitions(
            table_id="votacao_orientacao_bancada",
            date_column="data",
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_orientacao,
            dataset_id=dataset_id,
            table_id=table_id[2],
            dump_mode="append",
            wait=filepath_orientacao,
        )
        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[2],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[2]}",
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

        # ! ---------------------------------------------------> Não tem data. Precisaremos usar o historical_database
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id,
                table_id[2],
                date_format="%Y-%m",
                coverage_type="all_free",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                historical_database=False,
                upstream_tasks=[wait_for_materialization],
            )

br_camara.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara.schedule = every_day_camara_dados_abertos


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

# ------------------------------ TABLES DEPUTADOS ---------------------------------------

with Flow(
    name="br_camara_dados_abertos.deputado", code_owners=["trick"]
) as br_camara_deputado:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_camara_dados_abertos", required=True
    )
    table_id = Parameter(
        "table_id",
        default=[
            "deputado",
            "deputado_ocupacao",
            "deputado_profissao",
        ],
        required=True,
    )

    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id[0],
        wait=table_id[0],
    )

    update_metadata = Parameter("update_metadata", default=True, required=False)
    data_source_max_date_deputado = download_files_and_get_max_date_deputados()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id[2],
        data_source_max_date=data_source_max_date_deputado,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date_deputado],
    )

    with case(dados_desatualizados, False):
        log_task("Não há atualizações!")

    with case(dados_desatualizados, True):
        # --------------------------------------- > Deputados

        filepath_deputados = treat_and_save_table(
            table_id="deputados", upstream_tasks=[rename_flow_run]
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_deputados,
            dataset_id=dataset_id,
            table_id=table_id[0],
            dump_mode="append",
            wait=filepath_deputados,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[0],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[0]}",
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
                table_id[0],
                coverage_type="all_free",
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                historical_database=False,
                upstream_tasks=[wait_for_materialization],
            )

        # ----------------------------------------------> Deputados - Ocupacao

        filepath_deputados_ocupacao = treat_and_save_table(
            table_id="deputado_ocupacao", upstream_tasks=[filepath_deputados]
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_deputados_ocupacao,
            dataset_id=dataset_id,
            table_id=table_id[1],
            dump_mode="append",
            wait=filepath_deputados_ocupacao,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[1],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[1]}",
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
                table_id[1],
                coverage_type="all_free",
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                historical_database=False,
                upstream_tasks=[wait_for_materialization],
            )

        # ----------------------------------------------> Deputados - Profissão

        filepath_deputados_profissao = treat_and_save_table(
            table_id="deputado_profissao", upstream_tasks=[filepath_deputados_ocupacao]
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_deputados_profissao,
            dataset_id=dataset_id,
            table_id=table_id[2],
            dump_mode="append",
            wait=filepath_deputados_profissao,
        )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialization_flow = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id[2],
                    "mode": materialization_mode,
                    "dbt_alias": dbt_alias,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id[2]}",
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
                table_id[2],
                date_format="%Y-%m-%d",
                date_column_name={"date": "data"},
                coverage_type="all_free",
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                historical_database=True,
                upstream_tasks=[wait_for_materialization],
            )

br_camara_deputado.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_deputado.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_camara_deputado.schedule = every_day_camara_dados_abertos_deputados


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

# ------------------------------ TABLES PROPOSIÇÃO ---------------------------------------

with Flow(
    name="br_camara_dados_abertos.proposicao", code_owners=["trick"]
) as br_camara_proposicao:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_camara_dados_abertos", required=True
    )
    table_id = Parameter(
        "table_id",
        default=[
            "proposicao_microdados",
            "proposicao_autor",
            "proposicao_tema",
        ],
        required=True,
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id="Proposição",
        wait=table_id,
    )

    update_metadata = Parameter("update_metadata", default=True, required=False)

    filepath = save_data_proposicao.map(
        table_id=table_id,
    )
    output_path_list = output_path_list(table_id)
    wait_upload_table = create_table_and_upload_to_gcs.map(
        data_path=output_path_list,
        dataset_id=unmapped(dataset_id),
        table_id=table_id,
        dump_mode=unmapped("append"),
        wait=unmapped(output_path_list),
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run.map(
            flow_name=unmapped(utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value),
            project_name=unmapped(constants.PREFECT_DEFAULT_PROJECT.value),
            parameters={
                "dataset_id": unmapped(dataset_id),
                "table_id": table_id,
                "mode": unmapped(materialization_mode),
                "dbt_alias": unmapped(dbt_alias),
            },
            labels=unmapped(current_flow_labels),
            run_name=unmapped(f"Materialize {dataset_id}.{table_id}"),
        )

        wait_for_materialization = wait_for_flow_run.map(
            materialization_flow,
            stream_states=unmapped(True),
            stream_logs=unmapped(True),
            raise_final_state=unmapped(True),
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
            table_id=table_id,
            coverage_type="all_free",
            prefect_mode=unmapped(materialization_mode),
            bq_project=unmapped("basedosdados"),
            historical_database=False,
        )

br_camara_proposicao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_camara_proposicao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_camara_proposicao.schedule = every_day_camara_dados_abertos_deputados
