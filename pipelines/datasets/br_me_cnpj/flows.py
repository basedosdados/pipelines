# -*- coding: utf-8 -*-
"""
Flows for br_me_cnpj
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_me_cnpj.constants import constants as constants_cnpj
from pipelines.datasets.br_me_cnpj.schedules import (
    every_day_empresas,
    every_day_estabelecimentos,
    every_day_simples,
    every_day_socios,
)
from pipelines.datasets.br_me_cnpj.tasks import get_data_source_max_date, main
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

with Flow(
    name="br_me_cnpj.empresas",
    code_owners=[
        "lauris",
    ],
) as br_me_cnpj_empresas:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=True)
    table_id = Parameter("table_id", default="empresas", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    tabelas = constants_cnpj.TABELAS.value[0:1]

    data_source_max_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(tabelas)
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )


br_me_cnpj_empresas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_empresas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_me_cnpj_empresas.schedule = every_day_empresas

with Flow(
    name="br_me_cnpj.socios",
    code_owners=[
        "lauris",
    ],
) as br_me_cnpj_socios:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=True)
    table_id = Parameter("table_id", default="socios", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    tabelas = constants_cnpj.TABELAS.value[1:2]

    data_source_max_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(tabelas)
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

br_me_cnpj_socios.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_socios.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_me_cnpj_socios.schedule = every_day_socios


with Flow(
    name="br_me_cnpj.estabelecimentos",
    code_owners=[
        "lauris",
    ],
) as br_me_cnpj_estabelecimentos:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=True)
    table_id = Parameter("table_id", default="estabelecimentos", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    tabelas = constants_cnpj.TABELAS.value[2:3]

    data_source_max_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(tabelas)

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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        with case(materialize_after_dump, True):
            # Trigger DBT flow run
            current_flow_labels = get_current_flow_labels()
            materialize_second = create_flow_run(
                flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "dataset_id": "br_bd_diretorios_brasil",
                    "table_id": "empresa",
                    "mode": materialization_mode,
                    "dbt_alias": True,
                },
                labels=current_flow_labels,
                run_name=f"Materialize {dataset_id}.{table_id}",
            )
            materialize_second.set_upstream([materialization_flow])
            wait_for_materialization = wait_for_flow_run(
                materialize_second,
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

            update_django_metadata(
                dataset_id="br_bd_diretorios_brasil",
                table_id="empresa",
                date_column_name={"date": "data"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )


br_me_cnpj_estabelecimentos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_estabelecimentos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_estabelecimentos.schedule = every_day_estabelecimentos


with Flow(
    name="br_me_cnpj.simples",
    code_owners=[
        "lauris",
    ],
) as br_me_cnpj_simples:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=True)
    table_id = Parameter("table_id", default="simples", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    tabelas = constants_cnpj.TABELAS.value[3:]

    data_source_max_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id="br_me_cnpj",
        table_id="estabelecimentos",
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(tabelas)
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage_type="all_free",
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                historical_database=False,
                # upstream_tasks=[wait_for_materialization],
            )

br_me_cnpj_simples.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_simples.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_me_cnpj_simples.schedule = every_day_simples
