# -*- coding: utf-8 -*-
"""
Flows for dataset br_anatel_telefonia_movel
"""

from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_anatel_telefonia_movel.constants import (
    constants as anatel_constants,
)
from pipelines.datasets.br_anatel_telefonia_movel.schedules import every_month_anatel
from pipelines.datasets.br_anatel_telefonia_movel.tasks import (
    clean_csv_brasil,
    clean_csv_microdados,
    clean_csv_municipio,
    clean_csv_uf,
    setting_data_url,
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

with Flow(name="br_anatel_telefonia_movel", code_owners=["tricktx"]) as br_anatel:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_anatel_telefonia_movel", required=True
    )
    table_id = Parameter(
        "table_id",
        default=[
            "microdados",
            "densidade_brasil",
            "densidade_uf",
            "densidade_municipio",
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

    # ! as variáveis ano, mes_um, mes_dois é criada aqui e cria um objeto 'Parameter' no Prefect Cloud chamado 'ano', 'mes_um', 'mes_dois'
    # ! Importante salientar que o mes_um sempre será 01 ou 06 e o mes_dois será 07 ou 12
    anos = Parameter("anos", default=2023, required=True)

    mes_um = Parameter("mes_um", default="07", required=True)
    mes_dois = Parameter("mes_dois", default="12", required=True)
    update_metadata = Parameter("update_metadata", default=True, required=False)

    data_source_max_date = setting_data_url()
    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id[0],
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m",  # Alterado aqui
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, False):
        log_task("Não há atualizações!")

    with case(dados_desatualizados, True):
        # ! MICRODADOS
        filepath_microdados = clean_csv_microdados(
            anos=anos,
            mes_um=mes_um,
            mes_dois=mes_dois,
            upstream_tasks=[rename_flow_run],
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_microdados,
            dataset_id=dataset_id,
            table_id=table_id[0],
            dump_mode="append",
            wait=filepath_microdados,
        )

        # ! tabela bd +
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
                dataset_id=dataset_id,
                table_id=table_id[0],
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        # ! BRASIL
        filepath_brasil = clean_csv_brasil(upstream_tasks=[filepath_microdados])
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_brasil,
            dataset_id=dataset_id,
            table_id=table_id[1],
            dump_mode="append",
            wait=filepath_brasil,
        )
        # ! tabela bd +
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
                dataset_id=dataset_id,
                table_id=table_id[1],
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        # ! UF

        filepath_uf = clean_csv_uf(upstream_tasks=[filepath_microdados])
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_uf,
            dataset_id=dataset_id,
            table_id=table_id[2],
            dump_mode="append",
            wait=filepath_uf,
        )

        # ! tabela bd +
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
                dataset_id=dataset_id,
                table_id=table_id[2],
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=materialization_mode,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_materialization],
            )

        # ! MUNICIPIO
        filepath_municipio = clean_csv_municipio(upstream_tasks=[filepath_microdados])
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath_municipio,
            dataset_id=dataset_id,
            table_id=table_id[3],
            dump_mode="append",
            wait=filepath_municipio,
        )

        # ! tabela bd +
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
                    dataset_id=dataset_id,
                    table_id=table_id[3],
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

br_anatel.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_anatel.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_anatel.schedule = every_month_anatel
