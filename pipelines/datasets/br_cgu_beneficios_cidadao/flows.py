# -*- coding: utf-8 -*-
"""
Flows for br_cgu_bolsa_familia
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.datasets.br_cgu_beneficios_cidadao.constants import constants as constants_cgu
from pipelines.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.schedules import (
    every_day_bpc,
    every_day_garantia_safra,
    every_day_novo_bolsa_familia,
)
from pipelines.datasets.br_cgu_beneficios_cidadao.tasks import (
    get_source_max_date,
    crawler_bolsa_familia,
    crawler_bpc,
    crawler_garantia_safra,
    scrape_download_page,
    get_updated_files,
    crawler_beneficios_cidadao,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
    task_get_api_most_recent_date,
)
from pipelines.utils.tasks import (  # update_django_metadata,
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

###                 ###
#  NOVO BOLSA FAMÍLIA     #
###                 ###
with Flow(
    name="br_cgu_beneficios_cidadao.novo_bolsa_familia",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_cgu_bolsa_familia_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )
    table_id = Parameter("table_id", default="novo_bolsa_familia", required=False)

    historical_data = Parameter("historical_data", default=True, required=False)

    update_metadata = Parameter("update_metadata", default=False, required=False)
    url = Parameter("url", default=constants_cgu.MAIN_URL_NOVO_BOLSA_FAMILIA.value, required=False)
    year = Parameter("year", default="2023", required=False)

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

    files_and_dates_dataframe = scrape_download_page(table_id= table_id)

    source_max_date = get_source_max_date(files_df=files_and_dates_dataframe, upstream_tasks=[files_and_dates_dataframe])

    update = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_max_date[0],
        date_format="%Y-%m",
        upstream_tasks=[source_max_date,files_and_dates_dataframe ],
    )

    with case(update, True):
        table_last_date = task_get_api_most_recent_date(dataset_id = dataset_id,
                                                        table_id = table_id,
                                                        date_format ="%Y-%m",
                                                        upstream_tasks=[update])

        download_files_list = get_updated_files(files_df = files_and_dates_dataframe,
                                                table_last_date = table_last_date,
                                                upstream_tasks=[table_last_date])

        output_filepath = crawler_beneficios_cidadao(
            table_id = table_id,
            url = url,
            files_df = files_and_dates_dataframe,
            historical_data=historical_data,
            files=download_files_list,
            year=year,
            upstream_tasks=[download_files_list,table_last_date],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            source_format="parquet",
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
                    "dbt_command": "run/test",
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
                    date_column_name={
                        "year": "ano_competencia",
                        "month": "mes_competencia",
                    },
                    date_format="%Y-%m",
                    coverage_type="all_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

datasets_br_cgu_bolsa_familia_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_cgu_bolsa_familia_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_cgu_bolsa_familia_flow.schedule = every_day_novo_bolsa_familia


###                 ###
#  GARANTIA SAFRA     #
###                 ###

with Flow(
    name="br_cgu_beneficios_cidadao.garantia_safra",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_cgu_garantia_safra_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )

    table_id = Parameter("table_id", default="garantia_safra", required=False)

    historical_data = Parameter("historical_data", default=False, required=False)

    update_metadata = Parameter("update_metadata", default=False, required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    url = Parameter("url", default=constants_cgu.MAIN_URL_GARANTIA_SAFRA.value, required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    year = Parameter("year", default="2023", required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    files_and_dates_dataframe = scrape_download_page(table_id= table_id)

    source_max_date = get_source_max_date(files_df=files_and_dates_dataframe, upstream_tasks=[files_and_dates_dataframe])

    update = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_max_date[0],
        date_format="%Y-%m",
        upstream_tasks=[source_max_date,files_and_dates_dataframe ],
    )

    with case(update, True):
        table_last_date = task_get_api_most_recent_date(dataset_id = dataset_id,
                                                        table_id = table_id,
                                                        date_format ="%Y-%m",
                                                        upstream_tasks=[update])

        download_files_list = get_updated_files(files_df = files_and_dates_dataframe,
                                                table_last_date = table_last_date,
                                                upstream_tasks=[table_last_date])

        output_filepath = crawler_beneficios_cidadao(
            table_id = table_id,
            url = url,
            files_df = files_and_dates_dataframe,
            historical_data=historical_data,
            files=download_files_list,
            year=year,
            upstream_tasks=[download_files_list,table_last_date],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            source_format="parquet",
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
                    "dbt_command": "run/test",
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
                    date_column_name={
                        "year": "ano_referencia",
                        "month": "mes_referencia",
                    },
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                )

datasets_br_cgu_garantia_safra_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_cgu_garantia_safra_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_cgu_garantia_safra_flow.schedule = every_day_garantia_safra
###                 ###
#  BPC     #
###                 ###

with Flow(
    name="br_cgu_beneficios_cidadao.bpc",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_cgu_bpc_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )
    table_id = Parameter("table_id", default="bpc", required=False)
    historical_data = Parameter("historical_data", default=False, required=False)
    url = Parameter("url", default=constants_cgu.MAIN_URL_BPC.value, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    year = Parameter("year", default="2023", required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    files_and_dates_dataframe = scrape_download_page(table_id= table_id)

    source_max_date = get_source_max_date(files_df=files_and_dates_dataframe, upstream_tasks=[files_and_dates_dataframe])

    update = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=source_max_date[0],
        date_format="%Y-%m",
        upstream_tasks=[source_max_date,files_and_dates_dataframe ],
    )

    with case(update, True):
        table_last_date = task_get_api_most_recent_date(dataset_id = dataset_id,
                                                        table_id = table_id,
                                                        date_format ="%Y-%m",
                                                        upstream_tasks=[update])

        download_files_list = get_updated_files(files_df = files_and_dates_dataframe,
                                                table_last_date = table_last_date,
                                                upstream_tasks=[table_last_date])

        output_filepath = crawler_beneficios_cidadao(
            table_id = table_id,
            url = url,
            files_df = files_and_dates_dataframe,
            historical_data=historical_data,
            files=download_files_list,
            year=year,
            upstream_tasks=[download_files_list,table_last_date],
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            source_format="parquet",
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
                    "dbt_command": "run/test",
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
                    date_column_name={
                        "year": "ano_competencia",
                        "month": "mes_competencia",
                    },
                    date_format="%Y-%m",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=materialization_mode,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_materialization],
                )

datasets_br_cgu_bpc_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_cgu_bpc_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
datasets_br_cgu_bpc_flow.schedule = every_day_bpc
