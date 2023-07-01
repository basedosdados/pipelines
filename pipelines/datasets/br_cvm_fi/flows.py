# -*- coding: utf-8 -*-
# """
# Flows for br_cvm_fi

# """

# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
# from datetime import timedelta
# from pipelines.datasets.br_cvm_fi.tasks import (
#     extract_links_and_dates,
#     check_for_updates,
#     is_empty,
#     download_unzip_csv,
#     clean_data_and_make_partitions,
#     clean_data_make_partitions_cda,
#     download_csv_cvm,
#     clean_data_make_partitions_ext,
#     check_for_updates_ext,
#     clean_data_make_partitions_perfil,
#     clean_data_make_partitions_cad,
#     clean_data_make_partitions_balancete,
# )
# from pipelines.datasets.br_cvm_fi.schedules import (
#     every_day_informe,
#     every_day_carteiras,
#     every_day_balancete,
#     every_day_extratos,
#     every_day_informacao_cadastral,
#     every_day_perfil,
# )
# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
# from pipelines.utils.decorators import Flow
# from prefect import Parameter, case
# from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
# from pipelines.utils.constants import constants as utils_constants
# from pipelines.datasets.br_cvm_fi.constants import constants as cvm_constants
# from pipelines.constants import constants
# from pipelines.utils.utils import (
#     log,
# )
# from pipelines.utils.tasks import (
#     create_table_and_upload_to_gcs,
#     rename_current_flow_run_dataset_table,
#     get_current_flow_labels,
# )

# # rom pipelines.datasets.br_cvm_fi.schedules import every_day_cvm

# with Flow(
#     name="br_cvm_fi_documentos_informe_diario",
#     code_owners=[
#         "arthurfg",
#     ],
# ) as br_cvm_fi_documentos_informe_diario:
#     # Parameters
#     dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=True)
#     table_id = Parameter("table_id", default="documentos_informe_diario", required=True)
#     materialization_mode = Parameter(
#         "materialization_mode", default="dev", required=True
#     )
#     materialize_after_dump = Parameter(
#         "materialize_after_dump", default=False, required=True
#     )
#     dbt_alias = Parameter("dbt_alias", default=False, required=False)

#     url = Parameter(
#         "url",
#         default=cvm_constants.INFORME_DIARIO_URL.value,
#         required=True,
#     )
#     df = extract_links_and_dates(url)
#     log(f"Links e datas: {df}")
#     arquivos = check_for_updates(df, upstream_tasks=[df])
#     log(f"Arquivos: {arquivos}")
#     with case(is_empty(arquivos), True):
#         log(f"Não houveram atualizações em {url.default}!")

#     with case(is_empty(arquivos), False):
#         input_filepath = download_unzip_csv(
#             files=arquivos, url=url, id=table_id, upstream_tasks=[arquivos]
#         )
#         output_filepath = clean_data_and_make_partitions(
#             path=input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
#         )

#         rename_flow_run = rename_current_flow_run_dataset_table(
#             prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
#         )

#         wait_upload_table = create_table_and_upload_to_gcs(
#             data_path=output_filepath,
#             dataset_id=dataset_id,
#             table_id=table_id,
#             dump_mode="append",
#             wait=output_filepath,
#         )
#         with case(materialize_after_dump, True):
#             # Trigger DBT flow run
#             current_flow_labels = get_current_flow_labels()
#             materialization_flow = create_flow_run(
#                 flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
#                 project_name=constants.PREFECT_DEFAULT_PROJECT.value,
#                 parameters={
#                     "dataset_id": dataset_id,
#                     "table_id": table_id,
#                     "mode": materialization_mode,
#                     "dbt_alias": dbt_alias,
#                 },
#                 labels=current_flow_labels,
#                 run_name=f"Materialize {dataset_id}.{table_id}",
#             )

#             wait_for_materialization = wait_for_flow_run(
#                 materialization_flow,
#                 stream_states=True,
#                 stream_logs=True,
#                 raise_final_state=True,
#             )
#             wait_for_materialization.max_retries = (
#                 dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
#             )
#             wait_for_materialization.retry_delay = timedelta(
#                 seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
#             )


# br_cvm_fi_documentos_informe_diario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# br_cvm_fi_documentos_informe_diario.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value
# )
# br_cvm_fi_documentos_informe_diario.schedule = every_day_informe


# with Flow(
#     name="br_cvm_fi_documentos_carteiras_fundos_investimento",
#     code_owners=[
#         "arthurfg",
#     ],
# ) as br_cvm_fi_documentos_carteiras_fundos_investimento:
#     # Parameters
#     dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=False)
#     table_id = Parameter(
#         "table_id", default="documentos_carteiras_fundos_investimento", required=False
#     )

#     materialization_mode = Parameter(
#         "materialization_mode", default="dev", required=False
#     )
#     materialize_after_dump = Parameter(
#         "materialize_after_dump", default=False, required=False
#     )
#     dbt_alias = Parameter("dbt_alias", default=False, required=False)

#     url = Parameter(
#         "url",
#         default=cvm_constants.CDA_URL.value,
#         required=True,
#     )

#     df = extract_links_and_dates(url)
#     log(f"Links e datas: {df}")
#     arquivos = check_for_updates(df, upstream_tasks=[df])
#     log(f"Arquivos: {arquivos}")
#     with case(is_empty(arquivos), True):
#         log(f"Não houveram atualizações em {url.default}!")

#     with case(is_empty(arquivos), False):
#         input_filepath = download_unzip_csv(
#             url=url, files=arquivos, id=table_id, upstream_tasks=[arquivos]
#         )
#         output_filepath = clean_data_make_partitions_cda(
#             input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
#         )

#         rename_flow_run = rename_current_flow_run_dataset_table(
#             prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
#         )

#         wait_upload_table = create_table_and_upload_to_gcs(
#             data_path=output_filepath,
#             dataset_id=dataset_id,
#             table_id=table_id,
#             dump_mode="append",
#             wait=output_filepath,
#         )
#         with case(materialize_after_dump, True):
#             # Trigger DBT flow run
#             current_flow_labels = get_current_flow_labels()
#             materialization_flow = create_flow_run(
#                 flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
#                 project_name=constants.PREFECT_DEFAULT_PROJECT.value,
#                 parameters={
#                     "dataset_id": dataset_id,
#                     "table_id": table_id,
#                     "mode": materialization_mode,
#                     "dbt_alias": dbt_alias,
#                 },
#                 labels=current_flow_labels,
#                 run_name=f"Materialize {dataset_id}.{table_id}",
#             )

#             wait_for_materialization = wait_for_flow_run(
#                 materialization_flow,
#                 stream_states=True,
#                 stream_logs=True,
#                 raise_final_state=True,
#             )
#             wait_for_materialization.max_retries = (
#                 dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
#             )
#             wait_for_materialization.retry_delay = timedelta(
#                 seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
#             )


# br_cvm_fi_documentos_carteiras_fundos_investimento.storage = GCS(
#     constants.GCS_FLOWS_BUCKET.value
# )
# br_cvm_fi_documentos_carteiras_fundos_investimento.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value
# )
# br_cvm_fi_documentos_carteiras_fundos_investimento.schedule = every_day_carteiras


# with Flow(
#     name="br_cvm_fi_documentos_extratos_informacoes",
#     code_owners=[
#         "arthurfg",
#     ],
# ) as br_cvm_fi_documentos_extratos_informacoes:
#     # Parameters
#     dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=False)
#     table_id = Parameter(
#         "table_id", default="documentos_extratos_informacoes", required=False
#     )
#     materialization_mode = Parameter(
#         "materialization_mode", default="dev", required=True
#     )
#     materialize_after_dump = Parameter(
#         "materialize_after_dump", default=False, required=True
#     )
#     dbt_alias = Parameter("dbt_alias", default=False, required=False)

#     url = Parameter(
#         "url",
#         default=cvm_constants.URL_EXT.value,
#         required=True,
#     )

#     file = Parameter(
#         "file",
#         default=cvm_constants.FILE_EXT.value,
#         required=False,
#     )

#     df = extract_links_and_dates(url)
#     arquivos = check_for_updates_ext(df, upstream_tasks=[df])

#     with case(is_empty(arquivos), True):
#         log(f"Não houveram atualizações em {url.default}!")

#     with case(is_empty(arquivos), False):
#         input_filepath = download_csv_cvm(
#             url=url, table_id=table_id, files=arquivos, upstream_tasks=[arquivos]
#         )
#         output_filepath = clean_data_make_partitions_ext(
#             input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
#         )

#         rename_flow_run = rename_current_flow_run_dataset_table(
#             prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
#         )

#         wait_upload_table = create_table_and_upload_to_gcs(
#             data_path=output_filepath,
#             dataset_id=dataset_id,
#             table_id=table_id,
#             dump_mode="append",
#             wait=output_filepath,
#         )
#         with case(materialize_after_dump, True):
#             # Trigger DBT flow run
#             current_flow_labels = get_current_flow_labels()
#             materialization_flow = create_flow_run(
#                 flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
#                 project_name=constants.PREFECT_DEFAULT_PROJECT.value,
#                 parameters={
#                     "dataset_id": dataset_id,
#                     "table_id": table_id,
#                     "mode": materialization_mode,
#                     "dbt_alias": dbt_alias,
#                 },
#                 labels=current_flow_labels,
#                 run_name=f"Materialize {dataset_id}.{table_id}",
#             )

#             wait_for_materialization = wait_for_flow_run(
#                 materialization_flow,
#                 stream_states=True,
#                 stream_logs=True,
#                 raise_final_state=True,
#             )
#             wait_for_materialization.max_retries = (
#                 dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
#             )
#             wait_for_materialization.retry_delay = timedelta(
#                 seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
#             )


# br_cvm_fi_documentos_extratos_informacoes.storage = GCS(
#     constants.GCS_FLOWS_BUCKET.value
# )
# br_cvm_fi_documentos_extratos_informacoes.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value
# )
# br_cvm_fi_documentos_extratos_informacoes.schedule = every_day_extratos


# with Flow(
#     name="br_cvm_fi_documentos_perfil_mensal",
#     code_owners=[
#         "arthurfg",
#     ],
# ) as br_cvm_fi_documentos_perfil_mensal:
#     # Parameters
#     dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=False)
#     table_id = Parameter("table_id", default="documentos_perfil_mensal", required=False)
#     materialization_mode = Parameter(
#         "materialization_mode", default="dev", required=False
#     )
#     materialize_after_dump = Parameter(
#         "materialize_after_dump", default=False, required=False
#     )
#     dbt_alias = Parameter("dbt_alias", default=False, required=False)

#     url = Parameter(
#         "url",
#         default=cvm_constants.URL_PERFIL_MENSAL.value,
#         required=True,
#     )

#     df = extract_links_and_dates(url)
#     arquivos = check_for_updates(df, upstream_tasks=[df])

#     with case(is_empty(arquivos), True):
#         log(f"Não houveram atualizações em {url.default}!")

#     with case(is_empty(arquivos), False):
#         input_filepath = download_csv_cvm(
#             url=url, table_id=table_id, files=arquivos, upstream_tasks=[arquivos]
#         )
#         output_filepath = clean_data_make_partitions_perfil(
#             input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
#         )

#         rename_flow_run = rename_current_flow_run_dataset_table(
#             prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
#         )

#         wait_upload_table = create_table_and_upload_to_gcs(
#             data_path=output_filepath,
#             dataset_id=dataset_id,
#             table_id=table_id,
#             dump_mode="append",
#             wait=output_filepath,
#         )
#         with case(materialize_after_dump, True):
#             # Trigger DBT flow run
#             current_flow_labels = get_current_flow_labels()
#             materialization_flow = create_flow_run(
#                 flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
#                 project_name=constants.PREFECT_DEFAULT_PROJECT.value,
#                 parameters={
#                     "dataset_id": dataset_id,
#                     "table_id": table_id,
#                     "mode": materialization_mode,
#                     "dbt_alias": dbt_alias,
#                 },
#                 labels=current_flow_labels,
#                 run_name=f"Materialize {dataset_id}.{table_id}",
#             )

#             wait_for_materialization = wait_for_flow_run(
#                 materialization_flow,
#                 stream_states=True,
#                 stream_logs=True,
#                 raise_final_state=True,
#             )
#             wait_for_materialization.max_retries = (
#                 dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
#             )
#             wait_for_materialization.retry_delay = timedelta(
#                 seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
#             )


# br_cvm_fi_documentos_perfil_mensal.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# br_cvm_fi_documentos_perfil_mensal.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value
# )
# br_cvm_fi_documentos_perfil_mensal.schedule = every_day_perfil


# with Flow(
#     name="br_cvm_fi_documentos_informacao_cadastral",
#     code_owners=[
#         "arthurfg",
#     ],
# ) as br_cvm_fi_documentos_informacao_cadastral:
#     # Parameters
#     dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=False)
#     table_id = Parameter(
#         "table_id", default="documentos_informacao_cadastral", required=False
#     )
#     materialization_mode = Parameter(
#         "materialization_mode", default="dev", required=True
#     )
#     materialize_after_dump = Parameter(
#         "materialize_after_dump", default=False, required=True
#     )
#     dbt_alias = Parameter("dbt_alias", default=False, required=False)

#     url = Parameter(
#         "url",
#         default=cvm_constants.URL_INFO_CADASTRAL.value,
#         required=True,
#     )

#     files = Parameter("files", default=cvm_constants.CAD_FILE.value, required=False)

#     with case(is_empty(files), True):
#         log(f"Não houveram atualizações em {url.default}!")

#     with case(is_empty(files), False):
#         input_filepath = download_csv_cvm(url=url, files=files, table_id=table_id)
#         output_filepath = clean_data_make_partitions_cad(
#             diretorio=input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
#         )

#         rename_flow_run = rename_current_flow_run_dataset_table(
#             prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
#         )

#         wait_upload_table = create_table_and_upload_to_gcs(
#             data_path=output_filepath,
#             dataset_id=dataset_id,
#             table_id=table_id,
#             dump_mode="append",
#             wait=output_filepath,
#         )
#         with case(materialize_after_dump, True):
#             # Trigger DBT flow run
#             current_flow_labels = get_current_flow_labels()
#             materialization_flow = create_flow_run(
#                 flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
#                 project_name=constants.PREFECT_DEFAULT_PROJECT.value,
#                 parameters={
#                     "dataset_id": dataset_id,
#                     "table_id": table_id,
#                     "mode": materialization_mode,
#                     "dbt_alias": dbt_alias,
#                 },
#                 labels=current_flow_labels,
#                 run_name=f"Materialize {dataset_id}.{table_id}",
#             )

#             wait_for_materialization = wait_for_flow_run(
#                 materialization_flow,
#                 stream_states=True,
#                 stream_logs=True,
#                 raise_final_state=True,
#             )
#             wait_for_materialization.max_retries = (
#                 dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
#             )
#             wait_for_materialization.retry_delay = timedelta(
#                 seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
#             )


# br_cvm_fi_documentos_informacao_cadastral.storage = GCS(
#     constants.GCS_FLOWS_BUCKET.value
# )
# br_cvm_fi_documentos_informacao_cadastral.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value
# )
# br_cvm_fi_documentos_informacao_cadastral.schedule = every_day_informacao_cadastral


# with Flow(
#     name="br_cvm_fi_documentos_balancete",
#     code_owners=[
#         "arthurfg",
#     ],
# ) as br_cvm_fi_documentos_balancete:
#     # Parameters
#     dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=False)
#     table_id = Parameter("table_id", default="documentos_balancete", required=False)
#     materialization_mode = Parameter(
#         "materialization_mode", default="dev", required=True
#     )
#     materialize_after_dump = Parameter(
#         "materialize_after_dump", default=False, required=True
#     )
#     dbt_alias = Parameter("dbt_alias", default=False, required=False)

#     url = Parameter(
#         "url",
#         default=cvm_constants.URL_BALANCETE.value,
#         required=True,
#     )

#     df = extract_links_and_dates(url)

#     files = check_for_updates(df, upstream_tasks=[df])

#     with case(is_empty(files), True):
#         log(f"Não houveram atualizações em {url.default}!")

#     with case(is_empty(files), False):
#         input_filepath = download_unzip_csv(url=url, files=files, id=table_id)
#         output_filepath = clean_data_make_partitions_balancete(
#             input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
#         )

#         rename_flow_run = rename_current_flow_run_dataset_table(
#             prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
#         )

#         wait_upload_table = create_table_and_upload_to_gcs(
#             data_path=output_filepath,
#             dataset_id=dataset_id,
#             table_id=table_id,
#             dump_mode="append",
#             wait=output_filepath,
#         )
#         with case(materialize_after_dump, True):
#             # Trigger DBT flow run
#             current_flow_labels = get_current_flow_labels()
#             materialization_flow = create_flow_run(
#                 flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
#                 project_name=constants.PREFECT_DEFAULT_PROJECT.value,
#                 parameters={
#                     "dataset_id": dataset_id,
#                     "table_id": table_id,
#                     "mode": materialization_mode,
#                     "dbt_alias": dbt_alias,
#                 },
#                 labels=current_flow_labels,
#                 run_name=f"Materialize {dataset_id}.{table_id}",
#             )

#             wait_for_materialization = wait_for_flow_run(
#                 materialization_flow,
#                 stream_states=True,
#                 stream_logs=True,
#                 raise_final_state=True,
#             )
#             wait_for_materialization.max_retries = (
#                 dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
#             )
#             wait_for_materialization.retry_delay = timedelta(
#                 seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
#             )


# br_cvm_fi_documentos_balancete.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# br_cvm_fi_documentos_balancete.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value
# )
# br_cvm_fi_documentos_balancete.schedule = every_day_balancete
