"""
Flows for br_cvm_fi

"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_cvm_fi.constants import constants as cvm_constants
from pipelines.datasets.br_cvm_fi.schedules import (
    every_day_balancete,
    every_day_extratos,
    every_day_informacao_cadastral,
)
from pipelines.datasets.br_cvm_fi.tasks import (
    clean_data_and_make_partitions,
    clean_data_make_partitions_balancete,
    clean_data_make_partitions_cad,
    clean_data_make_partitions_cda,
    clean_data_make_partitions_ext,
    clean_data_make_partitions_perfil,
    download_csv_cvm,
    download_unzip_csv,
    extract_links_and_dates,
    generate_links_to_download,
    is_empty,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_cvm_fi_documentos_informe_diario",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_cvm_fi_documentos_informe_diario:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=True)
    table_id = Parameter(
        "table_id", default="documentos_informe_diario", required=True
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    url = Parameter(
        "url",
        default=cvm_constants.INFORME_DIARIO_URL.value,
        required=False,
    )

    df, max_date = extract_links_and_dates(url)

    log_task(f"Links e datas: {df}")

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_date,
        date_format="%Y-%m-%d",
        date_type="last_update_date",
        upstream_tasks=[df],
    )

    with case(check_if_outdated, False):
        log_task(
            "A execução sera agendada para a próxima data definida na schedule"
        )

    with case(check_if_outdated, True):
        arquivos = generate_links_to_download(df=df, max_date=max_date)
        log_task(f"Arquivos: {arquivos}")

        input_filepath = download_unzip_csv(
            files=arquivos, url=url, id=table_id, upstream_tasks=[arquivos]
        )
        output_filepath = clean_data_and_make_partitions(
            path=input_filepath,
            table_id=table_id,
            upstream_tasks=[input_filepath],
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            target=target,
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=output_filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_competencia"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )


br_cvm_fi_documentos_informe_diario.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi_documentos_informe_diario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_cvm_fi_documentos_informe_diario.schedule = every_day_informe


with Flow(
    name="br_cvm_fi_documentos_carteiras_fundos_investimento",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_cvm_fi_documentos_carteiras_fundos_investimento:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=True)
    table_id = Parameter(
        "table_id",
        default="documentos_carteiras_fundos_investimento",
        required=True,
    )

    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    url = Parameter(
        "url",
        default=cvm_constants.CDA_URL.value,
        required=False,
    )

    df, max_date = extract_links_and_dates(url)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_date,
        date_format="%Y-%m-%d",
        date_type="last_update_date",
        upstream_tasks=[df],
    )

    with case(check_if_outdated, False):
        log_task(
            "A execução sera agendada para a próxima data definida na schedule"
        )

    with case(check_if_outdated, True):
        arquivos = generate_links_to_download(df=df, max_date=max_date)

        input_filepath = download_unzip_csv(
            url=url, files=arquivos, id=table_id, upstream_tasks=[arquivos]
        )
        output_filepath = clean_data_make_partitions_cda(
            input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )
        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=output_filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_competencia"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )


br_cvm_fi_documentos_carteiras_fundos_investimento.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi_documentos_carteiras_fundos_investimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_cvm_fi_documentos_carteiras_fundos_investimento.schedule = (
#     every_day_carteiras
# )


with Flow(
    name="br_cvm_fi_documentos_extratos_informacoes",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_cvm_fi_documentos_extratos_informacoes:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=True)
    table_id = Parameter(
        "table_id", default="documentos_extratos_informacoes", required=True
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    url = Parameter(
        "url",
        default=cvm_constants.URL_EXT.value,
        required=False,
    )

    file = Parameter(
        "file",
        default=cvm_constants.FILE_EXT.value,
        required=False,
    )

    df, max_date = extract_links_and_dates(url)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_date,
        date_format="%Y-%m-%d",
        date_type="last_update_date",
        upstream_tasks=[df],
    )

    with case(check_if_outdated, False):
        log_task(
            "A execução sera agendada para a próxima data definida na schedule"
        )

    with case(check_if_outdated, True):
        arquivos = generate_links_to_download(df=df, max_date=max_date)

        input_filepath = download_csv_cvm(
            url=url,
            table_id=table_id,
            files=arquivos,
            upstream_tasks=[arquivos],
        )
        output_filepath = clean_data_make_partitions_ext(
            input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=output_filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_competencia"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )

br_cvm_fi_documentos_extratos_informacoes.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi_documentos_extratos_informacoes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi_documentos_extratos_informacoes.schedule = every_day_extratos


with Flow(
    name="br_cvm_fi_documentos_perfil_mensal",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_cvm_fi_documentos_perfil_mensal:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=True)
    table_id = Parameter(
        "table_id", default="documentos_perfil_mensal", required=True
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    url = Parameter(
        "url",
        default=cvm_constants.URL_PERFIL_MENSAL.value,
        required=False,
    )

    df, max_date = extract_links_and_dates(url)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_date,
        date_format="%Y-%m-%d",
        date_type="last_update_date",
        upstream_tasks=[df],
    )

    with case(check_if_outdated, False):
        log_task(
            "A execução sera agendada para a próxima data definida na schedule"
        )

    with case(check_if_outdated, True):
        arquivos = generate_links_to_download(df=df, max_date=max_date)

        input_filepath = download_csv_cvm(
            url=url,
            table_id=table_id,
            files=arquivos,
            upstream_tasks=[arquivos],
        )
        output_filepath = clean_data_make_partitions_perfil(
            input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )
        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=output_filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_competencia"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )

br_cvm_fi_documentos_perfil_mensal.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi_documentos_perfil_mensal.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_cvm_fi_documentos_perfil_mensal.schedule = every_day_perfil


with Flow(
    name="br_cvm_fi_documentos_informacao_cadastral",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_cvm_fi_documentos_informacao_cadastral:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=True)
    table_id = Parameter(
        "table_id", default="documentos_informacao_cadastral", required=True
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    url = Parameter(
        "url",
        default=cvm_constants.URL_INFO_CADASTRAL.value,
        required=False,
    )

    files = Parameter(
        "files", default=cvm_constants.CAD_FILE.value, required=False
    )

    with case(is_empty(files), True):
        log_task(
            "A execução sera agendada para a próxima data definida na schedule"
        )

    with case(is_empty(files), False):
        input_filepath = download_csv_cvm(
            url=url, files=files, table_id=table_id
        )
        output_filepath = clean_data_make_partitions_cad(
            diretorio=input_filepath,
            table_id=table_id,
            upstream_tasks=[input_filepath],
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            target=target,
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=output_filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                    historical_database=False,
                )


br_cvm_fi_documentos_informacao_cadastral.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cvm_fi_documentos_informacao_cadastral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi_documentos_informacao_cadastral.schedule = (
    every_day_informacao_cadastral
)


with Flow(
    name="br_cvm_fi_documentos_balancete",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_cvm_fi_documentos_balancete:
    # Parameters
    dataset_id = Parameter("dataset_id", default="br_cvm_fi", required=True)
    table_id = Parameter(
        "table_id", default="documentos_balancete", required=True
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    url = Parameter(
        "url",
        default=cvm_constants.URL_BALANCETE.value,
        required=False,
    )

    df, max_date = extract_links_and_dates(url)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_date,
        date_format="%Y-%m-%d",
        date_type="last_update_date",
        upstream_tasks=[df],
    )

    with case(check_if_outdated, False):
        log_task(
            "A execução sera agendada para a próxima data definida na schedule"
        )

    with case(check_if_outdated, True):
        arquivos = generate_links_to_download(df=df, max_date=max_date)

        input_filepath = download_unzip_csv(
            url=url, files=arquivos, id=table_id
        )
        output_filepath = clean_data_make_partitions_balancete(
            input_filepath, table_id=table_id, upstream_tasks=[input_filepath]
        )

        rename_flow_run = rename_current_flow_run_dataset_table(
            prefix="Dump: ",
            dataset_id=dataset_id,
            table_id=table_id,
            wait=table_id,
        )

        wait_upload_table = create_table_dev_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_filepath],
        )

        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_alias=dbt_alias,
            dbt_command="run/test",
            disable_elementary=False,
            upstream_tasks=[wait_upload_table],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod = create_table_prod_gcs_and_run_dbt(
                data_path=output_filepath,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data_competencia"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )


br_cvm_fi_documentos_balancete.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_fi_documentos_balancete.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_fi_documentos_balancete.schedule = every_day_balancete
