"""
Flows for br_me_cnpj - register 03/02/2026
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_me_cnpj.schedules import (
    every_day_empresas,
    every_day_estabelecimentos,
    every_day_simples,
    every_day_socios,
)
from pipelines.datasets.br_me_cnpj.tasks import get_data_source_max_date, main
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_me_cnpj.empresas",
    code_owners=[
        "equipe_dados",
    ],
) as br_me_cnpj_empresas:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter("table_id", default="empresas", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(dados_desatualizados, True):
        output_filepath = main(
            table_ids=[table_id],
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
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
            dbt_alias=dbt_alias,
            dbt_command="run/test",
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                bq_project="basedosdados",
                upstream_tasks=[wait_upload_prod],
            )


br_me_cnpj_empresas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_empresas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_empresas.schedule = every_day_empresas

with Flow(
    name="br_me_cnpj.socios",
    code_owners=[
        "equipe_dados",
    ],
) as br_me_cnpj_socios:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter("table_id", default="socios", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(dados_desatualizados, True):
        output_filepath = main(
            table_ids=[table_id],
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
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
            dbt_alias=dbt_alias,
            dbt_command="run/test",
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                bq_project="basedosdados",
                upstream_tasks=[wait_upload_prod],
            )

br_me_cnpj_socios.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_socios.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_socios.schedule = every_day_socios


with Flow(
    name="br_me_cnpj.estabelecimentos",
    code_owners=[
        "equipe_dados",
    ],
    executor=LocalDaskExecutor(),
) as br_me_cnpj_estabelecimentos:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter(
        "table_id", default="estabelecimentos", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    # with case(dados_desatualizados, True):
    output_filepath = main(
        table_ids=[table_id],
        max_folder_date=max_folder_date,
        max_last_modified_date=max_last_modified_date,
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
        dbt_alias=dbt_alias,
        dbt_command="run/test",
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

        wait_for_update_django_metadata = update_django_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            date_column_name={"year": "ano", "month": "mes"},
            date_format="%Y-%m",
            coverage_type="part_bdpro",
            time_delta={"months": 6},
            bq_project="basedosdados",
            upstream_tasks=[wait_upload_prod],
        )

        ## atualiza o diretório de empresas
        wait_for_second_materialization = run_dbt(
            dataset_id="br_bd_diretorios_brasil",
            table_id="empresa",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_for_update_django_metadata],
        )

        wait_for_second_dowload_data_to_gcs = download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[wait_for_second_materialization],
        )

        update_django_metadata(
            dataset_id="br_bd_diretorios_brasil",
            table_id="empresa",
            date_column_name={"date": "data"},
            date_format="%Y-%m-%d",
            coverage_type="all_bdpro",
            bq_project="basedosdados",
            upstream_tasks=[wait_for_second_dowload_data_to_gcs],
        )


br_me_cnpj_estabelecimentos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_estabelecimentos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_estabelecimentos.schedule = every_day_estabelecimentos


with Flow(
    name="br_me_cnpj.simples",
    code_owners=[
        "equipe_dados",
    ],
) as br_me_cnpj_simples:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter("table_id", default="simples", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(dados_desatualizados, True):
        output_filepath = main(
            table_ids=[table_id],
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
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

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage_type="all_free",
                bq_project="basedosdados",
                historical_database=False,
                upstream_tasks=[wait_upload_prod],
            )

br_me_cnpj_simples.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_simples.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_simples.schedule = every_day_simples


with Flow(
    name="br_me_cnpj.dicionario",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_me_cnpj_dicionario:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter("table_id", default="dicionario", required=False)
    tables = Parameter(
        "tables",
        default=["qualificacoes", "paises", "motivos", "cnaes", "naturezas"],
        required=False,
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    max_folder_date, max_last_modified_date = get_data_source_max_date()
    log_task(
        f"Max Last Modified Date: {max_last_modified_date}\nMax Folder Date:{max_folder_date}"
    )
    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_type="last_update_date",
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    # with case(dados_desatualizados, False):
    #     log_task(f"Não há atualizações para a tabela de {table_id}!")

    # with case(dados_desatualizados, True):
    output_filepath = main(
        table_ids=tables,
        max_folder_date=max_folder_date,
        max_last_modified_date=max_last_modified_date,
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
        dbt_alias=dbt_alias,
        dbt_command="run/test",
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

        update_django_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            historical_database=False,
            coverage_type="all_free",
            bq_project="basedosdados",
            upstream_tasks=[wait_upload_prod],
        )


br_me_cnpj_dicionario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_dicionario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
