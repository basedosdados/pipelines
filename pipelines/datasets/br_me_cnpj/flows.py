"""
Flows for br_me_cnpj
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_me_cnpj.constants import constants as constants_cnpj
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
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    log_task,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_me_cnpj.empresas",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_me_cnpj_empresas:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter("table_id", default="empresas", required=False)
    target = Parameter("target", default="prod", required=False)
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
    tabelas = constants_cnpj.TABELAS.value[0:1]

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(
            tabelas,
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
        )
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                dbt_command="run/test",
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )
            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )


br_me_cnpj_empresas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_empresas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_empresas.schedule = every_day_empresas

with Flow(
    name="br_me_cnpj.socios",
    code_owners=[
        "equipe_pipelines",
    ],
) as br_me_cnpj_socios:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter("table_id", default="socios", required=False)
    target = Parameter("target", default="prod", required=False)
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
    tabelas = constants_cnpj.TABELAS.value[1:2]

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(
            tabelas,
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
        )
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                dbt_command="run/test",
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )

br_me_cnpj_socios.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_socios.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_socios.schedule = every_day_socios


with Flow(
    name="br_me_cnpj.estabelecimentos",
    code_owners=[
        "equipe_pipelines",
    ],
    executor=LocalDaskExecutor(),
) as br_me_cnpj_estabelecimentos:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=False)
    table_id = Parameter(
        "table_id", default="estabelecimentos", required=False
    )
    target = Parameter("target", default="prod", required=False)
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
    tabelas = constants_cnpj.TABELAS.value[2:3]

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(
            tabelas,
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
        )
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                dbt_command="run/test",
                disable_elementary=False,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            wait_for_update_django_metadata = update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )

            ## atualiza o diretório de empresas
            wait_for_second_materialization = run_dbt(
                dataset_id="br_bd_diretorios_brasil",
                table_id="empresa",
                target=target,
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
                prefect_mode=target,
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
        "equipe_pipelines",
    ],
) as br_me_cnpj_simples:
    dataset_id = Parameter("dataset_id", default="br_me_cnpj", required=True)
    table_id = Parameter("table_id", default="simples", required=True)
    target = Parameter("target", default="prod", required=False)
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
    tabelas = constants_cnpj.TABELAS.value[3:]

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_folder_date,
        date_format="%Y-%m",
        upstream_tasks=[max_folder_date],
    )

    with case(dados_desatualizados, False):
        log_task(f"Não há atualizações para a tabela de {tabelas}!")

    with case(dados_desatualizados, True):
        output_filepath = main(
            tabelas,
            max_folder_date=max_folder_date,
            max_last_modified_date=max_last_modified_date,
        )
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
        )
        with case(materialize_after_dump, True):
            wait_for_materialization = run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                target=target,
                dbt_alias=dbt_alias,
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage_type="all_free",
                prefect_mode=target,
                bq_project="basedosdados",
                historical_database=False,
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )

br_me_cnpj_simples.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_me_cnpj_simples.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_me_cnpj_simples.schedule = every_day_simples
