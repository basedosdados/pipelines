from prefect import Parameter

from pipelines.datasets.br_bndes_operacoes_contratadas.tasks import (
    create_folders,
    get_source_last_date,
    process_data,
    process_de_para_cnae,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_bndes_operacoes_contratadas__operacoes_contratadas_forma_direta_e_indireta_nao_automatica",
    code_owners=["equipe_dados"],
) as br_bndes_operacoes_contratadas_direta_indireta_nao_automatica:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bndes_operacoes_contratadas", required=False
    )
    table_id = Parameter(
        "table_id",
        default="operacoes_contratadas_forma_direta_e_indireta_nao_automatica",
        required=False,
    )
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    check_for_updates = Parameter(
        "check_for_updates", default=True, required=False
    )
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    input_folder, output_folder = create_folders(table_id=table_id)

    source_last_date = get_source_last_date(
        table_id=table_id, input_folder=input_folder
    )

    # check_for_updates_true = check_if_data_is_outdated(
    #     dataset_id,
    #     table_id,
    #     data_source_max_date=source_last_date,
    #     date_format="%Y-%m-%d",
    #     upstream_tasks=[source_last_date],
    # )
    # check_for_updates_false = true_task()

    # coverage_check = switch_check_for_updates(
    #     check_for_updates,
    #     check_for_updates_true,
    #     check_for_updates_false,
    #     upstream_tasks=[check_for_updates_true, check_for_updates_false],
    # )

    # with case(coverage_check, True):
    output_file = process_data(
        input_folder=input_folder,
        output_folder=output_folder,
        data_apuracao=source_last_date,
        table_id=table_id,
        # upstream_tasks=[coverage_check],
    )

    wait_upload_base = create_table_dev_and_upload_to_gcs(
        data_path=output_folder,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[output_file],
    )
    # wait_for_materialization_base = run_dbt(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     dbt_command="run/test",
    #     dbt_alias=dbt_alias,
    #     upstream_tasks=[wait_upload_base],
    # )

    # with case(materialize_after_dump, True):
    #     wait_upload_prod_base = create_table_prod_gcs_and_run_dbt(
    #         data_path=output_folder,
    #         dataset_id=dataset_id,
    #         table_id=table_id,
    #         dump_mode="append",
    #         upstream_tasks=[wait_for_materialization_base],
    #     )

    #     with case(update_metadata, True):
    #         update_django_metadata(
    #             dataset_id=dataset_id,
    #             table_id=table_id,
    #             date_column_name={"date": "data_apuracao"},
    #             date_format="%Y-%m-%d",
    #             coverage_type="all_bdpro",
    #             bq_project="basedosdados",
    #             upstream_tasks=[
    #                 wait_upload_prod_base,
    #             ],
    #         )


with Flow(
    name="br_bndes_operacoes_contratadas__cnaes_agrupados_bndes",
    code_owners=["equipe_dados"],
) as br_bndes_operacoes_contratadas_cnaes_agrupados_bndes:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bndes_operacoes_contratadas", required=False
    )
    table_id = Parameter(
        "table_id", default="cnaes_agrupados_bndes", required=False
    )
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    check_for_updates = Parameter(
        "check_for_updates", default=True, required=False
    )
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    input_folder, output_folder = create_folders(table_id=table_id)

    source_last_date = get_source_last_date(
        table_id=table_id, input_folder=input_folder
    )

    # check_for_updates_true = check_if_data_is_outdated(
    #     dataset_id,
    #     table_id,
    #     data_source_max_date=source_last_date,
    #     date_format="%Y-%m-%d",
    #     upstream_tasks=[source_last_date],
    # )
    # check_for_updates_false = true_task()

    # coverage_check = switch_check_for_updates(
    #     check_for_updates,
    #     check_for_updates_true,
    #     check_for_updates_false,
    #     upstream_tasks=[check_for_updates_true, check_for_updates_false],
    # )

    # with case(coverage_check, True):
    output_file = process_de_para_cnae(
        input_folder=input_folder,
        output_folder=output_folder,
        table_id=table_id,
        # upstream_tasks=[coverage_check],
    )

    wait_upload_base = create_table_dev_and_upload_to_gcs(
        data_path=output_folder,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[output_file],
    )
    # wait_for_materialization_base = run_dbt(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     dbt_command="run/test",
    #     dbt_alias=dbt_alias,
    #     upstream_tasks=[wait_upload_base],
    # )

    # with case(materialize_after_dump, True):
    #     wait_upload_prod_base = create_table_prod_gcs_and_run_dbt(
    #         data_path=output_folder,
    #         dataset_id=dataset_id,
    #         table_id=table_id,
    #         dump_mode="append",
    #         upstream_tasks=[wait_for_materialization_base],
    #     )

    #     with case(update_metadata, True):
    #         update_django_metadata(
    #             dataset_id=dataset_id,
    #             table_id=table_id,
    #             date_column_name={"date": "data_apuracao"},
    #             date_format="%Y-%m-%d",
    #             coverage_type="all_bdpro",
    #             bq_project="basedosdados",
    #             upstream_tasks=[
    #                 wait_upload_prod_base,
    #             ],
    #         )
