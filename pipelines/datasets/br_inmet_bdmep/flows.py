from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.schedules import (
    every_day_inmet,
    every_month_inmet,
)
from pipelines.datasets.br_inmet_bdmep.tasks import (
    extract_last_date_from_source,
    get_base_inmet,
    get_stations_inmet,
    switch_check_for_updates,
    true_task,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_inmet_bdmep__microdados", code_owners=["equipe_dados"]
) as br_inmet_microdados:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_inmet_bdmep", required=False
    )
    table_id = Parameter("table_id", default="microdados", required=False)
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )
    year = Parameter("year", default=None, required=False)
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

    source_last_date = extract_last_date_from_source(year=year)
    check_for_updates_true = check_if_data_is_outdated(
        dataset_id,
        table_id,
        data_source_max_date=source_last_date,
        date_format="%Y-%m-%d",
        upstream_tasks=[source_last_date],
    )
    check_for_updates_false = true_task()

    coverage_check = switch_check_for_updates(
        check_for_updates,
        check_for_updates_true,
        check_for_updates_false,
        upstream_tasks=[check_for_updates_true, check_for_updates_false],
    )

    with case(coverage_check, True):
        output_base = get_base_inmet(
            upstream_tasks=[coverage_check],
        )

        wait_upload_base = create_table_dev_and_upload_to_gcs(
            data_path=output_base,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[output_base],
        )
        wait_for_materialization_base = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_base],
        )

        with case(materialize_after_dump, True):
            wait_upload_prod_base = create_table_prod_gcs_and_run_dbt(
                data_path=output_base,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode="append",
                upstream_tasks=[wait_for_materialization_base],
            )

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    bq_project="basedosdados",
                    upstream_tasks=[
                        wait_upload_prod_base,
                    ],
                )

with Flow(
    name="br_inmet_bdmep__estacao", code_owners=["equipe_dados"]
) as br_inmet_estacao:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_inmet_bdmep", required=False
    )
    table_id = Parameter("table_id", default="estacao", required=False)
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
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

    source_last_date = extract_last_date_from_source()
    # coverage_check = check_if_data_is_outdated(
    #     dataset_id,
    #     table_id,
    #     data_source_max_date=source_last_date,
    #     date_format="%Y-%m-%d",
    #     date_type="last_update_date",
    #     upstream_tasks=[source_last_date],
    # )

    # with case(coverage_check, True):
    output_estacoes = get_stations_inmet(upstream_tasks=[source_last_date])

    wait_upload_estacoes = create_table_dev_and_upload_to_gcs(
        data_path=output_estacoes,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[output_estacoes],
    )

    wait_for_materialization_estacoes = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_estacoes],
    )

    with case(materialize_after_dump, True):
        wait_upload_prod_estacoes = create_table_prod_gcs_and_run_dbt(
            data_path=output_estacoes,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization_estacoes],
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                bq_project="basedosdados",
                upstream_tasks=[
                    wait_upload_prod_estacoes,
                ],
            )

br_inmet_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_inmet_microdados.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_inmet_microdados.schedule = every_day_inmet
br_inmet_estacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_inmet_estacao.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_inmet_estacao.schedule = every_month_inmet
