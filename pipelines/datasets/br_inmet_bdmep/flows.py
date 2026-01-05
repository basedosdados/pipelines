from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.schedules import every_month_inmet
from pipelines.datasets.br_inmet_bdmep.tasks import (
    extract_last_date_from_source,
    get_base_inmet,
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

with Flow(name="br_inmet_bdmep", code_owners=["equipe_pipelines"]) as br_inmet:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_inmet_bdmep", required=False
    )
    table_id = Parameter("table_id", default="microdados", required=False)
    year = Parameter("year", default=2024, required=False)
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

    coverage_check = check_if_data_is_outdated(
        dataset_id,
        table_id,
        data_source_max_date=source_last_date,
        date_format="%Y-%m",
        upstream_tasks=[source_last_date],
    )

    with case(coverage_check, True):
        output_filepath = get_base_inmet(
            year=year, upstream_tasks=[coverage_check]
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
            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    bq_project="basedosdados",
                    upstream_tasks=[wait_upload_prod],
                )

br_inmet.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_inmet.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_inmet.schedule = every_month_inmet
