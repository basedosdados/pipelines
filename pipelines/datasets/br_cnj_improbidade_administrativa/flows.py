from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.datasets.br_cnj_improbidade_administrativa.schedules import every_month
from pipelines.datasets.br_cnj_improbidade_administrativa.tasks import (
    get_max_date,
    is_up_to_date,
    main_task,
    write_csv_file,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)
from pipelines.utils.utils import log_task

with Flow(
    name="br_cnj_improbidade_administrativa.condenacao",
    code_owners=[
        "aspeddro",
    ],
) as br_cnj_improbidade_administrativa_flow:
    dataset_id = Parameter(
        "dataset_id",
        default="br_cnj_improbidade_administrativa",
        required=True,
    )
    table_id = Parameter("table_id", default="condenacao", required=True)
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    is_updated = is_up_to_date()

    with case(is_updated, True):
        log_task("Data already updated")

    with case(is_updated, False):
        log_task("Data is outdated")

        df = main_task(upstream_tasks=[is_updated])

        log_task(df)

        max_date = get_max_date(df, upstream_tasks=[df])

        log_task(f"Max date: {max_date}")

        output_filepath = write_csv_file(df, upstream_tasks=[max_date])

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
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

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data_propositura"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                prefect_mode=target,
                time_delta={"months": 6},
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )

br_cnj_improbidade_administrativa_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cnj_improbidade_administrativa_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_cnj_improbidade_administrativa_flow.schedule = every_month
