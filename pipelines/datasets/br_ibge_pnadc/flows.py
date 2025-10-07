"""
Flows for br_ibge_pnadc
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_ibge_pnadc.tasks import (
    build_partitions,
    build_table_paths,
    get_data_source_date_and_url,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)
from pipelines.utils.to_download.tasks import download_async

with Flow(name="br_ibge_pnadc.microdados", code_owners=["luiz"]) as br_pnadc:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ibge_pnadc", required=False
    )
    table_id = Parameter("table_id", default="microdados", required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
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

    data_source_max_date, url = get_data_source_date_and_url(
        upstream_tasks=[rename_flow_run]
    )

    outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        date_type="last_update_date",
        data_source_max_date=data_source_max_date,
        upstream_tasks=[data_source_max_date],
    )

    with case(outdated, True):
        input_dir, output_dir = build_table_paths(
            table_id=table_id, upstream_tasks=[outdated]
        )
        input_filepath = download_async(
            url, input_dir, "zip", upstream_tasks=[input_dir, output_dir]
        )

        output_filepath = build_partitions(
            input_dir, output_dir, upstream_tasks=[input_filepath]
        )

        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_dir,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_dir,
            upstream_tasks=[output_filepath],
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
                    date_column_name={"year": "ano", "quarter": "trimestre"},
                    date_format="%Y-%m",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_pnadc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_pnadc.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_pnadc.schedule = every_day
