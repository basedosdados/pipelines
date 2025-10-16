"""
Flows for br_mg_belohorizonte_smfa_iptu
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.tasks import (
    download_and_transform,
    get_data_source_sfma_iptu_max_date,
    make_partitions,
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

with Flow(
    name="br_mg_belohorizonte_smfa_iptu.iptu", code_owners=["trick"]
) as br_mg_belohorizonte_smfa_iptu_iptu:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_mg_belohorizonte_smfa_iptu", required=True
    )
    table_id = Parameter("table_id", default="iptu", required=True)
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    data_source_max_date = get_data_source_sfma_iptu_max_date()

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m",
        upstream_tasks=[data_source_max_date],
    )

    with case(dados_desatualizados, True):
        df = download_and_transform()
        output_filepath = make_partitions(df, upstream_tasks=[df])
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=output_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            wait=output_filepath,
            upstream_tasks=[output_filepath],
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

            with case(update_metadata, True):
                update_django_metadata(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    date_column_name={"year": "ano", "month": "mes"},
                    date_format="%Y-%m",
                    coverage_type="all_bdpro",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_mg_belohorizonte_smfa_iptu_iptu.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_mg_belohorizonte_smfa_iptu_iptu.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_mg_belohorizonte_smfa_iptu_iptu.schedule = every_weeks_iptu
