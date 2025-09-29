"""
Flows for br_poder360_pesquisas
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_poder360_pesquisas.tasks import crawler
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    get_temporal_coverage,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_poder360_pesquisas.microdados", code_owners=["lucas_cr"]
) as br_poder360:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_poder360_pesquisas", required=True
    )
    table_id = Parameter("table_id", default="microdados", required=True)
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    filepath = crawler()

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["data"],
        time_unit="year",
        interval="1",
        upstream_tasks=[wait_upload_table],
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
            date_column_name={"date": "data"},
            date_format="%Y-%m-%d",
            coverage_type="part_bdpro",
            time_delta={"months": 6},
            prefect_mode=target,
            bq_project="basedosdados",
            upstream_tasks=[wait_for_dowload_data_to_gcs],
        )


br_poder360.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_poder360.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_poder360.schedule = every_monday_thursday
