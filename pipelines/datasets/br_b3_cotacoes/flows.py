"""
Flows for dataset br_b3_cotacoes dataset
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_b3_cotacoes.tasks import data_max_b3, tratamento
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(name="br_b3_cotacoes.cotacoes", code_owners=["trick"]) as cotacoes:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_b3_cotacoes", required=True
    )
    table_id = Parameter("table_id", default="cotacoes", required=True)

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
    delta_day = Parameter("delta_day", default=1, required=False)

    output_path = tratamento(
        delta_day=delta_day, upstream_tasks=[rename_flow_run]
    )
    data_max = data_max_b3(delta_day=delta_day, upstream_tasks=[output_path])

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[output_path],
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
            data_path=output_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )
        data_max = data_max_b3(
            delta_day=delta_day, upstream_tasks=[wait_upload_prod]
        )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data_referencia"},
                date_format="%Y-%m-%d",
                coverage_type="all_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[data_max],
            )

cotacoes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cotacoes.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# cotacoes.schedule = all_day_cotacoes
