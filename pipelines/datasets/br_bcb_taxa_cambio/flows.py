"""
Flows for br_bcb_taxa_cambio.taxa_cambio
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_bcb_taxa_cambio.schedules import (
    schedule_every_weekday_taxa_cambio,
)
from pipelines.datasets.br_bcb_taxa_cambio.tasks import (
    get_data_taxa_cambio,
    treat_data_taxa_cambio,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_bcb_taxa_cambio.taxa_cambio",
    code_owners=[
        "lauris",
    ],
) as datasets_br_bcb_taxa_cambio_moeda_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_bcb_taxa_cambio", required=True
    )
    table_id = Parameter("table_id", default="taxa_cambio", required=True)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    input_filepath = get_data_taxa_cambio(
        table_id=table_id, upstream_tasks=[rename_flow_run]
    )

    file_info = treat_data_taxa_cambio(
        table_id=table_id, upstream_tasks=[input_filepath]
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=file_info["save_output_path"],
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[file_info],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        dbt_command="run/test",
        disable_elementary=False,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        wait_upload_table = create_table_prod_gcs_and_run_dbt(
            data_path=file_info["save_output_path"],
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data_cotacao"},
                date_format="%Y-%m-%d",
                coverage_type="all_bdpro",
                bq_project="basedosdados",
                upstream_tasks=[wait_upload_table],
            )


datasets_br_bcb_taxa_cambio_moeda_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
datasets_br_bcb_taxa_cambio_moeda_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_bcb_taxa_cambio_moeda_flow.schedule = (
    schedule_every_weekday_taxa_cambio
)
