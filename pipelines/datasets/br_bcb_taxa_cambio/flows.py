# -*- coding: utf-8 -*-
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
    get_output,
    treat_data_taxa_cambio,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
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
    target = Parameter("target", default="prod", required=False)
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
    get_output = get_output(
        table_id=table_id,
        upstream_tasks=[file_info],
    )
    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project_id=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="prod",
            bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            billing_project_id=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[upload_and_materialization_dev],
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"date": "data_cotacao"},
                date_format="%Y-%m-%d",
                coverage_type="all_bdpro",
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
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
