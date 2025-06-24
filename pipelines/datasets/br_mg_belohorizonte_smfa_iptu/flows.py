# -*- coding: utf-8 -*-
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
    get_output,
    make_partitions,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
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

    # with case(dados_desatualizados, True):

    df = download_and_transform()
    output_filepath = make_partitions(df, upstream_tasks=[df])
    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
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
                upstream_tasks=[upload_and_materialization_prod],
            )

br_mg_belohorizonte_smfa_iptu_iptu.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_mg_belohorizonte_smfa_iptu_iptu.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_mg_belohorizonte_smfa_iptu_iptu.schedule = every_weeks_iptu
