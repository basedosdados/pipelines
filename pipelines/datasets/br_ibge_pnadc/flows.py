# -*- coding: utf-8 -*-
"""
Flows for br_ibge_pnadc
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_ibge_pnadc.tasks import (
    build_parquet_files,
    get_data_source_date_and_url,
    get_output,
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
from pipelines.utils.to_download.tasks import to_download

# pylint: disable=C0103
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
        input_filepath = to_download(
            url, "/tmp/data/input/", "zip", upstream_tasks=[outdated]
        )

        output_filepath = build_parquet_files(
            input_filepath, upstream_tasks=[input_filepath]
        )

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
                date_column_name={"year": "ano", "quarter": "trimestre"},
                date_format="%Y-%m",
                coverage_type="all_free",
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )

br_pnadc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_pnadc.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# br_pnadc.schedule = every_day
