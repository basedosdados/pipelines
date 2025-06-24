# -*- coding: utf-8 -*-
"""
Flows for br_inmet_bdmep
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_inmet_bdmep.schedules import every_month_inmet
from pipelines.datasets.br_inmet_bdmep.tasks import (
    extract_last_date_from_source,
    get_base_inmet,
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

# from pipelines.datasets.br_ibge_pnadc.schedules import every_quarter

# pylint: disable=C0103
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

        upload_and_materialization_dev = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="dev",
                bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
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
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[upload_and_materialization_prod],
                )

br_inmet.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_inmet.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_inmet.schedule = every_month_inmet
