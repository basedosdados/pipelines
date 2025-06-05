# -*- coding: utf-8 -*-

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.datasets.br_cnj_improbidade_administrativa.schedules import every_month
from pipelines.datasets.br_cnj_improbidade_administrativa.tasks import (
    get_max_date,
    get_output,
    is_up_to_date,
    main_task,
    write_csv_file,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
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

        get_output = get_output(upstream_tasks=[output_filepath])

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
                date_column_name={"date": "data_propositura"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                prefect_mode=target,
                time_delta={"months": 6},
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )

br_cnj_improbidade_administrativa_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_cnj_improbidade_administrativa_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_cnj_improbidade_administrativa_flow.schedule = every_month
