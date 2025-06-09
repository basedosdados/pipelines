# -*- coding: utf-8 -*-
"""
Flows for br_anp_precos_combustiveis
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_anp_precos_combustiveis.schedules import (
    every_week_anp_microdados,
)
from pipelines.datasets.br_anp_precos_combustiveis.tasks import (
    download_and_transform,
    get_output,
    make_partitions,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
)
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

with Flow(
    name="br_anp_precos_combustiveis.microdados", code_owners=["trick"]
) as anp_microdados:
    dataset_id = Parameter(
        "dataset_id", default="br_anp_precos_combustiveis", required=True
    )
    table_id = Parameter("table_id", default="microdados", required=True)

    target = Parameter("target", default="prod", required=False)

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    update_metadata = Parameter(
        "update_metadata", default=True, required=False
    )

    df = download_and_transform(upstream_tasks=[rename_flow_run])
    output_path = make_partitions(df=df, upstream_tasks=[df])
    get_output = get_output(upstream_tasks=[output_path])
    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[output_path],
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
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

        update_django_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            date_column_name={"date": "data_coleta"},
            date_format="%Y-%m-%d",
            coverage_type="part_bdpro",
            time_delta={"weeks": 6},
            prefect_mode=target,
            bq_project="basedosdados",
        )


anp_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
anp_microdados.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
anp_microdados.schedule = every_week_anp_microdados
