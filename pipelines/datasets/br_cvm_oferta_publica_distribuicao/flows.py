# -*- coding: utf-8 -*-
"""
Flows for br_cvm_oferta_publica_distribuicao
"""

# pylint: disable=C0103, E1123, invalid-name

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_cvm_oferta_publica_distribuicao.schedules import (
    schedule_dia,
)
from pipelines.datasets.br_cvm_oferta_publica_distribuicao.tasks import (
    clean_table_oferta_distribuicao,
    crawl,
    get_output,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

ROOT = "/tmp/data"
URL = "http://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"

with Flow(
    name="br_cvm_oferta_publica_distribuicao.dia",
    code_owners=["equipe_pipelines"],
) as br_cvm_ofe_pub_dis_dia:
    # Parameters
    dataset_id = Parameter(
        "dataset_id",
        default="br_cvm_oferta_publica_distribuicao",
        required=True,
    )
    table_id = Parameter("table_id", default="dia", required=True)
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_oferta_distribuicao(
        root=ROOT, upstream_tasks=[wait_crawl]
    )

    get_output(root=ROOT, upstream_tasks=[filepath])

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
                date_column_name={"date": "data_comunicado"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )

br_cvm_ofe_pub_dis_dia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_ofe_pub_dis_dia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_ofe_pub_dis_dia.schedule = schedule_dia
