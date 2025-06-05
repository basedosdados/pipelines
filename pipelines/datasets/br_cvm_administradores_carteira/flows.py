# -*- coding: utf-8 -*-
"""
Flows for br_cvm_administradores_carteira
"""

# pylint: disable=C0103, E1123, invalid-name

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_cvm_administradores_carteira.schedules import (
    schedule_fisica,
    schedule_juridica,
    schedule_responsavel,
)
from pipelines.datasets.br_cvm_administradores_carteira.tasks import (
    clean_table_pessoa_fisica,
    clean_table_pessoa_juridica,
    clean_table_responsavel,
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
URL = "http://dados.cvm.gov.br/dados/ADM_CART/CAD/DADOS/cad_adm_cart.zip"

# ============================= Responsável ==============================
with Flow(
    name="br_cvm_administradores_carteira.responsavel",
    code_owners=["equipe_pipelines"],
) as br_cvm_adm_car_res:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_cvm_administradores_carteira", required=True
    )
    table_id = Parameter("table_id", default="responsavel", required=True)

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_responsavel(root=ROOT, upstream_tasks=[wait_crawl])
    get_output = get_output(
        root=ROOT,
        table_id=table_id,
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

br_cvm_adm_car_res.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_res.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_adm_car_res.schedule = schedule_responsavel

# ============================= Pessoa Física ==============================

with Flow(
    "br_cvm_administradores_carteira.pessoa_fisica",
    code_owners=["equipe_pipelines"],
) as br_cvm_adm_car_pes_fis:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_cvm_administradores_carteira", required=True
    )
    table_id = Parameter("table_id", default="pessoa_fisica", required=True)

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

    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_pessoa_fisica(
        root=ROOT, upstream_tasks=[wait_crawl]
    )

    get_output = get_output(
        root=ROOT,
        table_id=table_id,
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
                date_column_name={"date": "data_registro"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )


br_cvm_adm_car_pes_fis.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_pes_fis.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_adm_car_pes_fis.schedule = schedule_fisica

# ============================= Pessoa Jurídica ==============================
with Flow(
    "br_cvm_administradores_carteira.pessoa_juridica",
    code_owners=["equipe_pipelines"],
) as br_cvm_adm_car_pes_jur:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_cvm_administradores_carteira", required=True
    )
    table_id = Parameter("table_id", default="pessoa_juridica", required=True)

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

    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_pessoa_juridica(
        root=ROOT, upstream_tasks=[wait_crawl]
    )

    get_output = get_output(
        root=ROOT,
        table_id=table_id,
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
                date_column_name={"date": "data_registro"},
                date_format="%Y-%m-%d",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )
br_cvm_adm_car_pes_jur.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_pes_jur.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_adm_car_pes_jur.schedule = schedule_juridica
