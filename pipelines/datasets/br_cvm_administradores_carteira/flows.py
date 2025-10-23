"""
Flows for br_cvm_administradores_carteira
"""

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
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

ROOT = "/tmp/data"
URL = "http://dados.cvm.gov.br/dados/ADM_CART/CAD/DADOS/cad_adm_cart.zip"

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

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )
    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target=target,
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )
    # dont generate temporal coverage since there's no date variable
    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

br_cvm_adm_car_res.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_res.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_adm_car_res.schedule = schedule_responsavel

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

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
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
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
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
                upstream_tasks=[wait_upload_prod],
            )


br_cvm_adm_car_pes_fis.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_pes_fis.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_adm_car_pes_fis.schedule = schedule_fisica

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

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        target=target,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        wait_upload_prod = create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
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
                upstream_tasks=[wait_upload_prod],
            )
br_cvm_adm_car_pes_jur.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_pes_jur.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_adm_car_pes_jur.schedule = schedule_juridica
