# -*- coding: utf-8 -*-
"""
Flows for br_cvm_administradores_carteira
"""

# pylint: disable=C0103, E1123, invalid-name
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

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
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import (
    constants as dump_db_constants,
)
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
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

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )
    # dont generate temporal coverage since there's no date variable
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "target": target,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
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

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "target": target,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
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
                upstream_tasks=[wait_for_materialization],
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

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "target": target,
                "dbt_alias": dbt_alias,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
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
                upstream_tasks=[wait_for_materialization],
            )
br_cvm_adm_car_pes_jur.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_adm_car_pes_jur.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_cvm_adm_car_pes_jur.schedule = schedule_juridica
