# -*- coding: utf-8 -*-
"""
Flows for br_cvm_oferta_publica_distribuicao
"""
# pylint: disable=C0103, E1123, invalid-name
from datetime import datetime, timedelta

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect import Parameter, case
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.constants import constants
from pipelines.datasets.br_cvm_oferta_publica_distribuicao.tasks import (
    crawl,
    clean_table_oferta_distribuicao,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_metadata,
    get_temporal_coverage,
    rename_current_flow_run_dataset_table,
    get_current_flow_labels,
)
from pipelines.datasets.br_cvm_oferta_publica_distribuicao.schedules import schedule_dia

ROOT = "/tmp/data"
URL = "http://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.csv"

with Flow(
    name="br_cvm_oferta_publica_distribuicao.dia", code_owners=["lucas_cr"]
) as br_cvm_ofe_pub_dis_dia:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_cvm_oferta_publica_distribuicao", required=True
    )
    table_id = Parameter("table_id", default="dia", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    wait_crawl = crawl(root=ROOT, url=URL)
    filepath = clean_table_oferta_distribuicao(root=ROOT, upstream_tasks=[wait_crawl])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    # update_metadata
    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["data_abertura_processo"],
        time_unit="day",
        interval="1",
        upstream_tasks=[wait_upload_table],
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            {"temporal_coverage": [temporal_coverage]},
        ],
        upstream_tasks=[temporal_coverage],
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
                "mode": materialization_mode,
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

br_cvm_ofe_pub_dis_dia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cvm_ofe_pub_dis_dia.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cvm_ofe_pub_dis_dia.schedule = schedule_dia
