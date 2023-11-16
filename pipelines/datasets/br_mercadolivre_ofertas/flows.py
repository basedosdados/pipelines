# -*- coding: utf-8 -*-
"""
Flows for mercadolivre_ofertas
"""
import datetime

# pylint: disable=invalid-name
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_mercadolivre_ofertas.schedules import every_day_item
from pipelines.datasets.br_mercadolivre_ofertas.tasks import (
    clean_item,
    clean_seller,
    crawler_mercadolivre_item,
    crawler_mercadolivre_seller,
    get_today_sellers,
    is_empty_list,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
)

with Flow(
    name="br_mercadolivre_ofertas.item", code_owners=["Gabriel Pisa"]
) as br_mercadolivre_ofertas_item:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_mercadolivre_ofertas", required=True
    )
    table_id = Parameter("table_id", default="item", required=True)
    table_id_sellers = Parameter("table_id_sellers", default="vendedor", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    materialize_after_dump_sellers = Parameter(
        "materialize_after_dump_sellers", default=True, required=False
    )

    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    data_atual = datetime.datetime.now().strftime("%Y-%m-%d")
    get_sellers = Parameter("get_sellers", default=True, required=True)

    filepath_raw = crawler_mercadolivre_item()

    seller_ids, seller_links = get_today_sellers(filepath_raw)

    filepath = clean_item(filepath_raw)

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
                "mode": materialization_mode,
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
        # update_django_metadata(
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     date_column_name={"date": "dia"},
        #     date_format="%Y-%m-%d",
        #     coverage_type="all_bdpro",
        #     prefect_mode=materialization_mode,
        #     bq_project="basedosdados",
        #     upstream_tasks=[wait_for_materialization],
        # )

    with case(get_sellers, True) and case(is_empty_list(seller_ids), False):
        # Trigger DBT flow run
        current_flow_labels = get_current_flow_labels()
        sellers_flow = create_flow_run(
            flow_name="br_mercadolivre_ofertas.vendedor",
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id_sellers,
                "mode": materialization_mode,
                "dbt_alias": dbt_alias,
                "seller_ids": seller_ids,
                "seller_links": seller_links,
                "materialize_after_dump": materialize_after_dump_sellers,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )

        wait_for_materialization = wait_for_flow_run(
            sellers_flow,
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
        # update_django_metadata(
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     date_column_name={"date": "dia"},
        #     date_format="%Y-%m-%d",
        #     coverage_type="all_bdpro",
        #     prefect_mode=materialization_mode,
        #     bq_project="basedosdados",
        #     upstream_tasks=[wait_for_materialization],
        # )

        materialization_flow.set_upstream([sellers_flow])

br_mercadolivre_ofertas_item.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_mercadolivre_ofertas_item.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_mercadolivre_ofertas_item.schedule = every_day_item

with Flow(
    name="br_mercadolivre_ofertas.vendedor", code_owners=["Gabriel Pisa"]
) as br_mercadolivre_ofertas_vendedor:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_mercadolivre_ofertas", required=True
    )
    table_id = Parameter("table_id", default="vendedor", required=True)
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    seller_ids = Parameter("seller_ids", default=None, required=False)
    seller_links = Parameter("seller_links", default=None, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    data_atual = datetime.datetime.now().strftime("%Y-%m-%d")
    filepath_raw = crawler_mercadolivre_seller(seller_ids, seller_links)

    filepath = clean_seller(filepath_raw)

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
                "mode": materialization_mode,
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
        # update_django_metadata(
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     date_column_name={"date": "dia"},
        #     date_format="%Y-%m-%d",
        #     coverage_type="all_bdpro",
        #     prefect_mode=materialization_mode,
        #     bq_project="basedosdados",
        #     upstream_tasks=[wait_for_materialization],
        # )

br_mercadolivre_ofertas_vendedor.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_mercadolivre_ofertas_vendedor.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
