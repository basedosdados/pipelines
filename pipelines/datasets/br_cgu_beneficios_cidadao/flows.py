# -*- coding: utf-8 -*-
"""
Flows for br_cgu_bolsa_familia
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_cgu_beneficios_cidadao.tasks import (
    check_for_updates,
    crawl_last_date,
    crawler_bolsa_familia,
    crawler_bpc,
    crawler_garantia_safra,
    get_today_date,
    print_last_file,
    teste_selenium,
)
from pipelines.utils.constants import constants as utils_constants

# from pipelines.datasets.br_cgu_bolsa_familia.schedules import every_two_weeks
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.tasks import (  # update_django_metadata,
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    log_task,
    rename_current_flow_run_dataset_table,
)

###                 ###
#  NOVO BOLSA FAMÍLIA     #
###                 ###
with Flow(
    name="novo_bolsa_familia",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_cgu_bolsa_familia_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )
    table_id = Parameter("table_id", default="novo_bolsa_familia", required=False)
    historical_data = Parameter("historical_data", default=False, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    output_filepath = crawler_bolsa_familia(historical_data)
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        source_format="parquet",
        wait=output_filepath,
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
        with case(update_metadata, True):
            date = get_today_date()
            update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                _last_date=date,
                bq_table_last_year_month=False,
                bq_last_update=False,
                is_bd_pro=True,
                is_free=True,
                date_format="yy-mm",
                api_mode="prod",
                time_delta=6,
                time_unit="months",
            )

datasets_br_cgu_bolsa_familia_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_cgu_bolsa_familia_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# flow.schedule = every_two_weeks


###                 ###
#  GARANTIA SAFRA     #
###                 ###

with Flow(
    name="garantia_safra",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_cgu_garantia_safra_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )
    table_id = Parameter("table_id", default="garantia_safra", required=False)
    historical_data = Parameter("historical_data", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    output_filepath = crawler_garantia_safra(historical_data)
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        source_format="parquet",
        wait=output_filepath,
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
        with case(update_metadata, True):
            date = get_today_date()
            update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                _last_date=date,
                bq_table_last_year_month=False,
                bq_last_update=False,
                is_bd_pro=True,
                is_free=True,
                date_format="yy-mm",
                api_mode="prod",
                time_delta=6,
                time_unit="months",
            )

datasets_br_cgu_garantia_safra_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_cgu_garantia_safra_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

###                 ###
#  BPC     #
###                 ###

with Flow(
    name="bpc",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_cgu_bpc_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )
    table_id = Parameter("table_id", default="bpc", required=False)
    historical_data = Parameter("historical_data", default=True, required=False)
    update_metadata = Parameter("update_metadata", default=False, required=False)
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    a = crawl_last_date(dataset_id=dataset_id, table_id=table_id)
    output_filepath = crawler_bpc(historical_data, file=a[1])
    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        source_format="csv",
        wait=output_filepath,
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
        with case(update_metadata, True):
            date = get_today_date()
            update_django_metadata(
                dataset_id,
                table_id,
                metadata_type="DateTimeRange",
                _last_date=date,
                bq_table_last_year_month=False,
                bq_last_update=False,
                is_bd_pro=True,
                is_free=True,
                date_format="yy-mm",
                api_mode="prod",
                time_delta=6,
                time_unit="months",
            )

datasets_br_cgu_bpc_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_cgu_bpc_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)

###                 ###
#  TESTE     #
###                 ###

with Flow(
    name="teste_selenium",
    code_owners=[
        "arthurfg",
    ],
) as datasets_br_cgu_test_driver_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=False
    )
    table_id = Parameter("table_id", default="bpc", required=False)
    historical_data = Parameter("historical_data", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    # setup = setup_web_driver()
    # teste_selenium(upstream_tasks=[setup])
    a = crawl_last_date(dataset_id=dataset_id, table_id=table_id)
    update = check_for_updates(
        dataset_id=dataset_id, table_id=table_id, max_date=a[0], upstream_tasks=[a]
    )
    with case(update, True):
        print_last_file(a[1])
        crawler_bpc(historical_data, file=a[1])

datasets_br_cgu_test_driver_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datasets_br_cgu_test_driver_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
