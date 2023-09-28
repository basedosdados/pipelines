# -*- coding: utf-8 -*-
"""
Flows for br_rj_isp_estatisticas_seguranca.
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.constants import (
    constants as isp_constants,
)
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.schedules import (
    every_month_armas_apreendidas_mensal,
    every_month_evolucao_mensal_cisp,
    every_month_evolucao_mensal_municipio,
    every_month_evolucao_mensal_uf,
    every_month_evolucao_policial_morto_servico_mensal,
    every_month_feminicidio_mensal_cisp,
    every_month_taxa_evolucao_mensal_municipio,
    every_month_taxa_evolucao_mensal_uf,
)
from pipelines.datasets.br_rj_isp_estatisticas_seguranca.tasks import (
    clean_data,
    download_files,
    get_today_date,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
    update_django_metadata,
)

# ! Evolucao_mensal_cisp
with Flow(
    name="br_rj_isp_estatisticas_seguranca.evolucao_mensal_cisp",
    code_owners=[
        "trick",
    ],
) as evolucao_mensal_cisp:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter("table_id", default="evolucao_mensal_cisp", required=True)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    update_metadata = Parameter("update_metadata", default=True, required=False)

    d_files = download_files(
        file_name=isp_constants.EVOLUCAO_MENSAL_CISP.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.EVOLUCAO_MENSAL_CISP.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )

evolucao_mensal_cisp.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
evolucao_mensal_cisp.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
evolucao_mensal_cisp.schedule = every_month_evolucao_mensal_cisp


# ! taxa_evolucao_mensal_uf

with Flow(
    name="br_rj_isp_estatisticas_seguranca.taxa_evolucao_mensal_uf",
    code_owners=[
        "trick",
    ],
) as taxa_evolucao_mensal_uf:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter("table_id", default="taxa_evolucao_mensal_uf", required=True)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    update_metadata = Parameter("update_metadata", default=True, required=False)

    d_files = download_files(
        file_name=isp_constants.TAXA_EVOLUCAO_MENSAL_UF.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.TAXA_EVOLUCAO_MENSAL_UF.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )

taxa_evolucao_mensal_uf.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
taxa_evolucao_mensal_uf.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
taxa_evolucao_mensal_uf.schedule = every_month_taxa_evolucao_mensal_uf


# ! taxa_evolucao_mensal_municipio

with Flow(
    name="br_rj_isp_estatisticas_seguranca.taxa_evolucao_mensal_municipio",
    code_owners=[
        "trick",
    ],
) as taxa_evolucao_mensal_municipio:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter(
        "table_id", default="taxa_evolucao_mensal_municipio", required=True
    )

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )
    update_metadata = Parameter("update_metadata", default=True, required=False)

    d_files = download_files(
        file_name=isp_constants.TAXA_EVOLUCAO_MENSAL_MUNICIPIO.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.TAXA_EVOLUCAO_MENSAL_MUNICIPIO.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )


taxa_evolucao_mensal_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
taxa_evolucao_mensal_municipio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
taxa_evolucao_mensal_municipio.schedule = every_month_taxa_evolucao_mensal_municipio


# ! Feminicidio_mensal_cisp
with Flow(
    name="br_rj_isp_estatisticas_seguranca.feminicidio_mensal_cisp",
    code_owners=[
        "trick",
    ],
) as feminicidio_mensal_cisp:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter("table_id", default="feminicidio_mensal_cisp", required=True)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    d_files = download_files(
        file_name=isp_constants.FEMINICIDIO_MENSAL_CISP.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.FEMINICIDIO_MENSAL_CISP.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )


feminicidio_mensal_cisp.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
feminicidio_mensal_cisp.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
feminicidio_mensal_cisp.schedule = every_month_feminicidio_mensal_cisp

# ! evolucao_policial_morto_servico_mensal

with Flow(
    name="br_rj_isp_estatisticas_seguranca.evolucao_policial_morto_servico_mensal",
    code_owners=[
        "trick",
    ],
) as evolucao_policial_morto_servico_mensal:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter(
        "table_id", default="evolucao_policial_morto_servico_mensal", required=True
    )

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    d_files = download_files(
        file_name=isp_constants.EVOLUCAO_POLICIAL_MORTO.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.EVOLUCAO_POLICIAL_MORTO.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )

evolucao_policial_morto_servico_mensal.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
evolucao_policial_morto_servico_mensal.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
evolucao_policial_morto_servico_mensal.schedule = (
    every_month_evolucao_policial_morto_servico_mensal
)

# ! armas_apreendidas_mensal

with Flow(
    name="br_rj_isp_estatisticas_seguranca.armas_apreendidas_mensal",
    code_owners=[
        "trick",
    ],
) as armas_apreendidas_mensal:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter("table_id", default="armas_apreendidas_mensal", required=True)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    d_files = download_files(
        file_name=isp_constants.ARMAS_APREENDIDADAS_MENSAL.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.ARMAS_APREENDIDADAS_MENSAL.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )

armas_apreendidas_mensal.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
armas_apreendidas_mensal.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
armas_apreendidas_mensal.schedule = every_month_armas_apreendidas_mensal

# ! evolucao_mensal_municipio

with Flow(
    name="br_rj_isp_estatisticas_seguranca.evolucao_mensal_municipio",
    code_owners=[
        "trick",
    ],
) as evolucao_mensal_municipio:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter("table_id", default="evolucao_mensal_municipio", required=True)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    d_files = download_files(
        file_name=isp_constants.EVOLUCAO_MENSAL_MUNICIPIO.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.EVOLUCAO_MENSAL_MUNICIPIO.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )

evolucao_mensal_municipio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
evolucao_mensal_municipio.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
evolucao_mensal_municipio.schedule = every_month_evolucao_mensal_municipio

# ! evolucao_mensal_municipio

with Flow(
    name="br_rj_isp_estatisticas_seguranca.evolucao_mensal_uf",
    code_owners=[
        "trick",
    ],
) as evolucao_mensal_uf:
    dataset_id = Parameter(
        "dataset_id", default="br_rj_isp_estatisticas_seguranca", required=True
    )
    table_id = Parameter("table_id", default="evolucao_mensal_uf", required=True)

    # Materialization mode
    materialization_mode = Parameter(
        "materialization_mode", default="prod", required=False
    )
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    update_metadata = Parameter("update_metadata", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    d_files = download_files(
        file_name=isp_constants.EVOLUCAO_MENSAL_UF.value,
        save_dir=isp_constants.INPUT_PATH.value,
    )

    filepath = clean_data(
        file_name=isp_constants.EVOLUCAO_MENSAL_UF.value,
        upstream_tasks=[d_files],
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
        date = get_today_date()  # task que retorna a data atual
        update_django_metadata(
            dataset_id,
            table_id,
            metadata_type="DateTimeRange",
            bq_last_update=False,
            api_mode="prod",
            date_format="yy-mm",
            _last_date=date,
        )

evolucao_mensal_uf.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
evolucao_mensal_uf.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
evolucao_mensal_uf.schedule = every_month_evolucao_mensal_uf
