"""
Flows for ibge inflacao
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.ibge_inflacao.tasks import (
    check_for_updates,
    clean_mes_brasil,
    clean_mes_geral,
    clean_mes_municipio,
    clean_mes_rm,
    crawler,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import update_django_metadata
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="BD Template - IBGE Inflação: mes_brasil"
) as flow_ibge_inflacao_mes_brasil_:
    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
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

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )

    filepath = clean_mes_brasil(indice=INDICE, upstream_tasks=[was_downloaded])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )
        wait_for_dowload_data_to_gcs = download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[wait_for_materialization],
        )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )


flow_ibge_inflacao_mes_brasil_.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_brasil_.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow("BD Template - IBGE Inflação: mes_rm") as flow_ibge_inflacao_mes_rm:
    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
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

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )

    filepath = clean_mes_rm(indice=INDICE, upstream_tasks=[was_downloaded])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )
        wait_for_dowload_data_to_gcs = download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[wait_for_materialization],
        )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )

flow_ibge_inflacao_mes_rm.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_rm.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    "BD Template - IBGE Inflação: mes_municipio"
) as flow_ibge_inflacao_mes_municipio_:
    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
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

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )

    filepath = clean_mes_municipio(
        indice=INDICE, upstream_tasks=[was_downloaded]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_alias=dbt_alias,
            dbt_command="run/test",
            disable_elementary=False,
            upstream_tasks=[wait_upload_table],
        )
        wait_for_dowload_data_to_gcs = download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[wait_for_materialization],
        )
        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )


flow_ibge_inflacao_mes_municipio_.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
flow_ibge_inflacao_mes_municipio_.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


with Flow(
    "BD Template - IBGE Inflação: mes_geral"
) as flow_ibge_inflacao_mes_geral:
    # Parameters
    INDICE = Parameter("indice")
    FOLDER = Parameter("folder")
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
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

    needs_to_update = check_for_updates(
        indice=INDICE, dataset_id=dataset_id, table_id=table_id
    )

    with case(needs_to_update[0], True):
        was_downloaded = crawler(
            indice=INDICE, folder=FOLDER, upstream_tasks=[needs_to_update]
        )

    filepath = clean_mes_geral(indice=INDICE, upstream_tasks=[was_downloaded])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    with case(materialize_after_dump, True):
        wait_for_materialization = run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            target=target,
            dbt_alias=dbt_alias,
            upstream_tasks=[wait_upload_table],
        )

        wait_for_dowload_data_to_gcs = download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[wait_for_materialization],
        )

        with case(update_metadata, True):
            update_django_metadata(
                dataset_id=dataset_id,
                table_id=table_id,
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[wait_for_dowload_data_to_gcs],
            )

flow_ibge_inflacao_mes_geral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_ibge_inflacao_mes_geral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
