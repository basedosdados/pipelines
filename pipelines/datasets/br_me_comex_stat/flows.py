# -*- coding: utf-8 -*-
"""
Flows for br_me_comex_stat
"""

# pylint: disable=invalid-name

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_me_comex_stat.constants import (
    constants as comex_constants,
)
from pipelines.datasets.br_me_comex_stat.schedules import (
    schedule_municipio_exportacao,
    schedule_municipio_importacao,
    schedule_ncm_exportacao,
    schedule_ncm_importacao,
)
from pipelines.datasets.br_me_comex_stat.tasks import (
    clean_br_me_comex_stat,
    download_br_me_comex_stat,
    get_output,
    parse_last_date,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    log_task,
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

with Flow(
    name="br_me_comex_stat.municipio_exportacao", code_owners=["Gabriel Pisa"]
) as br_comex_municipio_exportacao:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_me_comex_stat", required=True
    )
    table_id = Parameter(
        "table_id", default="municipio_exportacao", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

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

    last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=last_date,
        date_format="%Y-%m",
        upstream_tasks=[last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será iniciada")

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[1],
        year_download=last_date,
        upstream_tasks=[check_if_outdated],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[1],
        upstream_tasks=[download_data],
    )

    get_output = get_output(
        table_name=comex_constants.TABLE_NAME.value[1],
        upstream_tasks=[filepath],
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
        # coverage updater
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
                upstream_tasks=[upload_and_materialization_prod],
            )

br_comex_municipio_exportacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_municipio_exportacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_comex_municipio_exportacao.schedule = schedule_municipio_exportacao


with Flow(
    name="br_me_comex_stat.municipio_importacao", code_owners=["Gabriel Pisa"]
) as br_comex_municipio_importacao:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_me_comex_stat", required=True
    )
    table_id = Parameter(
        "table_id", default="municipio_importacao", required=True
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=last_date,
        date_format="%Y-%m",
        upstream_tasks=[last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será iniciada")

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[0],
        year_download=last_date,
        upstream_tasks=[check_if_outdated],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[0],
        table_name=comex_constants.TABLE_NAME.value[0],
        upstream_tasks=[download_data],
    )

    get_output = get_output(
        table_name=comex_constants.TABLE_NAME.value[0],
        upstream_tasks=[filepath],
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
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )


br_comex_municipio_importacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_municipio_importacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_comex_municipio_importacao.schedule = schedule_municipio_importacao


with Flow(
    name="br_me_comex_stat.ncm_exportacao", code_owners=["Gabriel Pisa"]
) as br_comex_ncm_exportacao:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_me_comex_stat", required=True
    )
    table_id = Parameter("table_id", default="ncm_exportacao", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=last_date,
        date_format="%Y-%m",
        upstream_tasks=[last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será iniciada")

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[3],
        year_download=last_date,
        upstream_tasks=[check_if_outdated],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[3],
        upstream_tasks=[download_data],
    )

    get_output = get_output(
        table_name=comex_constants.TABLE_NAME.value[3],
        upstream_tasks=[filepath],
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
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )


br_comex_ncm_exportacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_ncm_exportacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_comex_ncm_exportacao.schedule = schedule_ncm_exportacao


with Flow(
    name="br_me_comex_stat.ncm_importacao", code_owners=["Gabriel Pisa"]
) as br_comex_ncm_importacao:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_me_comex_stat", required=True
    )
    table_id = Parameter("table_id", default="ncm_importacao", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)

    check_if_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=last_date,
        date_format="%Y-%m",
        upstream_tasks=[last_date],
    )

    with case(check_if_outdated, False):
        log_task(f"Não há atualizações para a tabela de {table_id}!")

    with case(check_if_outdated, True):
        log_task("Existem atualizações! A run será iniciada")

    download_data = download_br_me_comex_stat(
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[2],
        year_download=last_date,
        upstream_tasks=[check_if_outdated],
    )

    filepath = clean_br_me_comex_stat(
        path=comex_constants.PATH.value,
        table_type=comex_constants.TABLE_TYPE.value[1],
        table_name=comex_constants.TABLE_NAME.value[2],
        upstream_tasks=[download_data],
    )

    get_output = get_output(
        table_name=comex_constants.TABLE_NAME.value[2],
        upstream_tasks=[filepath],
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
                date_column_name={"year": "ano", "month": "mes"},
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )

br_comex_ncm_importacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_comex_ncm_importacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_comex_ncm_importacao.schedule = schedule_ncm_importacao
