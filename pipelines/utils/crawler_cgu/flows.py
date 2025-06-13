# -*- coding: utf-8 -*-
"""
Flows for br_cgu_cartao_pagamento
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.crawler_cgu.tasks import (
    dict_for_table,
    get_current_date_and_download_file,
    get_output,
    partition_data,
    read_and_partition_beneficios_cidadao,
    verify_all_url_exists_to_download,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

with Flow(
    name="CGU - Cartão de Pagamento", code_owners=["trick"]
) as flow_cgu_cartao_pagamento:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_cartao_pagamento", required=True
    )
    table_id = Parameter("table_id", required=True)
    ####
    # Relative_month =  1 means that the data will be downloaded for the current month
    ####
    relative_month = Parameter("relative_month", default=1, required=False)
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

    data_source_max_date = get_current_date_and_download_file(
        table_id,
        dataset_id,
        relative_month,
    )

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m",
        upstream_tasks=[data_source_max_date],
    )

    # with case(dados_desatualizados, True):
    filepath = partition_data(
        table_id=table_id,
        dataset_id=dataset_id,
    )

    get_output = get_output(table_id=table_id, upstream_tasks=[filepath])

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
                date_column_name={
                    "year": "ano_extrato",
                    "month": "mes_extrato",
                },
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )

flow_cgu_cartao_pagamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cgu_cartao_pagamento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)

# ! ============================================== CGU - Servidores Públicos do Executivo Federal =============================================

with Flow(
    name="CGU - Servidores Públicos do Executivo Federal",
    code_owners=["trick"],
) as flow_cgu_servidores_publicos:
    dataset_id = Parameter(
        "dataset_id",
        default="br_cgu_servidores_executivo_federal",
        required=True,
    )
    table_id = Parameter("table_id", required=True)
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

    ####
    # Relative_month =  1 means that the data will be downloaded for the current month
    ####
    relative_month = Parameter("relative_month", default=1, required=False)

    with case(
        verify_all_url_exists_to_download(
            dataset_id, table_id, relative_month
        ),
        True,
    ):
        data_source_max_date = get_current_date_and_download_file(
            table_id,
            dataset_id,
            relative_month,
        )

        dados_desatualizados = check_if_data_is_outdated(
            dataset_id=dataset_id,
            table_id=table_id,
            data_source_max_date=data_source_max_date,
            date_format="%Y-%m",
            upstream_tasks=[data_source_max_date],
        )

    #     with case(dados_desatualizados, True):
    filepath = partition_data(
        table_id=table_id,
        dataset_id=dataset_id,
    )
    get_output = get_output(table_id=table_id, upstream_tasks=[filepath])

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
flow_cgu_servidores_publicos.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cgu_servidores_publicos.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


# ! ================================== CGU - Licitacao e Contrato =====================================

with Flow(
    name="CGU - Licitacão e Contrato", code_owners=["trick"]
) as flow_cgu_licitacao_contrato:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_licitacao_contrato", required=True
    )
    table_id = Parameter("table_id", required=True)
    ####
    # Relative_month =  1 means that the data will be downloaded for the current month
    ####
    relative_month = Parameter("relative_month", default=1, required=False)
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

    data_source_max_date = get_current_date_and_download_file(
        table_id=table_id,
        dataset_id=dataset_id,
        relative_month=relative_month,
    )

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m",
        upstream_tasks=[data_source_max_date],
    )

    # with case(dados_desatualizados, True):
    filepath = partition_data(
        table_id=table_id,
        dataset_id=dataset_id,
        # upstream_tasks=[data_source_max_date],
    )

    get_output = get_output(table_id=table_id, upstream_tasks=[filepath])

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
flow_cgu_licitacao_contrato.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cgu_licitacao_contrato.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


# ! ================================== CGU - Benefícios Cidadão ====================================

with Flow(
    name="CGU - Benefícios Cidadão", code_owners=["trick"]
) as flow_cgu_beneficios_cidadao:
    dataset_id = Parameter(
        "dataset_id", default="br_cgu_beneficios_cidadao", required=True
    )
    table_id = Parameter("table_id", required=True)
    ####
    # Relative_month =  1 means that the data will be downloaded for the current month
    ####
    relative_month = Parameter("relative_month", default=1, required=False)
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

    data_source_max_date = get_current_date_and_download_file(
        table_id=table_id,
        dataset_id=dataset_id,
        relative_month=relative_month,
    )

    dados_desatualizados = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=data_source_max_date,
        date_format="%Y-%m",
        upstream_tasks=[data_source_max_date],
    )

    # with case(dados_desatualizados, True):
    filepath = read_and_partition_beneficios_cidadao(
        table_id=table_id,
        upstream_tasks=[dados_desatualizados],
    )

    get_output = get_output(table_id=table_id, upstream_tasks=[filepath])

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
                date_column_name=dict_for_table(table_id),
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )

flow_cgu_beneficios_cidadao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cgu_beneficios_cidadao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
