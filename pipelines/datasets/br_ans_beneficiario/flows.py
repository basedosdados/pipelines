# -*- coding: utf-8 -*-
"""
Flows for br_ans_beneficiario
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_ans_beneficiario.schedules import every_day_ans
from pipelines.datasets.br_ans_beneficiario.tasks import (
    check_condition,
    check_if_update_date_is_today,
    crawler_ans,
    extract_links_and_dates,
    files_to_download,
    get_file_max_date,
    get_output,
    is_empty,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.flows import update_django_metadata
from pipelines.utils.metadata.tasks import check_if_data_is_outdated
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

with Flow(
    name="br_ans_beneficiario.informacao_consolidada",
    code_owners=[
        "equipe_pipelines",
    ],
) as datasets_br_ans_beneficiario_flow:
    dataset_id = Parameter(
        "dataset_id", default="br_ans_beneficiario", required=False
    )
    table_id = Parameter(
        "table_id", default="informacao_consolidada", required=False
    )
    url = Parameter(
        "url",
        default="https://dadosabertos.ans.gov.br/FTP/PDA/informacoes_consolidadas_de_beneficiarios-024/",
        required=False,
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    target = Parameter("target", default="prod", required=False)

    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    force_update = Parameter("force_update", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    # Monta dataframe com as colunas: ["arquivo", "ultima_atualizacao", "data_hoje", "desatualizado"]
    links_and_dates = extract_links_and_dates(url=url)

    # Se update == True, força o update dos dados.
    with case(force_update, True):
        files = files_to_download(
            links_and_dates, upstream_tasks=[links_and_dates]
        )

    with case(force_update, False):
        # Se update == False, verifica se a data de hoje == data de atualização | data da cobertura temporal < data do último arquivo (ex: arquivo 202401 -> (2023-12-01 < 2024-01-01) )
        # Essa condição é verificada na task `check_condition()` abaixo.

        file_last_date = get_file_max_date(
            links_and_dates, upstream_tasks=[links_and_dates]
        )

        update_date_check = check_if_update_date_is_today(
            links_and_dates, upstream_tasks=[links_and_dates]
        )

        coverage_check = check_if_data_is_outdated(
            dataset_id,
            table_id,
            data_source_max_date=file_last_date,
            date_format="%Y-%m-%d",
            upstream_tasks=[links_and_dates, file_last_date],
        )

        with case(check_condition(update_date_check, coverage_check), True):
            # Se check_condition = True, cria a lista com os arquivos para o download.
            files = files_to_download(
                links_and_dates, upstream_tasks=[links_and_dates]
            )

    with case(is_empty(files), False):
        output_filepath = crawler_ans(files, upstream_tasks=[files])
        upload_and_materialization_dev = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="dev",
                bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[output_filepath],
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
                    date_format="%Y-%m-%d",
                    coverage_type="part_bdpro",
                    time_delta={"months": 6},
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[upload_and_materialization_prod],
                )

datasets_br_ans_beneficiario_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
datasets_br_ans_beneficiario_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
datasets_br_ans_beneficiario_flow.schedule = every_day_ans
