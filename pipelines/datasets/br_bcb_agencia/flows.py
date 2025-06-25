# -*- coding: utf-8 -*-

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_bcb_agencia.constants import (
    constants as agencia_constants,
)
from pipelines.datasets.br_bcb_agencia.tasks import (
    clean_data,
    download_table,
    extract_last_date,
    get_output,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    update_django_metadata,
)
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

with Flow(
    name="br_bcb_agencia.agencia",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_agencia_agencia:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_bcb_agencia", required=True
    )
    table_id = Parameter("table_id", default="agencia", required=True)
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

    data_source_max_date = extract_last_date(
        table_id=table_id,
    )

    # task check if is outdated
    # check_if_outdated = check_if_data_is_outdated(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     data_source_max_date=data_source_max_date[0],
    #     date_format="%Y-%m",
    #     upstream_tasks=[data_source_max_date],
    # )

    # with case(check_if_outdated, False):
    #     log_task(f"Não há atualizações para a tabela de {table_id}!")

    # with case(check_if_outdated, True):
    #     log_task("Existem atualizações! A run será inciada")

    donwload_files = download_table(
        save_path=agencia_constants.ZIPFILE_PATH_AGENCIA.value,
        table_id=table_id,
        date=data_source_max_date[1],
        # upstream_tasks=[check_if_outdated],
    )

    filepath = clean_data(
        upstream_tasks=[donwload_files],
    )

    get_output = get_output(
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
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[filepath],
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
                date_format="%Y-%m",
                coverage_type="part_bdpro",
                time_delta={"months": 6},
                prefect_mode=target,
                bq_project="basedosdados",
                upstream_tasks=[upload_and_materialization_prod],
            )


br_bcb_agencia_agencia.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_agencia_agencia.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_bcb_agencia_agencia.schedule = every_month_agencia
