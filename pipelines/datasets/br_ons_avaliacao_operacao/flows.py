# -*- coding: utf-8 -*-
"""
Flows for br_ons_avaliacao_operacao
"""

# pylint: disable=invalid-name

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_ons_avaliacao_operacao.constants import (
    constants as ons_constants,
)
from pipelines.datasets.br_ons_avaliacao_operacao.tasks import (
    download_data,
    wrang_data,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.metadata.tasks import (
    task_get_api_most_recent_date,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_ons_avaliacao_operacao.reservatorio", code_owners=["Gabriel Pisa"]
) as br_ons_avaliacao_operacao_reservatorio:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter("table_id", default="reservatorio", required=True)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

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

    data_mais_recente_do_bq = task_get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m-%d",
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[0],
        upstream_tasks=[data_mais_recente_do_bq],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[0],
        data_mais_recente_do_bq=data_mais_recente_do_bq,
        upstream_tasks=[dow_data, data_mais_recente_do_bq],
    )

    with case(filepath[0], True):
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath[1],
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
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_ons_avaliacao_operacao_reservatorio.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_avaliacao_operacao_reservatorio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ons_avaliacao_operacao_reservatorio.schedule = (
#    schedule_br_ons_avaliacao_operacao_reservatorio
# )

with Flow(
    name="br_ons_avaliacao_operacao.geracao_usina",
    code_owners=["Gabriel Pisa"],
) as br_ons_avaliacao_operacao_geracao_usina:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter("table_id", default="geracao_usina", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
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

    data_mais_recente_do_bq = task_get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m-%d",
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[1],
        upstream_tasks=[data_mais_recente_do_bq],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[1],
        data_mais_recente_do_bq=data_mais_recente_do_bq,
        upstream_tasks=[dow_data, data_mais_recente_do_bq],
    )

    with case(filepath[0], True):
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath[1],
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
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_ons_avaliacao_operacao_geracao_usina.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_avaliacao_operacao_geracao_usina.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ons_avaliacao_operacao_geracao_usina.schedule = (
#    schedule_br_ons_avaliacao_operacao_geracao_usina
# )

with Flow(
    name="br_ons_avaliacao_operacao.geracao_termica_motivo_despacho",
    code_owners=["Gabriel Pisa"],
) as br_ons_avaliacao_operacao_geracao_termica_motivo_despacho:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter(
        "table_id", default="geracao_termica_motivo_despacho", required=True
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
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

    data_mais_recente_do_bq = task_get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m-%d",
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[2],
        upstream_tasks=[data_mais_recente_do_bq],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[2],
        data_mais_recente_do_bq=data_mais_recente_do_bq,
        upstream_tasks=[dow_data, data_mais_recente_do_bq],
    )

    with case(filepath[0], True):
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath[1],
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
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_ons_avaliacao_operacao_geracao_termica_motivo_despacho.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_avaliacao_operacao_geracao_termica_motivo_despacho.run_config = (
    KubernetesRun(image=constants.DOCKER_IMAGE.value)
)
# br_ons_avaliacao_operacao_geracao_termica_motivo_despacho.schedule = (
#    schedule_br_ons_avaliacao_operacao_geracao_termica_motivo_despacho
# )

with Flow(
    name="br_ons_avaliacao_operacao.energia_natural_afluente",
    code_owners=["Gabriel Pisa"],
) as br_ons_avaliacao_operacao_energia_natural_afluente:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter(
        "table_id", default="energia_natural_afluente", required=True
    )
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )

    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    data_mais_recente_do_bq = task_get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m-%d",
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[3],
        upstream_tasks=[data_mais_recente_do_bq],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[3],
        data_mais_recente_do_bq=data_mais_recente_do_bq,
        upstream_tasks=[dow_data, data_mais_recente_do_bq],
    )

    with case(filepath[0], True):
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath[1],
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
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_ons_avaliacao_operacao_energia_natural_afluente.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_avaliacao_operacao_energia_natural_afluente.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ons_avaliacao_operacao_energia_natural_afluente.schedule = (
#    schedule_br_ons_avaliacao_operacao_energia_natural_afluente
# )

with Flow(
    name="br_ons_avaliacao_operacao.energia_armazenada_reservatorio",
    code_owners=["Gabriel Pisa"],
) as br_ons_energia_armazenada_reservatorio:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter(
        "table_id", default="energia_armazenada_reservatorio", required=True
    )
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

    data_mais_recente_do_bq = task_get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m-%d",
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[4],
        upstream_tasks=[data_mais_recente_do_bq],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[4],
        data_mais_recente_do_bq=data_mais_recente_do_bq,
        upstream_tasks=[dow_data, data_mais_recente_do_bq],
    )

    with case(filepath[0], True):
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath[1],
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
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_ons_energia_armazenada_reservatorio.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_energia_armazenada_reservatorio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ons_energia_armazenada_reservatorio.schedule = (
#   schedule_br_ons_avaliacao_operacao_energia_armazenada_reservatorio
#

with Flow(
    name="br_ons_avaliacao_operacao.restricao_operacao_usinas_eolicas",
    code_owners=["Gabriel Pisa"],
) as br_ons_restricao_operacao_usinas_eolicas:
    # Parameters
    dataset_id = Parameter(
        "dataset_id", default="br_ons_avaliacao_operacao", required=True
    )
    table_id = Parameter(
        "table_id", default="restricao_operacao_usinas_eolicas", required=True
    )
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

    data_mais_recente_do_bq = task_get_api_most_recent_date(
        table_id=table_id,
        dataset_id=dataset_id,
        date_format="%Y-%m-%d",
    )

    dow_data = download_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[5],
        upstream_tasks=[data_mais_recente_do_bq],
    )

    filepath = wrang_data(
        table_name=ons_constants.TABLE_NAME_LIST.value[5],
        data_mais_recente_do_bq=data_mais_recente_do_bq,
        upstream_tasks=[dow_data, data_mais_recente_do_bq],
    )

    with case(filepath[0], True):
        wait_upload_table = create_table_and_upload_to_gcs(
            data_path=filepath[1],
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
                    date_column_name={"date": "data"},
                    date_format="%Y-%m-%d",
                    coverage_type="all_free",
                    prefect_mode=target,
                    bq_project="basedosdados",
                    upstream_tasks=[wait_for_dowload_data_to_gcs],
                )

br_ons_restricao_operacao_usinas_eolicas.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_ons_restricao_operacao_usinas_eolicas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_ons_restricao_operacao_usinas_eolicas.schedule = (
#    schedule_br_ons_avaliacao_operacao_restricao_operacao_usinas_eolicas
# )
