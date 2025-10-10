"""
Flows for br_bd_indicadores
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

# from pipelines.datasets.br_bd_indicadores.schedules import (
#     schedule_contabilidade,
#     schedule_equipes,
#     schedule_pessoas,
#     schedule_receitas,
# )
from pipelines.datasets.br_bd_indicadores.tasks import (
    crawler_metricas,
    crawler_real_time,
    crawler_report_ga,
    echo,
    get_data_from_sheet,
    get_ga_credentials,
    get_twitter_credentials,
    has_new_tweets,
    save_data_to_csv,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    download_data_to_gcs,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

with Flow(
    name="br_bd_indicadores.twitter_metrics",
    code_owners=[
        "lucas_cr",
    ],
) as bd_twt_metricas:
    # Parameters
    target = Parameter("target", default="prod", required=False)
    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter("table_id", default="twitter_metrics", required=True)
    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    (
        access_secret,
        access_token,
        consumer_key,
        consumer_secret,
        bearer_token,
    ) = get_twitter_credentials(secret_path="twitter_credentials", wait=None)

    cond = has_new_tweets(bearer_token, table_id=table_id)

    with case(cond, False):
        echo("No tweets to update")

    with case(cond, True):
        filepath = crawler_metricas(
            access_secret,
            access_token,
            consumer_key,
            consumer_secret,
            upstream_tasks=[cond],
            table_id=table_id,
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
                upstream_tasks=[wait_upload_table],
            )

            wait_for_dowload_data_to_gcs = download_data_to_gcs(
                dataset_id=dataset_id,
                table_id=table_id,
                upstream_tasks=[wait_for_materialization],
            )

bd_twt_metricas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# bd_twt_metricas.schedule = every_day


with Flow(
    name="br_bd_indicadores.twitter_metrics_agg", code_owners=["lucas_cr"]
) as bd_twt_metricas_agg:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter(
        "table_id", default="twitter_metrics_agg", required=True
    )
    target = Parameter("target", default="prod", required=False)
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

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

bd_twt_metricas_agg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_twt_metricas_agg.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# bd_twt_metricas_agg.schedule = every_week


with Flow(
    name="br_bd_indicadores.page_views",
    code_owners=[
        "lucas_cr",
    ],
) as bd_pageviews:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter("table_id", default="page_views", required=True)
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    property_id = get_ga_credentials(
        secret_path="ga_credentials", key="property_id", wait=None
    )

    filepath = crawler_real_time(
        lst_dimension=["country", "city", "unifiedScreenName"],
        lst_metric=[
            "activeUsers",
            "conversions",
            "eventCount",
            "screenPageViews",
        ],
        property_id=property_id,
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )


bd_pageviews.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_pageviews.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)


with Flow(
    name="br_bd_indicadores.ga_users",
    code_owners=[
        "lucas_cr",
    ],
) as bd_ga_users:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter("table_id", default="website_user", required=True)

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize after dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    view_id = get_ga_credentials(
        secret_path="ga_credentials", key="view_id", wait=None
    )

    filepath = crawler_report_ga(
        view_id=view_id,
        metrics=[
            "1dayUsers",
            "7dayUsers",
            "14dayUsers",
            "28dayUsers",
            "30dayUsers",
            "newUsers",
        ],
        upstream_tasks=[view_id],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )

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


bd_ga_users.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_ga_users.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# bd_ga_users.schedule = schedule_users


with Flow(
    name="br_bd_indicadores.contabilidade",
    code_owners=[],
) as bd_indicadores_contabilidade:
    # force deploy
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter("table_id", default="contabilidade", required=True)
    # manual id for the cases where the original file breaks
    # 1OfPAtE42M53rPm-qVWl8pbH8bN16x3uUxXA2WZunlJc
    sheet_id = Parameter(
        "sheet_id",
        default="1jtZAV2SFEdEX99DumpUQ1LjZE2vcSgvL4DNo4n6HIec",
        required=True,
    )
    sheet_name = Parameter(
        "sheet_name", default="transacoes_anonimizado", required=True
    )

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    df_contabilidade = get_data_from_sheet(
        sheet_id=sheet_id, sheet_name=sheet_name
    )
    filepath = save_data_to_csv(
        df=df_contabilidade,
        filename="contabilidade",
        upstream_tasks=[df_contabilidade],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )

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

bd_indicadores_contabilidade.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_indicadores_contabilidade.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# bd_indicadores_contabilidade.schedule = schedule_contabilidade


with Flow(
    name="br_bd_indicadores.receitas_planejadas",
    code_owners=[],
) as bd_indicadores_receitas_planejadas:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter(
        "table_id", default="receitas_planejadas", required=True
    )
    sheet_id = Parameter(
        "sheet_id",
        default="1fHp1NNUyhFIAAJ9bZOdZ2i9PSLIbkjSjMcGAlaxur90",
        required=True,
    )
    sheet_name = Parameter(
        "sheet_name", default="receitas_planejadas_anonimizado", required=True
    )

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    df_receitas = get_data_from_sheet(sheet_id=sheet_id, sheet_name=sheet_name)
    filepath = save_data_to_csv(
        df=df_receitas,
        filename="receitas_planejadas",
        upstream_tasks=[df_receitas],
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )

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

bd_indicadores_receitas_planejadas.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
bd_indicadores_receitas_planejadas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# bd_indicadores_receitas_planejadas.schedule = schedule_receitas


with Flow(
    name="br_bd_indicadores.equipes",
    code_owners=[],
) as bd_indicadores_equipes:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter("table_id", default="equipes", required=True)
    sheet_id = Parameter(
        "sheet_id",
        default="1gLJyoxiFeIRn7FKiP3Fpbr04bScVuhmF",
        required=True,
    )
    bd_indicadores_equipes.add_task(sheet_id)

    sheet_name = Parameter("sheet_name", default="equipes", required=True)
    bd_indicadores_equipes.add_task(sheet_name)

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    df_equipes = get_data_from_sheet(
        sheet_id, sheet_name, usecols=6, upstream_tasks=[sheet_id, sheet_name]
    )
    filepath = save_data_to_csv(
        df=df_equipes, filename="equipes", upstream_tasks=[df_equipes]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )

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

bd_indicadores_equipes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_indicadores_equipes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# bd_indicadores_equipes.schedule = schedule_equipes


with Flow(
    name="br_bd_indicadores.pessoas",
    code_owners=[],
) as bd_indicadores_pessoas:
    dataset_id = Parameter(
        "dataset_id", default="br_bd_indicadores", required=True
    )
    table_id = Parameter("table_id", default="pessoas", required=True)
    sheet_id = Parameter(
        "sheet_id",
        default="1cQj9ItJoO_AQElRT2ngpHZXhFCSpQCrV",
        required=True,
    )
    sheet_name = Parameter("sheet_name", default="pessoas", required=True)

    target = Parameter("target", default="prod", required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    df_pessoas = get_data_from_sheet(
        sheet_id=sheet_id, sheet_name=sheet_name, usecols=9
    )
    filepath = save_data_to_csv(
        df=df_pessoas, filename="pessoas", upstream_tasks=[df_pessoas]
    )

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        wait=filepath,
        upstream_tasks=[filepath],
    )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )

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

bd_indicadores_pessoas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bd_indicadores_pessoas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# bd_indicadores_pessoas.schedule = schedule_pessoas
