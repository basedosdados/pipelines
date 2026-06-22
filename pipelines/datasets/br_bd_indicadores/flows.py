"""
Flows for br_bd_indicadores — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bd_indicadores.tasks import (
    crawler_metricas,
    crawler_real_time,
    crawler_report_ga,
    get_data_from_sheet,
    get_ga_credentials,
    get_twitter_credentials,
    has_new_tweets,
    save_data_to_csv,
)
from pipelines.utils.tasks import (
    download_data_to_gcs,
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

_DATASET = "br_bd_indicadores"


def _upload_and_dbt(
    filepath: str,
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    target: str,
) -> None:
    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )
    if not materialize_after_dump:
        return
    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )


@flow(name="br_bd_indicadores__twitter_metrics", log_prints=True)
def br_bd_indicadores__twitter_metrics(
    dataset_id: str = _DATASET,
    table_id: str = "twitter_metrics",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )
    creds = get_twitter_credentials(secret_path="twitter_credentials")
    (
        access_secret,
        access_token,
        consumer_key,
        consumer_secret,
        bearer_token,
    ) = creds
    if not has_new_tweets(bearer_token, table_id=table_id):
        print("No tweets to update")
        return
    filepath = crawler_metricas(
        access_secret,
        access_token,
        consumer_key,
        consumer_secret,
        table_id=table_id,
    )
    _upload_and_dbt(
        filepath,
        dataset_id,
        table_id,
        materialize_after_dump,
        dbt_alias,
        target,
    )


@flow(name="br_bd_indicadores__twitter_metrics_agg", log_prints=True)
def br_bd_indicadores__twitter_metrics_agg(
    dataset_id: str = _DATASET,
    table_id: str = "twitter_metrics_agg",
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )
    download_data_to_gcs(dataset_id=dataset_id, table_id=table_id)


@flow(name="br_bd_indicadores__page_views", log_prints=True)
def br_bd_indicadores__page_views(
    dataset_id: str = _DATASET,
    table_id: str = "page_views",
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )
    property_id = get_ga_credentials(
        secret_path="ga_credentials", key="property_id"
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
    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )


@flow(name="br_bd_indicadores__website_user", log_prints=True)
def br_bd_indicadores__website_user(
    dataset_id: str = _DATASET,
    table_id: str = "website_user",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )
    view_id = get_ga_credentials(secret_path="ga_credentials", key="view_id")
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
    )
    _upload_and_dbt(
        filepath,
        dataset_id,
        table_id,
        materialize_after_dump,
        dbt_alias,
        target,
    )


def _sheet_flow_body(
    dataset_id: str,
    table_id: str,
    sheet_id: str,
    sheet_name: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    target: str,
    filename: str,
    usecols: int | None = None,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )
    if usecols is None:
        df = get_data_from_sheet(sheet_id=sheet_id, sheet_name=sheet_name)
    else:
        df = get_data_from_sheet(
            sheet_id=sheet_id, sheet_name=sheet_name, usecols=usecols
        )
    filepath = save_data_to_csv(df=df, filename=filename)
    _upload_and_dbt(
        filepath,
        dataset_id,
        table_id,
        materialize_after_dump,
        dbt_alias,
        target,
    )


@flow(name="br_bd_indicadores__contabilidade", log_prints=True)
def br_bd_indicadores__contabilidade(
    dataset_id: str = _DATASET,
    table_id: str = "contabilidade",
    sheet_id: str = "1jtZAV2SFEdEX99DumpUQ1LjZE2vcSgvL4DNo4n6HIec",
    sheet_name: str = "transacoes_anonimizado",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    _sheet_flow_body(
        dataset_id,
        table_id,
        sheet_id,
        sheet_name,
        materialize_after_dump,
        dbt_alias,
        target,
        "contabilidade",
    )


@flow(name="br_bd_indicadores__receitas_planejadas", log_prints=True)
def br_bd_indicadores__receitas_planejadas(
    dataset_id: str = _DATASET,
    table_id: str = "receitas_planejadas",
    sheet_id: str = "1fHp1NNUyhFIAAJ9bZOdZ2i9PSLIbkjSjMcGAlaxur90",
    sheet_name: str = "receitas_planejadas_anonimizado",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    _sheet_flow_body(
        dataset_id,
        table_id,
        sheet_id,
        sheet_name,
        materialize_after_dump,
        dbt_alias,
        target,
        "receitas_planejadas",
    )


@flow(name="br_bd_indicadores__equipes", log_prints=True)
def br_bd_indicadores__equipes(
    dataset_id: str = _DATASET,
    table_id: str = "equipes",
    sheet_id: str = "1gLJyoxiFeIRn7FKiP3Fpbr04bScVuhmF",
    sheet_name: str = "equipes",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    _sheet_flow_body(
        dataset_id,
        table_id,
        sheet_id,
        sheet_name,
        materialize_after_dump,
        dbt_alias,
        target,
        "equipes",
        usecols=6,
    )


@flow(name="br_bd_indicadores__pessoas", log_prints=True)
def br_bd_indicadores__pessoas(
    dataset_id: str = _DATASET,
    table_id: str = "pessoas",
    sheet_id: str = "1cQj9ItJoO_AQElRT2ngpHZXhFCSpQCrV",
    sheet_name: str = "pessoas",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    target: str = "prod",
) -> None:
    _sheet_flow_body(
        dataset_id,
        table_id,
        sheet_id,
        sheet_name,
        materialize_after_dump,
        dbt_alias,
        target,
        "pessoas",
        usecols=9,
    )


# Schedules — apenas contabilidade e receitas tinham schedule no Prefect 0
br_bd_indicadores__contabilidade.deploy_schedules = []
br_bd_indicadores__receitas_planejadas.deploy_schedules = []
br_bd_indicadores__twitter_metrics.deploy_schedules = []
br_bd_indicadores__twitter_metrics_agg.deploy_schedules = []
br_bd_indicadores__page_views.deploy_schedules = []
br_bd_indicadores__website_user.deploy_schedules = []
br_bd_indicadores__equipes.deploy_schedules = []
br_bd_indicadores__pessoas.deploy_schedules = []
