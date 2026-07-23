"""
Flows for br_me_comex_stat — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.me_comex_stat.constants import (
    constants as comex_constants,
)
from pipelines.crawler.me_comex_stat.tasks import (
    clean_br_me_comex_stat,
    download_br_me_comex_stat,
    parse_last_date,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _comex_flow(table_id: str, table_name: str, table_type: str, cron: str):
    @flow(
        name=f"br_me_comex_stat__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_me_comex_stat",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        # pyrefly: ignore [unused-coroutine]
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
        )

        last_date = parse_last_date(link=comex_constants.DOWNLOAD_LINK.value)

        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=last_date,
                env="prod",
                date_format="%Y-%m",
            )
            if not has_new_data:
                return

        download_br_me_comex_stat(
            table_name=table_name,
            year_download=last_date,
        )

        filepath = clean_br_me_comex_stat(
            path=comex_constants.PATH.value,
            table_type=table_type,
            table_name=table_name,
        )

        # pyrefly: ignore [no-matching-overload]
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

        # pyrefly: ignore [no-matching-overload]
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

        if update_metadata:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=PartBdpro(
                    date_column=YearMonth(year="ano", month="mes"),
                    date_format=DateFormat.YEAR_MONTH,
                ),
                env="prod",
                bq_project="basedosdados",
            )

            if last_date is not None:
                commit_source_update_task(
                    dataset_id=dataset_id,
                    table_id=table_id,
                    source_max_date=last_date,
                    env="prod",
                    date_format="%Y-%m",
                )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_me_comex_stat__municipio_exportacao = _comex_flow(
    table_id="municipio_exportacao",
    table_name=comex_constants.TABLE_NAME.value[1],
    table_type=comex_constants.TABLE_TYPE.value[0],
    cron="0 21 * * 1-5",
)

br_me_comex_stat__municipio_importacao = _comex_flow(
    table_id="municipio_importacao",
    table_name=comex_constants.TABLE_NAME.value[0],
    table_type=comex_constants.TABLE_TYPE.value[0],
    cron="0 20 * * 1-5",
)

br_me_comex_stat__ncm_exportacao = _comex_flow(
    table_id="ncm_exportacao",
    table_name=comex_constants.TABLE_NAME.value[3],
    table_type=comex_constants.TABLE_TYPE.value[1],
    cron="0 8,17 * * 1-5",
)

br_me_comex_stat__ncm_importacao = _comex_flow(
    table_id="ncm_importacao",
    table_name=comex_constants.TABLE_NAME.value[2],
    table_type=comex_constants.TABLE_TYPE.value[1],
    cron="0 8,17 * * 1-5",
)
