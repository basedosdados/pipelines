"""
Flow compartilhado para br_tse_eleicoes — Prefect 3.
"""

from pipelines.crawler.tse_eleicoes.tasks import (
    flows_control,
    get_data_source_max_date,
    preparing_data,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearOnly,
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


def _run_tse_eleicoes(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool = False,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    flow = flows_control(table_id=table_id, mode="prod")
    data_source_max_date = get_data_source_max_date(flow_class=flow)

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=data_source_max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new_data:
            return

    ready_data_path = preparing_data(flow_class=flow)

    upload_to_gcs(
        data_path=ready_data_path,
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
        data_path=ready_data_path,
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
        # Legado lia data_eleicao (DATE) mas formatava p/ "%Y" → cobertura
        # year-only. O domínio refatorado proíbe DateOnly+YEAR (R4); a coluna
        # `ano` (partição canônica das tabelas TSE) dá a mesma granularidade
        # ano-only de forma R4-limpa. ⚠ oráculo in-pod: comparar só o ano.
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllFree(
                date_column=YearOnly(col="ano"),
                date_format=DateFormat.YEAR,
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if data_source_max_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=data_source_max_date,
                env="prod",
                date_format="%Y-%m-%d",
            )
