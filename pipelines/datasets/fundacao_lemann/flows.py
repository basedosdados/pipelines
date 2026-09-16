"""
Flow fundacao_lemann — Prefect 3.
"""

from prefect import flow

from pipelines.utils.tasks import (
    download_data_to_gcs,
    rename_flow_run_dataset_table,
    run_dbt,
)


@flow(
    name="fundacao_lemann__ano_escola_serie_educacao_aprendizagem_adequada",
    log_prints=True,
)
def fundacao_lemann__ano_escola_serie_educacao_aprendizagem_adequada(
    dataset_id: str = "fundacao_lemann",
    table_id: str = "ano_escola_serie_educacao_aprendizagem_adequada",
    materialize_after_dump: bool = True,
    dbt_alias: bool = False,
    update_metadata: bool = False,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    download_data_to_gcs(dataset_id=dataset_id, table_id=table_id)


# pyrefly: ignore [missing-attribute]
fundacao_lemann__ano_escola_serie_educacao_aprendizagem_adequada.deploy_schedules = [
    {"cron": "0 9 1 1 *", "timezone": "America/Sao_Paulo"}
]
