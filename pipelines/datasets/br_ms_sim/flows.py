"""
Flows de br_ms_sim — Prefect 3.
"""

from prefect import flow

from pipelines.datasets.br_ms_sim.tasks import (
    clean_table,
    download_table,
    get_source_max_year,
    resolve_year_source,
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


@flow(
    name="br_ms_sim__microdados",
    log_prints=True,
)
def br_ms_sim__microdados(
    dataset_id: str = "br_ms_sim",
    table_id: str = "microdados",
    ano: int | None = None,
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    """Carrega um ano do SIM, do FTP do DATASUS até a materialização."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    backfill = ano is not None
    source_max_year = get_source_max_year()
    ano = int(ano if backfill else source_max_year)

    if not force_run and not backfill:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_year,
            env="prod",
            date_format=DateFormat.YEAR,
            compare_against="coverage",
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {table_id}!")
            return

    if not backfill:
        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_year,
            env="prod",
            date_format=DateFormat.YEAR,
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

    source = resolve_year_source(ano)
    print(f"Carregando {ano} a partir do diretório {source}")

    download_table(table_id=table_id, ano=ano, source=source)
    filepath = clean_table(table_id=table_id, ano=ano, source=source)

    upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
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
        source_format="csv",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target=target,
    )

    if update_metadata:
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


# `memory` não existe no template do work pool, que só conhece o par abaixo, e
# chave fora do template é descartada em silêncio — o pod ficaria no padrão.
# pyrefly: ignore [missing-attribute]
br_ms_sim__microdados.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
