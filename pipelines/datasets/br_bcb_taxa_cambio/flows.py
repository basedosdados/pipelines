"""
Flow br_bcb_taxa_cambio — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bcb_taxa_cambio.tasks import (
    get_data_taxa_cambio,
    get_source_max_date,
    treat_data_taxa_cambio,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
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
    name="br_bcb_taxa_cambio__taxa_cambio",
    log_prints=True,
)
def br_bcb_taxa_cambio__taxa_cambio(
    dataset_id: str = "br_bcb_taxa_cambio",
    table_id: str = "taxa_cambio",
    anos: list[int] | None = None,
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    """Carrega as cotações do PTAX, do Olinda até a materialização.

    `anos` vazio baixa o ano corrente e só age se a fonte tiver publicado data
    mais nova que a cobertura da tabela — é o que a execução agendada faz.
    Passar uma lista recarrega esses anos sem consultar a fonte, para consertar
    partição incompleta ou duplicada.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    backfill = bool(anos)
    anos_alvo: list[int | None] = list(anos) if backfill else [None]

    if not backfill:
        source_max_date = get_source_max_date()

        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=source_max_date,
                env="prod",
                date_format=DateFormat.YEAR_MD,
                compare_against="coverage",
            )
            if not has_new_data:
                print(f"Não há atualizações para a tabela {table_id}!")
                return

        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
            env="prod",
            date_format=DateFormat.YEAR_MD,
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

    output_paths = []
    for ano in anos_alvo:
        print(f"Carregando {ano or 'ano corrente'}")
        get_data_taxa_cambio(table_id=table_id, ano=ano)
        file_info = treat_data_taxa_cambio(table_id=table_id)
        # pyrefly: ignore [bad-index]
        output_paths.append(file_info["save_output_path"])

    save_output_path = output_paths[-1]

    upload_to_gcs(
        data_path=save_output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
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
        data_path=save_output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
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
            coverage=PartBdpro(
                date_column=DateOnly(col="data_cotacao"),
                date_format=DateFormat.YEAR_MD,
                free_lag=FreeLag(unit="months", value=6),
            ),
            env="prod",
            bq_project="basedosdados",
        )


# pyrefly: ignore [missing-attribute]
br_bcb_taxa_cambio__taxa_cambio.deploy_schedules = [
    {"cron": "0 8 * * *", "timezone": "America/Sao_Paulo"}
]
