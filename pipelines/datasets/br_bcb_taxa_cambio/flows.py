"""
Flow br_bcb_taxa_cambio — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.bcb_taxa_cambio.tasks import (
    get_data_taxa_cambio,
    treat_data_taxa_cambio,
)
from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    DateOnly,
)
from pipelines.utils.metadata.tasks import (
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

    `anos` vazio baixa o ano corrente — é o que a execução agendada faz. Passar
    uma lista recarrega esses anos, para consertar partição incompleta ou
    duplicada.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    backfill = anos is not None
    # None é o ano corrente; `year_bounds` resolve isso no download.
    anos_alvo: list[int | None] = list(anos) if backfill else [None]

    # Cada ano cai numa partição própria, então os particionados coexistem no
    # mesmo diretório e o upload sai uma vez só no fim.
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
            coverage=AllBdpro(
                date_column=DateOnly(col="data_cotacao"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


# pyrefly: ignore [missing-attribute]
br_bcb_taxa_cambio__taxa_cambio.deploy_schedules = [
    {"cron": "0 8 * * *", "timezone": "America/Sao_Paulo"}
]
