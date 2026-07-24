"""
Flows for br_rf_cnpj — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.rf_cnpj.flows import _run_rf_cnpj


def _rf_cnpj_flow(table_id: str, cron: str):
    @flow(
        name=f"br_rf_cnpj__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_rf_cnpj",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        chunk_size: int = 100000,
        folder_date: str | None = None,
    ) -> None:
        _run_rf_cnpj(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
            chunk_size=chunk_size,
            folder_date=folder_date,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_rf_cnpj__dicionario = _rf_cnpj_flow(table_id="dicionario", cron="0 5 * * *")
br_rf_cnpj__empresas = _rf_cnpj_flow(table_id="empresas", cron="0 6 * * *")
br_rf_cnpj__socios = _rf_cnpj_flow(table_id="socios", cron="0 7 * * *")
br_rf_cnpj__simples = _rf_cnpj_flow(table_id="simples", cron="0 8 * * *")
br_rf_cnpj__estabelecimentos = _rf_cnpj_flow(
    table_id="estabelecimentos", cron="0 9 * * *"
)
