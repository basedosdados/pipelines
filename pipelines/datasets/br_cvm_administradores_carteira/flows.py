"""
Flows for br_cvm_administradores_carteira — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.cvm_administradores_carteira.flows import (
    _run_cvm_administradores_carteira,
)


def _adm_cart_flow(table_id: str, cron: str):
    @flow(
        name=f"br_cvm_administradores_carteira__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_cvm_administradores_carteira",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_cvm_administradores_carteira(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_cvm_administradores_carteira__responsavel = _adm_cart_flow(
    table_id="responsavel", cron="12 6 * * 1-5"
)
br_cvm_administradores_carteira__pessoa_fisica = _adm_cart_flow(
    table_id="pessoa_fisica", cron="50 6 * * 1-5"
)
br_cvm_administradores_carteira__pessoa_juridica = _adm_cart_flow(
    table_id="pessoa_juridica", cron="0 6 * * 1-5"
)
