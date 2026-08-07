"""
Flows para br_ibge_ipca15 — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.ibge_inflacao.flows import _run_ibge_inflacao


def _ipca15_flow(table_id: str, cron: str):
    @flow(
        name=f"br_ibge_ipca15__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_ibge_ipca15",
        table_id: str = table_id,
        periodo: str | None = None,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_ibge_inflacao(
            dataset_id=dataset_id,
            table_id=table_id,
            periodo=periodo,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_ibge_ipca15__mes_brasil = _ipca15_flow(
    "mes_brasil", "15 13 23,24,25,26,27 * *"
)
br_ibge_ipca15__mes_categoria_brasil = _ipca15_flow(
    "mes_categoria_brasil", "30 13 23,24,25,26,27 * *"
)
br_ibge_ipca15__mes_categoria_rm = _ipca15_flow(
    "mes_categoria_rm", "20 13 23,24,25,26,27 * *"
)
br_ibge_ipca15__mes_categoria_municipio = _ipca15_flow(
    "mes_categoria_municipio", "10 13 23,24,25,26,27 * *"
)
