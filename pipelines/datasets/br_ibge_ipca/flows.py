"""
Flows para br_ibge_ipca — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.ibge_inflacao.flows import _run_ibge_inflacao


def _ipca_flow(table_id: str, cron: str):
    @flow(name=f"br_ibge_ipca__{table_id}", log_prints=True)
    def _flow(
        dataset_id: str = "br_ibge_ipca",
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

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_ibge_ipca__mes_brasil = _ipca_flow(
    table_id="mes_brasil",
    cron="40 14 8,9,10,11,12,13 * *",
)

br_ibge_ipca__mes_categoria_brasil = _ipca_flow(
    table_id="mes_categoria_brasil",
    cron="30 14 8,9,10,11,12,13 * *",
)

br_ibge_ipca__mes_categoria_rm = _ipca_flow(
    table_id="mes_categoria_rm",
    cron="20 14 8,9,10,11,12,13 * *",
)

br_ibge_ipca__mes_categoria_municipio = _ipca_flow(
    table_id="mes_categoria_municipio",
    cron="50 14 8,9,10,11,12,13 * *",
)
