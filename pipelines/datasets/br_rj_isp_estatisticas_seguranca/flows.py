"""
Flows for br_rj_isp_estatisticas_seguranca — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.isp.flows import _run_isp


def _isp_flow(table_id: str, cron: str):
    @flow(
        name=f"br_rj_isp_estatisticas_seguranca__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_rj_isp_estatisticas_seguranca",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_isp(
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


br_rj_isp_estatisticas_seguranca__evolucao_mensal_cisp = _isp_flow(
    "evolucao_mensal_cisp", "5 10 * * *"
)
br_rj_isp_estatisticas_seguranca__evolucao_policial_morto_servico_mensal = (
    _isp_flow("evolucao_policial_morto_servico_mensal", "10 10 * * *")
)
br_rj_isp_estatisticas_seguranca__armas_apreendidas_mensal = _isp_flow(
    "armas_apreendidas_mensal", "15 10 * * *"
)
br_rj_isp_estatisticas_seguranca__evolucao_mensal_municipio = _isp_flow(
    "evolucao_mensal_municipio", "20 10 * * 5"
)
br_rj_isp_estatisticas_seguranca__evolucao_mensal_uf = _isp_flow(
    "evolucao_mensal_uf", "25 10 * * *"
)
br_rj_isp_estatisticas_seguranca__feminicidio_mensal_cisp = _isp_flow(
    "feminicidio_mensal_cisp", "40 10 * * *"
)
