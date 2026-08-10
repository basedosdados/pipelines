"""
Flows for br_tse_eleicoes — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.tse_eleicoes.flows import _run_tse_eleicoes


def _tse_flow(table_id: str, cron: str | None):
    @flow(
        name=f"br_tse_eleicoes__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_tse_eleicoes",
        table_id: str = table_id,
        materialize_after_dump: bool = False,
        dbt_alias: bool = False,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_tse_eleicoes(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = (
        [{"cron": cron, "timezone": "America/Sao_Paulo"}] if cron else []
    )
    return _flow


# Schedules eram comentados no Prefect 0 — mantém sem cron por enquanto.
br_tse_eleicoes__candidatos = _tse_flow("candidatos", None)
br_tse_eleicoes__bens_candidato = _tse_flow("bens_candidato", None)
br_tse_eleicoes__despesas_candidato = _tse_flow("despesas_candidato", None)
br_tse_eleicoes__receitas_candidato = _tse_flow("receitas_candidato", None)
