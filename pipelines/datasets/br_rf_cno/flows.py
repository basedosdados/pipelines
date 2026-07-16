"""
Flows for br_rf_cno (Cadastro Nacional de Obras — Receita Federal) — Prefect 3.

Um flow por tabela (microdados, vinculos, areas, cnaes), gerado por `_cno_flow` e
delegando a `_run_rf` (crawler compartilhado `rf`). Contexto da fonte, do WAF e das
particularidades por tabela: ver `pipelines/datasets/br_rf_cno/README.md`.
"""

from prefect import flow

from pipelines.crawler.rf.flows import _run_rf


def _cno_flow(table_id: str, cron: str):
    @flow(
        name=f"br_rf_cno__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_rf_cno",
        table_id: str = table_id,
        chunksize: int = 100000,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_rf(
            dataset_id=dataset_id,
            table_id=table_id,
            chunksize=chunksize,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_rf_cno__microdados = _cno_flow("microdados", "5 4 * * 1-5")
br_rf_cno__vinculos = _cno_flow("vinculos", "15 4 * * 1-5")
br_rf_cno__areas = _cno_flow("areas", "25 4 * * 1-5")
br_rf_cno__cnaes = _cno_flow("cnaes", "35 4 * * 1-5")
