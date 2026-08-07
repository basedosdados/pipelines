"""
Flows for br_anatel_banda_larga_fixa — Prefect 3.

Contexto e decisões do fix de atualização (poll deferido): ver o README.md
deste diretório.
"""

from prefect import flow

from pipelines.crawler.anatel.banda_larga_fixa.flows import (
    _run_anatel_banda_larga_fixa,
)


def _anatel_blf_flow(table_id: str, cron: str | None):
    @flow(
        name=f"br_anatel_banda_larga_fixa__{table_id}",
        log_prints=True,
        description=f"Dump da tabela {table_id} do dataset br_anatel_banda_larga_fixa.",
    )
    def _flow(
        dataset_id: str = "br_anatel_banda_larga_fixa",
        table_id: str = table_id,
        ano: int | None = None,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_anatel_banda_larga_fixa(
            dataset_id=dataset_id,
            table_id=table_id,
            ano=ano,
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


br_anatel_banda_larga_fixa__microdados = _anatel_blf_flow(
    table_id="microdados", cron="0 15 * * *"
)
br_anatel_banda_larga_fixa__densidade_municipio = _anatel_blf_flow(
    table_id="densidade_municipio", cron=None
)
br_anatel_banda_larga_fixa__densidade_brasil = _anatel_blf_flow(
    table_id="densidade_brasil", cron=None
)
br_anatel_banda_larga_fixa__densidade_uf = _anatel_blf_flow(
    table_id="densidade_uf", cron=None
)
