"""Flows for br_anatel_telefonia_movel — Prefect 3."""

from prefect import flow

from pipelines.crawler.anatel.telefonia_movel.flows import (
    _run_anatel_telefonia_movel,
)


def _anatel_tm_flow(table_id: str, cron: str):
    @flow(
        name=f"br_anatel_telefonia_movel__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_anatel_telefonia_movel",
        table_id: str = table_id,
        ano: int | None = None,
        semestre: int | None = None,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_anatel_telefonia_movel(
            dataset_id=dataset_id,
            table_id=table_id,
            ano=ano,
            semestre=semestre,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_anatel_telefonia_movel__microdados = _anatel_tm_flow(
    table_id="microdados", cron="30 16 * * *"
)
br_anatel_telefonia_movel__densidade_municipio = _anatel_tm_flow(
    table_id="densidade_municipio", cron="30 17 * * *"
)
br_anatel_telefonia_movel__densidade_uf = _anatel_tm_flow(
    table_id="densidade_uf", cron="30 18 * * *"
)
br_anatel_telefonia_movel__densidade_brasil = _anatel_tm_flow(
    table_id="densidade_brasil", cron="30 19 * * *"
)
