"""
Flows for br_bndes_operacoes_contratadas — Prefect 3.

Wrapper @flow do crawler: expoe os parametros de run e o cron. A logica de
orquestracao (poll deferido) vive em pipelines/crawler/bndes/flows.py.
"""

from prefect import flow

from pipelines.crawler.bndes.flows import (
    _run_operacoes_contratadas_forma_indireta_automatica,
)


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_contratadas_forma_indireta_automatica",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_contratadas_forma_indireta_automatica "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_contratadas_forma_indireta_automatica(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_contratadas_forma_indireta_automatica",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes_contratadas_forma_indireta_automatica(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        dbt_alias=dbt_alias,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


br_bndes_operacoes_contratadas__operacoes_contratadas_forma_indireta_automatica.deploy_schedules = [
    {"cron": "0 6 * * 1", "timezone": "America/Sao_Paulo"}
]
