"""
Flows for br_bndes_operacoes_contratadas — Prefect 3.

Wrapper @flow do crawler: expoe os parametros de run e o cron. A logica de
orquestracao (poll deferido) vive em pipelines/crawler/bndes/flows.py.
"""

from prefect import flow

from pipelines.crawler.bndes.flows import (
    _run_operacoes,
    _run_operacoes_administracao_publica,
    _run_operacoes_exportacao_bens,
    _run_operacoes_exportacao_servicos,
)


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_indiretas_automaticas "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_indiretas_automaticas",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# pyrefly: ignore [missing-attribute]
br_bndes_operacoes_contratadas__operacoes_indiretas_automaticas.deploy_schedules = [
    {"cron": "0 6 * * 1", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_nao_automaticas",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_nao_automaticas "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_nao_automaticas(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_nao_automaticas",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# pyrefly: ignore [missing-attribute]
br_bndes_operacoes_contratadas__operacoes_nao_automaticas.deploy_schedules = [
    {"cron": "0 6 * * 1", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_administracao_publica",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_administracao_publica "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_administracao_publica(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_administracao_publica",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes_administracao_publica(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# cron semanal (segunda 06h BRT), igual a outra tabela; a fonte atualiza mensal e o poll
# deferido no-opa quando nao ha novidade. Ajuste se quiser outra janela.
# pyrefly: ignore [missing-attribute]
br_bndes_operacoes_contratadas__operacoes_administracao_publica.deploy_schedules = [
    {"cron": "0 6 * * 1", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_exportacao_bens",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_exportacao_bens "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_exportacao_bens(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_exportacao_bens",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    _run_operacoes_exportacao_bens(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        force_run=force_run,
    )


# pyrefly: ignore [missing-attribute]
br_bndes_operacoes_contratadas__operacoes_exportacao_bens.deploy_schedules = [
    {"cron": "0 6 * * 1", "timezone": "America/Sao_Paulo"}
]


@flow(
    name="br_bndes_operacoes_contratadas__operacoes_exportacao_servicos",
    log_prints=True,
    description=(
        "Dump da tabela operacoes_exportacao_servicos "
        "do dataset br_bndes_operacoes_contratadas."
    ),
)
def br_bndes_operacoes_contratadas__operacoes_exportacao_servicos(
    dataset_id: str = "br_bndes_operacoes_contratadas",
    table_id: str = "operacoes_exportacao_servicos",
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    _run_operacoes_exportacao_servicos(
        dataset_id=dataset_id,
        table_id=table_id,
        materialize_after_dump=materialize_after_dump,
        update_metadata=update_metadata,
        target=target,
        force_run=force_run,
    )


# Sem cron, ao contrário das irmãs: a série da fonte termina em 2015-04-28 e
# não recebe operação nova há dez anos. A tabela entra como carga única, por
# disparo manual. Se a fonte voltar a publicar, basta acrescentar
# deploy_schedules aqui — o resto do flow já está pronto para o poll.
