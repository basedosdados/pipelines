"""
Flows para br_camara_dados_abertos — Prefect 3.
"""

from prefect import flow

from pipelines.crawler.camara_dados_abertos.flows import (
    _run_camara_dados_abertos,
)


def _camara_flow(table_id: str, cron: str):
    @flow(
        name=f"br_camara_dados_abertos__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_camara_dados_abertos",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
    ) -> None:
        _run_camara_dados_abertos(
            dataset_id=dataset_id,
            table_id=table_id,
            materialize_after_dump=materialize_after_dump,
            dbt_alias=dbt_alias,
            update_metadata=update_metadata,
            target=target,
            force_run=force_run,
        )

    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


# Votação
br_camara_dados_abertos__votacao = _camara_flow("votacao", "0 6 * * *")
br_camara_dados_abertos__votacao_objeto = _camara_flow(
    "votacao_objeto", "10 6 * * *"
)
br_camara_dados_abertos__votacao_orientacao_bancada = _camara_flow(
    "votacao_orientacao_bancada", "20 6 * * *"
)
br_camara_dados_abertos__votacao_parlamentar = _camara_flow(
    "votacao_parlamentar", "30 6 * * *"
)
br_camara_dados_abertos__votacao_proposicao = _camara_flow(
    "votacao_proposicao", "40 6 * * *"
)

# Deputado
br_camara_dados_abertos__deputado = _camara_flow("deputado", "50 6 * * *")
br_camara_dados_abertos__deputado_ocupacao = _camara_flow(
    "deputado_ocupacao", "0 7 * * *"
)
br_camara_dados_abertos__deputado_profissao = _camara_flow(
    "deputado_profissao", "10 7 * * *"
)

# Proposição
br_camara_dados_abertos__proposicao_microdados = _camara_flow(
    "proposicao_microdados", "20 7 * * *"
)
br_camara_dados_abertos__proposicao_autor = _camara_flow(
    "proposicao_autor", "30 7 * * *"
)
br_camara_dados_abertos__proposicao_tema = _camara_flow(
    "proposicao_tema", "40 7 * * *"
)

# Órgão
br_camara_dados_abertos__orgao = _camara_flow("orgao", "50 7 * * *")
br_camara_dados_abertos__orgao_deputado = _camara_flow(
    "orgao_deputado", "0 8 * * *"
)

# Evento
br_camara_dados_abertos__evento = _camara_flow("evento", "10 8 * * *")
br_camara_dados_abertos__evento_orgao = _camara_flow(
    "evento_orgao", "20 8 * * *"
)
br_camara_dados_abertos__evento_presenca_deputado = _camara_flow(
    "evento_presenca_deputado", "30 8 * * *"
)
br_camara_dados_abertos__evento_requerimento = _camara_flow(
    "evento_requerimento", "40 8 * * *"
)

# Funcionário
br_camara_dados_abertos__funcionario = _camara_flow(
    "funcionario", "50 8 * * *"
)

# Frente
br_camara_dados_abertos__frente = _camara_flow("frente", "0 9 * * *")
br_camara_dados_abertos__frente_deputado = _camara_flow(
    "frente_deputado", "10 9 * * *"
)

# Licitação
br_camara_dados_abertos__licitacao = _camara_flow("licitacao", "15 9 * * *")
br_camara_dados_abertos__licitacao_proposta = _camara_flow(
    "licitacao_proposta", "20 9 * * *"
)
br_camara_dados_abertos__licitacao_contrato = _camara_flow(
    "licitacao_contrato", "30 9 * * *"
)
br_camara_dados_abertos__licitacao_item = _camara_flow(
    "licitacao_item", "40 9 * * *"
)
br_camara_dados_abertos__licitacao_pedido = _camara_flow(
    "licitacao_pedido", "50 9 * * *"
)

# Despesa
br_camara_dados_abertos__despesa = _camara_flow("despesa", "0 10 * * *")
