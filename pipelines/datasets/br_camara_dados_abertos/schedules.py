# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

# ! - > Votação

schedules_br_camara_dados_abertos_votacao = Schedule(
    clocks=[
        CronClock(
            cron="0 6 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "votacao",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Votação Objeto

schedules_br_camara_dados_abertos_votacao_objeto = Schedule(
    clocks=[
        CronClock(
            cron="10 6 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "votacao_objeto",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Votação Orientação Bancada

schedules_br_camara_dados_abertos_votacao_orientacao_bancada = Schedule(
    clocks=[
        CronClock(
            cron="20 6 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "votacao_orientacao_bancada",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Votação parlamentar

schedules_br_camara_dados_abertos_votacao_parlamentar = Schedule(
    clocks=[
        CronClock(
            cron="30 6 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "votacao_parlamentar",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Votação Proposição

schedules_br_camara_dados_abertos_votacao_proposicao = Schedule(
    clocks=[
        CronClock(
            cron="40 6 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "votacao_proposicao",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Deputado

schedules_br_camara_dados_abertos_deputado = Schedule(
    clocks=[
        CronClock(
            cron="50 6 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "deputado",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Deputado Ocupação

schedules_br_camara_dados_abertos_deputado_ocupacao = Schedule(
    clocks=[
        CronClock(
            cron="0 7 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "deputado_ocupacao",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)
# ! - > Deputado Profissão

schedules_br_camara_dados_abertos_deputado_profissao = Schedule(
    clocks=[
        CronClock(
            cron="10 7 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id":"deputado_profissao",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Proposição microdados

schedules_br_camara_dados_abertos_proposicao_microdados = Schedule(
    clocks=[
        CronClock(
            cron="20 7 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "proposicao_microdados",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Proposição Autor

schedules_br_camara_dados_abertos_proposicao_autor = Schedule(
    clocks=[
        CronClock(
            cron="30 7 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "proposicao_autor",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Proposição Tema

schedules_br_camara_dados_abertos_proposicao_tema = Schedule(
    clocks=[
        CronClock(
            cron="40 7 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "proposicao_tema",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Órgão

schedules_br_camara_dados_abertos_orgao = Schedule(
    clocks=[
        CronClock(
            cron="50 7 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "orgao",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)


# ! - > Órgão deputado

schedules_br_camara_dados_abertos_orgao_deputado = Schedule(
    clocks=[
        CronClock(
            cron="00 8 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "orgao_deputado",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Evento

schedules_br_camara_dados_abertos_evento = Schedule(
    clocks=[
        CronClock(
            cron="10 8 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "evento",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Evento órgão

schedules_br_camara_dados_abertos_evento_orgao = Schedule(
    clocks=[
        CronClock(
            cron="20 8 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "evento_orgao",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Evento presença deputado

schedules_br_camara_dados_abertos_evento_presenca_deputado = Schedule(
    clocks=[
        CronClock(
            cron="30 8 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "evento_presenca_deputado",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Evento requerimento

schedules_br_camara_dados_abertos_evento_requerimento = Schedule(
    clocks=[
        CronClock(
            cron="40 8 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "evento_requerimento",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Funcionário

schedules_br_camara_dados_abertos_funcionario = Schedule(
    clocks=[
        CronClock(
            cron="50 8 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "funcionario",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Frente

schedules_br_camara_dados_abertos_frente = Schedule(
    clocks=[
        CronClock(
            cron="0 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "frente",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Frente Deputado

schedules_br_camara_dados_abertos_frente_deputado = Schedule(
    clocks=[
        CronClock(
            cron="10 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "frente_deputado",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > licitacao

schedules_br_camara_dados_abertos_licitacao = Schedule(
    clocks=[
        CronClock(
            cron="10 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "licitacao",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Licitação Proposta

schedules_br_camara_dados_abertos_licitacao_proposta = Schedule(
    clocks=[
        CronClock(
            cron="20 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "licitacao_proposta",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)


# ! - > Licitação Contrato

schedules_br_camara_dados_abertos_licitacao_contrato = Schedule(
    clocks=[
        CronClock(
            cron="30 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "licitacao_contrato",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Licitacao Item

schedules_br_camara_dados_abertos_licitacao_item = Schedule(
    clocks=[
        CronClock(
            cron="40 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "licitacao_item",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Licitação Pedido

schedules_br_camara_dados_abertos_licitacao_pedido = Schedule(
    clocks=[
        CronClock(
            cron="50 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "licitacao_pedido",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

# ! - > Despesa
schedules_br_camara_dados_abertos_despesa = Schedule(
    clocks=[
        CronClock(
            cron="00 10 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": "despesa",
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)