# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_camara_dados_abertos = Schedule(
    clocks=[
        CronClock(
            cron="0 9 * * *",  # every day at 9:00 UTC
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": [
                    "votacao_microdados",
                    "votacao_objeto",
                    "votacao_orientacao_bancada",
                    "voto_parlamentar",
                    "votacao_proposicao_afetada",
                ],
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)


every_day_camara_dados_abertos_deputados = Schedule(
    clocks=[
        CronClock(
            cron="30 9 * * *",  # every day at 9:00 UTC
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": [
                    "deputado",
                    "deputado_ocupacao",
                    "deputado_profissao",
                ],
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)

every_day_camara_dados_abertos_universal = Schedule(
    clocks=[
        CronClock(
            cron="0 10 * * *",  # every day at 10:00 UTC
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_PROD_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": [
                    "proposicao_microdados",
                    "proposicao_autor",
                    "proposicao_tema",
                    "orgao",
                    "orgao_deputado",
                ],
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)
