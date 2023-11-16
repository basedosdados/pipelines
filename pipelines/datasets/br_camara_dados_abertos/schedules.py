# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_camara_dados_abertos = Schedule(
    clocks=[
        CronClock(
            cron="0 9 * * *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_DEV_AGENT_LABEL.value],
            parameter_defaults={
                "update_metadata": True,
                "dbt_alias": True,
                "materialize_after_dump": True,
                "materialization_mode": "prod",
                "table_id": [
                    "votacao_microdados",
                    "votacao_objeto",
                    "votacao_orientacao_bancada",
                    "votacao_parlamentar",
                    "votacao_proposicao_afetada",
                ],
                "dataset_id": "br_camara_dados_abertos",
            },
        ),
    ],
)
