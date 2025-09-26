# -*- coding: utf-8 -*-
"""
Schedules for br_me_caged
"""

from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

every_month_movimentacao = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)

every_month_movimentacao_fora_prazo = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)

every_month_movimentacao_excluida = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
        )
    ]
)
