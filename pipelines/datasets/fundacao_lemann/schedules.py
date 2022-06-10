# -*- coding: utf-8 -*-
"""
Schedules for fundacao_lemann
"""

from datetime import timedelta, datetime
from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

every_year = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(weeks=50),
            start_date=datetime(2021, 1, 1, 17, 35),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "fundacao_lemann",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "ano_escola_serie_educacao_aprendizagem_adequada",
            },
        ),
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
