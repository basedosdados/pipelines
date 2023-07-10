# -*- coding: utf-8 -*-
"""
Schedules for br_ms_cnes
"""

from prefect.schedules import Schedule, filters, adjustments
from prefect.schedules.clocks import CronClock
from datetime import datetime
from pipelines.constants import constants


schedule_br_ms_cnes_estabelecimento = Schedule(
    clocks=[
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 7, 24, 0, 0),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "estabelecimento",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)


schedule_br_ms_cnes_profissional = Schedule(
    clocks=[
        CronClock(
            cron="@monthly",
            start_date=datetime(2023, 7, 24, 0, 0),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ms_cnes",
                "table_id": "profissionais",
                "materialization_mode": "prod",
                "materialize after dump": True,
                "dbt_alias": False,
            },
        )
    ],
    filters=[filters.is_weekday],
    adjustments=[adjustments.next_weekday],
)
