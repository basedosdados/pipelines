# -*- coding: utf-8 -*-
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

every_day_microdados_governo_federal = Schedule(
    clocks=[
        CronClock(
            cron="0 20 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_cartao_pagamento",
                "table_id": "microdados_governo_federal",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_microdados_defesa_civil = Schedule(
    clocks=[
        CronClock(
            cron="30 20 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_cartao_pagamento",
                "table_id": "microdados_defesa_civil",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)

every_day_microdados_compras_centralizadas = Schedule(
    clocks=[
        CronClock(
            cron="00 21 * * *",
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_cgu_cartao_pagamento",
                "table_id": "microdados_compras_centralizadas",
                "target": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)
