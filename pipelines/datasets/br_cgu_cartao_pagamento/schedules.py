# -*- coding: utf-8 -*-
from datetime import datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock, IntervalClock
from pipelines.constants import constants
from pipelines.utils.crawler_cgu.constants import constants as constants_cgu

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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
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
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
                "historical_data": False,
                "update_metadata": True,
            },
        ),
    ],
)