# -*- coding: utf-8 -*-
"""
Schedules for br_fgv_igp
"""

from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

igp_di_mes = Schedule(
    clocks=[
        CronClock(
            cron="00 03 08 * *",  # At 03:00 on day-of-month 8
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_di_mes",
                "indice": "IGPDI",
                "periodo": "mes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)

igp_di_ano = Schedule(
    clocks=[
        CronClock(
            cron="00 03 08 01 *",  # At 03:00 on day-of-month 8 in January
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_di_ano",
                "indice": "IGPDI",
                "periodo": "ano",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)

igp_m_mes = Schedule(
    clocks=[
        CronClock(
            cron="00 03 01 * *",  # At 03:00 on day-of-month 1
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_m_mes",
                "indice": "IGPM",
                "periodo": "mes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)

igp_m_ano = Schedule(
    clocks=[
        CronClock(
            cron="00 03 01 01 *",  # At 03:00 on day-of-month 1 in January
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_m_ano",
                "indice": "IGPM",
                "periodo": "ano",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)

igp_og_mes = Schedule(
    clocks=[
        CronClock(
            cron="00 03 08 * *",  # At 03:00 on day-of-month 8
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_og_mes",
                "indice": "IGPOG",
                "periodo": "mes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)

igp_og_ano = Schedule(
    clocks=[
        CronClock(
            cron="00 03 08 01 *",  # At 03:00 on day-of-month 8 in January
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_og_ano",
                "indice": "IGPOG",
                "periodo": "ano",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)

igp_10_mes = Schedule(
    clocks=[
        CronClock(
            cron="00 03 18 * *",  # At 03:00 on day-of-month 18
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_fgv_igp",
                "table_id": "igp_10_mes",
                "indice": "IGP10",
                "periodo": "mes",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ]
)
