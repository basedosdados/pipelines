# -*- coding: utf-8 -*-
"""
Schedules for br_ans_beneficiario
"""


from datetime import datetime
from prefect.schedules import Schedule
from pipelines.constants import constants
from prefect.schedules.clocks import CronClock
from pipelines.datasets.br_cvm_fi.constants import constants as cvm_constants


every_day_ans = Schedule(
    clocks=[
        CronClock(
            cron="0 21 * * *",  # At 23:00 on every day-of-week from Monday through Friday.
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_ans_beneficiario",
                "table_id": "informacao_consolidada",
                "materialization_mode": "prod",
                "materialize_after_dump": True,
                "dbt_alias": False,
                "update_metadata": True,
            },
        ),
    ],
)
