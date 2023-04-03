# -*- coding: utf-8 -*-
"""
Schedules for bd_tweet_data
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

every_day_organizations = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 00),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "organizations",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)

every_day_datasets = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 5),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "datasets",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)

every_day_resources = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 10),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "resources",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)

every_day_external_links = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 15),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "external_links",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)

every_day_information_requests = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 20),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "information_requests",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)

every_day_tables = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 25),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "tables",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)

every_day_columns = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2022, 9, 20, 10, 30),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_bd_metadados",
                "table_id": "columns",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": False,
            },
        ),
    ],
)
