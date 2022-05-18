# -*- coding: utf-8 -*-
"""
General utilities for interacting with dbt-rpc
"""
from datetime import timedelta, datetime
from typing import List

from dbt_client import DbtClient
from prefect.schedules.clocks import IntervalClock


def get_dbt_client(
    host: str = "dbt-rpc",
    port: int = 8580,
    jsonrpc_version: str = "2.0",
) -> DbtClient:
    """
    Returns a DBT RPC client.

    Args:
        host: The hostname of the DBT RPC server.
        port: The port of the DBT RPC server.
        jsonrpc_version: The JSON-RPC version to use.

    Returns:
        A DBT RPC client.
    """
    return DbtClient(
        host=host,
        port=port,
        jsonrpc_version=jsonrpc_version,
    )


def generate_execute_dbt_model_schedules(  # pylint: disable=too-many-arguments,too-many-locals
    interval: timedelta,
    start_date: datetime,
    labels: List[str],
    table_parameters: dict,
    runs_interval_minutes: int = 15,
) -> List[IntervalClock]:
    """
    Generates multiple schedules for execute dbt model.
    """
    clocks = []
    for count, (table_id, parameters) in enumerate(table_parameters.items()):
        parameter_defaults = {
            "dataset_id": parameters["dataset_id"],
            "table_id": table_id,
            "mode": parameters["mode"],
        }
        clocks.append(
            IntervalClock(
                interval=interval,
                start_date=start_date
                + timedelta(minutes=runs_interval_minutes * count),
                labels=labels,
                parameter_defaults=parameter_defaults,
            )
        )
    return clocks
