# -*- coding: utf-8 -*-
"""
General utilities for interacting with dbt-rpc
"""

import json
from datetime import datetime, timedelta
from typing import List

from dbt_client import DbtClient
from prefect.schedules.clocks import IntervalClock

from pipelines.utils.utils import log


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


def merge_vars(vars1, vars2):
    try:
        dict1 = json.loads(vars1)
        dict2 = json.dumps(vars2, ensure_ascii=False)
        dict2 = json.loads(dict2)
        merged = {**dict1, **dict2}
        return json.dumps(merged)
    except (json.JSONDecodeError, TypeError) as e:
        log(f"Erro ao mesclar variáveis: {e}")
        raise
