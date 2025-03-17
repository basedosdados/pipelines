# -*- coding: utf-8 -*-
"""
General utilities for interacting with dbt-rpc
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

from dbt.cli.main import dbtRunnerResult
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


def process_dbt_logs(result: dbtRunnerResult) -> None:
    """
    Process and log dbt execution results.

    Args:
        result (dbtRunnerResult): The result of a dbt command execution.
    """

    if hasattr(result, "result") and hasattr(result.result, "logs"):
        for event in result.result.logs:
            if event.get("levelname") in ("INFO", "WARN"):
                log(event.get("message", ""))
            if event.get("levelname") == "DEBUG" and "On model" in event.get(
                "message", ""
            ):
                log(event.get("message", ""))

    if hasattr(result, "result") and hasattr(result.result, "results"):
        for node_result in result.result.results:
            if hasattr(node_result, "status") and node_result.status in [
                "fail",
                "error",
            ]:
                # Extract detailed error information
                error_details = getattr(
                    node_result, "message", "No details available"
                )
                log(
                    f"Error in model {node_result.node.name}: {error_details}",
                    level="error",
                )


def create_dbt_report(running_result: dbtRunnerResult) -> Dict[str, Any]:
    """
    Create a report of the dbt execution.

    Args:
        running_result (dbtRunnerResult): The result of a dbt command execution.

    Returns:
        Dict[str, Any]: Report data
    """
    report_data = {
        "success": running_result.success,
        "errors": [],
        "warnings": [],
    }

    if hasattr(running_result, "result"):
        for command_result in getattr(running_result.result, "results", []):
            if command_result.status == "fail":
                msg = f"FAIL: {command_result.node.name}"
                report_data["errors"].append(msg)
                log(msg, level="error")
            elif command_result.status == "error":
                msg = f"ERROR: {command_result.node.name}"
                report_data["errors"].append(msg)
                log(msg, level="error")
            elif command_result.status == "warn":
                msg = f"WARN: {command_result.node.name}"
                report_data["warnings"].append(msg)
                log(msg, level="warning")

    return report_data
