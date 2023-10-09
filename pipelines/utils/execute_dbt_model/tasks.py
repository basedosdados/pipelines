# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""
# pylint: disable=unused-argument

from datetime import timedelta
import json

from dbt_client import DbtClient
from prefect import task

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.utils import get_dbt_client
from pipelines.utils.utils import log


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_k8s_dbt_client(
    mode: str = "dev",
    wait=None,
) -> DbtClient:
    """
    Get a DBT client for the Kubernetes cluster.
    """
    if mode not in ["dev", "prod"]:
        raise ValueError(f"Invalid mode: {mode}")
    return get_dbt_client(
        host=f"dbt-rpc-{mode}",
    )


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def run_dbt_model(
    dbt_client: DbtClient,
    dataset_id: str,
    table_id: str,
    dbt_alias: bool,
    dbt_command: str,
    sync: bool = True,
    vars: dict = None
):
    """
    Run a DBT model.
    """
    if dbt_command not in ["run", "test", "run and test", "run/test"]:
        raise ValueError(f"Invalid dbt_command: {dbt_command}")

    if dbt_alias:
        table_id = f"{dataset_id}__{table_id}"
    
    vars_command = ''

    if vars:
        vars_command = f" --vars '{json.dumps(vars)}'"
    

    if "run" in dbt_command:
        logs_dict = dbt_client.cli(
            f"run --models {dataset_id}.{table_id}{vars_command}",
            sync=sync,
            logs=True,
        )
        for event in logs_dict["result"]["logs"]:
            if event["levelname"] in ("INFO","WARN"):
                log(event["message"])
            if event["levelname"] == "DEBUG":
                if "On model" in event["message"]:
                    log(event["message"])

    if "test" in dbt_command:
        logs_dict = dbt_client.cli(
            f"test --models {dataset_id}.{table_id}",
            sync=sync,
            logs=True,
        )
        for event in logs_dict["result"]["logs"]:
            if event["levelname"] in ("INFO","WARN"):
                log(event["message"])
            if event["levelname"] == "DEBUG":
                if "On model" in event["message"]:
                    log(event["message"])
