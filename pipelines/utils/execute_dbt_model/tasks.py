# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""

from cProfile import run
from datetime import timedelta
from genericpath import exists

from dbt_client import DbtClient
from prefect import task

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.utils import get_dbt_client, merge_vars
from pipelines.utils.utils import log
from pipelines.utils.execute_dbt_model.constants import constants as constants_execute


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
    max_retries=0,
)
def run_dbt_model(
    dbt_client: DbtClient,
    dataset_id: str,
    table_id: str,
    dbt_alias: bool = True,
    dbt_command: str = "run",
    sync: bool = True,
    flags: str = None,
    _vars=None,
    disable_elementary:bool=False,
):
    """
    Run a DBT model.
    Args:
        dataset_id (str): Dataset ID of the dbt model.
        table_id (str, optional): Table ID of the dbt model. If None, the
        whole dataset will be run.
        dbt_alias (bool, optional): If True, the model will be run by
        its alias. Defaults to False.
        flags (str, optional): Flags to pass to the dbt run command.
        See:
        https://docs.getdbt.com/reference/dbt-jinja-functions/flags/
        _vars (Union[dict, List[Dict]], optional): Variables to pass to
        dbt. Defaults to None.
        disable_elementary (bool, optional): Disable elementary on-run-end hooks.
    """
    if dbt_command not in ["run", "test", "run and test", "run/test"]:
        raise ValueError(f"Invalid dbt_command: {dbt_command}")

    if table_id:
        if dbt_alias:
            selected_table = f"{dataset_id}.{dataset_id}__{table_id}"
        else:
            selected_table = f"{dataset_id}.{table_id}"
    else:
        selected_table = dataset_id

    if disable_elementary:
        _vars = constants_execute.DISABLE_ELEMENTARY_VARS.value if _vars is None else merge_vars(constants_execute.DISABLE_ELEMENTARY_VARS.value, _vars)

    if "run" in dbt_command:
        if flags == "--full-refresh":
            run_command = f"dbt run --full-refresh --select {selected_table}"
            flags = None
        else:
            run_command = f"dbt run --select {selected_table}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f"'{_vars}'"
            run_command = None if dbt_command in ["test"] else run_command
            if run_command is not None:
                run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    if "run" in dbt_command:
        log(f"Running dbt with command: {run_command}")
        logs_dict = dbt_client.cli(
            run_command,
            sync=sync,
            logs=True,
        )
        for event in logs_dict["result"]["logs"]:
            if event["levelname"] in ("INFO", "WARN"):
                log(event["message"])
            if event["levelname"] == "DEBUG":
                if "On model" in event["message"]:
                    log(event["message"])

    if "test" in dbt_command:
        vars_str = f"'{_vars}'" if disable_elementary else f'"{_vars}"'
        test_command = f"test --select {selected_table} --vars {vars_str}" if disable_elementary else f"test --select {selected_table}"
        log(f"Running dbt with command: {test_command}")
        logs_dict = dbt_client.cli(
            test_command,
            sync=sync,
            logs=True,
        )
        for event in logs_dict["result"]["logs"]:
            if event["levelname"] in ("INFO", "WARN"):
                log(event["message"])
            if event["levelname"] == "DEBUG":
                if "On model" in event["message"]:
                    log(event["message"])
