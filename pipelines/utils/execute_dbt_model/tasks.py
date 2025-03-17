# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""

import json
import os
import shutil
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

import git
from dbt.cli.main import dbtRunner
from dbt_client import DbtClient
from prefect import task
from prefect.engine.signals import FAIL

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import (
    constants as constants_execute,
)
from pipelines.utils.execute_dbt_model.utils import (
    create_dbt_report,
    get_dbt_client,
    merge_vars,
    process_dbt_logs,
)
from pipelines.utils.utils import log


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_repository() -> str:
    """
    Downloads the repository specified by the REPOSITORY_URL constant.

    This function creates a repository folder, clones the repository from the specified URL,
    and logs the success or failure of the download.

    Returns:
        str: Path to the downloaded repository.

    Raises:
        FAIL: If there is an error when creating the repository folder or downloading the repository.
    """
    repo_url = constants_execute.REPOSITORY_URL.value

    try:
        repository_path = os.path.join(os.getcwd(), "dbt_repository")

        if os.path.exists(repository_path):
            log(
                f"Repository folder already exists. Removing: {repository_path}"
            )
            shutil.rmtree(repository_path, ignore_errors=False)
        os.makedirs(repository_path)

        log(f"Repository folder created: {repository_path}")

    except Exception as e:
        raise FAIL(str(f"Error when creating repository folder: {e}")) from e

    try:
        repo = git.Repo.clone_from(repo_url, repository_path)
        log(f"Repository downloaded: {repo_url}")

        if not os.path.exists(
            os.path.join(repository_path, "dbt_project.yml")
        ):
            raise FAIL(
                "Repository downloaded but dbt_project.yml not found. Check repository structure."
            )

        commit_hash = repo.head.commit.hexsha
        log(f"Repository cloned at commit: {commit_hash}")

    except git.GitCommandError as e:
        raise FAIL(str(f"Error when downloading repository: {e}")) from e

    log(f"Repository contents: {os.listdir(repository_path)}")
    return repository_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def install_dbt_dependencies(dbt_repository_path: str) -> None:
    """
    Install dbt dependencies using 'dbt deps'.

    Args:
        dbt_repository_path (str): Path to the dbt repository.

    Returns:
        None

    Raises:
        FAIL: If there is an error when installing dependencies.
    """
    try:
        os.chdir(dbt_repository_path)
        log("Installing dbt dependencies...", level="info")

        dbt_runner = dbtRunner()
        result = dbt_runner.invoke(["deps"])

        if not result.success:
            raise FAIL("Failed to install dbt dependencies")

        log("Successfully installed dbt dependencies", level="info")
        return result
    except Exception as e:
        raise FAIL(str(f"Error installing dbt dependencies: {e}")) from e


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def new_execute_dbt_model(
    dbt_repository_path: str,
    dataset_id: str,
    table_id: Optional[str] = None,
    dbt_alias: bool = True,
    dbt_command: str = "run",
    target: str = "dev",
    sync: bool = True,
    flags: Optional[str] = None,
    _vars: Optional[Union[dict, List[Dict], str]] = None,
    disable_elementary: bool = False,
    generate_report: bool = True,
) -> Dict[str, Any]:
    """
    Run a DBT model using the dbt CLI and process the results.

    Args:
        dbt_repository_path (str): Path to the dbt repository.
        dataset_id (str): Dataset ID of the dbt model.
        table_id (str, optional): Table ID of the dbt model. If None, the
            whole dataset will be run.
        dbt_alias (bool, optional): If True, the model will be run by
            its alias. Defaults to True.
        dbt_command (str, optional): Command to run in dbt. Defaults to "run".
        target (str, optional): Target to run in dbt. It specifies in which
            Big Query project the model will run. Defaults to "dev".
        sync (bool, optional): If True, the task will be synchronous.
        flags (str, optional): Flags to pass to the dbt run command.
            See: https://docs.getdbt.com/reference/dbt-jinja-functions/flags/
        _vars (Union[dict, List[Dict], str], optional): Variables to pass to
            dbt. Defaults to None.
        disable_elementary (bool, optional): Disable elementary on-run-end hooks.
        generate_report (bool, optional): Whether to generate a detailed report.
            Defaults to True.

    Returns:
        Dict[str, Any]: Execution report and results

    Raises:
        ValueError: If dbt_command is not valid.
        FAIL: If there is an error during execution.
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
        if _vars is None:
            _vars = constants_execute.DISABLE_ELEMENTARY_VARS.value
        else:
            _vars = merge_vars(
                constants_execute.DISABLE_ELEMENTARY_VARS.value, _vars
            )

    os.chdir(dbt_repository_path)

    all_results = []
    reports = []

    commands_to_run = []
    if "run" in dbt_command:
        commands_to_run.append("run")
    if "test" in dbt_command:
        commands_to_run.append("test")

    for cmd in commands_to_run:
        # Build CLI arguments
        cli_args = [cmd, "--select", selected_table, "--target", target]

        # Handle full-refresh flag specially
        if flags and flags.startswith("--full-refresh") and cmd == "run":
            cli_args.insert(1, "--full-refresh")
        elif flags:
            cli_args.extend(flags.split())

        # Add variables if provided
        if _vars:
            if isinstance(_vars, list):
                vars_dict = {}
                for elem in _vars:
                    vars_dict.update(elem)
                vars_str = json.dumps(vars_dict)
            else:
                if isinstance(_vars, str):
                    try:
                        json.loads(_vars.replace("'", '"'))
                        vars_str = _vars.replace("'", '"')
                    except json.JSONDecodeError:
                        vars_str = json.dumps(eval(_vars))
                else:
                    vars_str = json.dumps(_vars)

            cli_args.extend(["--vars", vars_str])

        # Execute command
        log(f"Executing dbt command: {' '.join(cli_args)}", level="info")
        dbt_runner = dbtRunner()

        try:
            running_result = dbt_runner.invoke(cli_args)
            all_results.append(running_result)

            process_dbt_logs(running_result)

            if generate_report:
                report = create_dbt_report(running_result)
                reports.append(report)

                if report["success"]:
                    log(f"DBT {cmd} completed successfully.", level="info")
                else:
                    log(f"DBT {cmd} completed with errors.", level="warning")
                    error_details = "; ".join(report["errors"])
                    log(f"Errors: {error_details}", level="error")

            if not running_result.success:
                raise FAIL(
                    f"DBT {cmd} command failed. Check logs for details."
                )

        except Exception as e:
            log(f"Error executing dbt {cmd}: {str(e)}", level="error")
            raise FAIL(f"Error executing dbt {cmd}: {str(e)}") from e

    final_report = {
        "success": all(result.success for result in all_results),
        "command_results": all_results,
        "reports": reports,
    }

    return final_report


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
    max_retries=constants.RUN_DBT_MODEL_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
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
    disable_elementary: bool = False,
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
        if _vars is None:
            _vars = constants_execute.DISABLE_ELEMENTARY_VARS.value
        else:
            _vars = merge_vars(
                constants_execute.DISABLE_ELEMENTARY_VARS.value, _vars
            )

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
        if _vars:
            vars_str = f"'{_vars}'"
            test_command = f"test --select {selected_table} --vars {vars_str}"
        else:
            test_command = f"test --select {selected_table}"

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
