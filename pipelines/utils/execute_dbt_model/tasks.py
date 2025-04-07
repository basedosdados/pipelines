# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""

import json
import os
import shutil
from datetime import timedelta
from typing import Dict, List, Optional, Union

import git
from dbt.cli.main import dbtRunner
from prefect import task
from prefect.engine.signals import FAIL

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import (
    constants as constants_execute,
)
from pipelines.utils.execute_dbt_model.utils import (
    extract_model_execution_status_from_logs,
    log_dbt_from_file,
    merge_vars,
    process_dbt_log_file,
    update_keyfile_path_in_profiles,
    update_profiles_for_env_credentials,
)
from pipelines.utils.utils import log


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_repository(repo_url: str, branch: str = "main") -> str:
    """
    Downloads the repository specified by the repo_url, optionally from a specific branch.

    Args:
        repo_url (str): queries-basedosdados repository url.
        branch (str, optional): The specific branch to clone. Defaults to None (which clones the default branch).

    Returns:
        str: Path to the downloaded repository.

    Raises:
        FAIL: If there is an error when creating the repository folder or downloading the repository.
    """

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
        if branch:
            repo = git.Repo.clone_from(
                repo_url, repository_path, branch=branch
            )
            log(f"Repository downloaded: {repo_url}, branch: {branch}")
        else:
            repo = git.Repo.clone_from(repo_url, repository_path)
            log(f"Repository downloaded: {repo_url}, default branch")

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

    return repository_path


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def install_dbt_dependencies(
    dbt_repository_path: str,
    custom_keyfile_path: Optional[str] = None,
    use_env_credentials: bool = True,
) -> None:
    """
    Install dbt dependencies using 'dbt deps' and configure authentication.

    This task performs three important functions:
    1. Updates the profiles.yml file for authentication (either file-based or environment-based)
    2. Installs all DBT dependencies using 'dbt deps'
    3. Validates that the installation was successful

    Args:
        dbt_repository_path (str): Path to the dbt repository.
        custom_keyfile_path (str, optional): Custom path to the keyfile to use for
            BigQuery authentication. If provided, the keyfile path in profiles.yml
            will be updated. Defaults to None.
        use_env_credentials (bool, optional): Whether to update profiles.yml to use
            environment variables for authentication. This is only used if
            custom_keyfile_path is None. Defaults to True.

    Returns:
        None

    Raises:
        FAIL: If there is an error when installing dependencies or configuring authentication.
    """
    try:
        os.chdir(dbt_repository_path)
        log(f"Working directory set to: {dbt_repository_path}", level="info")

        if custom_keyfile_path:
            try:
                update_keyfile_path_in_profiles(
                    dbt_repository_path, custom_keyfile_path
                )
                log(
                    f"Updated profiles.yml to use custom keyfile: {custom_keyfile_path}",
                    level="info",
                )
            except Exception as e:
                raise FAIL(
                    f"Failed to update profiles.yml with custom keyfile: {str(e)}"
                ) from e
        elif use_env_credentials:
            try:
                update_profiles_for_env_credentials(dbt_repository_path)
                log(
                    "Updated profiles.yml to use environment-based authentication",
                    level="info",
                )
            except Exception as e:
                raise FAIL(
                    f"Failed to update profiles.yml for environment credentials: {str(e)}"
                ) from e

        log("Installing dbt dependencies...", level="info")
        dbt_runner = dbtRunner()
        cli_args = ["deps"]
        result = dbt_runner.invoke(cli_args)

        if not result.success:
            error_details = ""
            if hasattr(result, "result") and hasattr(result.result, "logs"):
                error_logs = [
                    log_entry.get("message", "")
                    for log_entry in result.result.logs
                    if log_entry.get("levelname")
                    in ("ERROR", "CRITICAL", "FATAL")
                ]
                if error_logs:
                    error_details = f" Details: {'; '.join(error_logs)}"

            raise FAIL(f"Failed to install dbt dependencies.{error_details}")

        log("Successfully installed dbt dependencies", level="info")
        return result
    except Exception as e:
        if not isinstance(e, FAIL):
            raise FAIL(f"Error installing dbt dependencies: {str(e)}") from e
        else:
            raise


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def execute_dbt_model(
    dbt_repository_path: str,
    dataset_id: str,
    table_id: Optional[str] = None,
    dbt_alias: bool = True,
    dbt_command: str = "run",
    target: str = "dev",
    flags: Optional[str] = None,
    _vars: Optional[Union[dict, List[Dict], str]] = None,
    disable_elementary: bool = False,
) -> bool:
    """
    Execute a DBT model and process logs from the log file.

    Args:
        dbt_repository_path (str): Path to the dbt repository.
        dataset_id (str): Dataset ID of the dbt model.
        table_id (str, optional): Table ID of the dbt model. If None, the
            whole dataset will be run.
        dbt_alias (bool, optional): If True, the model will be run by
            its alias. Defaults to True.
        dbt_command (str, optional): The dbt command to run. Defaults to "run".
        target (str, optional): The dbt target to use. Defaults to "dev".
        flags (Optional[str], optional): Flags to pass to the dbt command. Defaults to None.
        _vars (Optional[Union[dict, List[Dict], str]], optional): Variables to pass to
            dbt. Defaults to None.
        disable_elementary (bool, optional): Disable elementary on-run-end hooks. Defaults to False.

    Raises:
        ValueError: If dbt_command is invalid.
        FAIL: If dbt execution fails.
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

    logs_dir = os.path.join(dbt_repository_path, "logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
        log(f"Created logs directory: {logs_dir}", level="info")

    os.chdir(dbt_repository_path)
    log(f"Working directory set to: {dbt_repository_path}", level="info")

    commands_to_run = []
    if "run" in dbt_command:
        commands_to_run.append("run")
    if "test" in dbt_command:
        commands_to_run.append("test")

    for cmd in commands_to_run:
        cli_args = [cmd, "--select", selected_table, "--target", target]

        if flags and flags.startswith("--full-refresh") and cmd == "run":
            cli_args.insert(1, "--full-refresh")
        elif flags:
            cli_args.extend(flags.split())

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

        log(f"Executing dbt command: {' '.join(cli_args)}", level="info")

        try:
            dbt_runner = dbtRunner()
            result = dbt_runner.invoke(cli_args)

            if hasattr(result, "success"):
                if result.success:
                    log(
                        f"DBT runner reports success for {cmd} command",
                        level="info",
                    )
                else:
                    log(
                        f"DBT runner reports failure for {cmd} command",
                        level="warning",
                    )

            log_file_path = os.path.join(
                dbt_repository_path, "logs", "dbt.log"
            )

            if os.path.exists(log_file_path):
                log(f"Processing DBT log file: {log_file_path}", level="info")

                logs_df = process_dbt_log_file(log_file_path)

                if not logs_df.empty:
                    log(
                        f"Found {len(logs_df)} log entries in log file",
                        level="info",
                    )

                    log_summary = log_dbt_from_file(log_file_path)

                    model_status = extract_model_execution_status_from_logs(
                        logs_df
                    )
                    if model_status:
                        log(
                            f"Model execution status: {model_status}",
                            level="info",
                        )

                    if log_summary["error_count"] > 0:
                        if log_summary["error_count"] > 0:
                            raise FAIL(
                                f"DBT '{cmd}' command failed with {log_summary['error_count']} errors. See logs for details."
                            )
                else:
                    log("No log entries found in log file", level="warning")
            else:
                log(
                    f"DBT log file not found at {log_file_path}",
                    level="warning",
                )

                if hasattr(result, "success") and not result.success:
                    raise FAIL(
                        f"DBT '{cmd}' command failed and no log file was found."
                    )

        except Exception as e:
            if not isinstance(e, FAIL):
                error_msg = (
                    f"Unexpected error executing dbt {cmd}: {str(e)}\n\n"
                )
                raise FAIL(error_msg) from e
            else:
                raise
