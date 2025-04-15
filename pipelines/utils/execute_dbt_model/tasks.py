# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""

import json
import os
from datetime import timedelta
from typing import Dict, List, Optional, Union

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
    process_dbt_log_file,
    # update_keyfile_path_in_profiles,
    # update_profiles_for_env_credentials,
)
from pipelines.utils.utils import log


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def run_dbt(
    dataset_id: str,
    table_id: Optional[str] = None,
    dbt_alias: bool = True,
    dbt_command: str = "run",
    target: str = "dev",
    flags: Optional[str] = None,
    _vars: Optional[Union[dict, List[Dict], str]] = None,
    disable_elementary: bool = False,
    # custom_keyfile_path: str | None = None,
    # use_env_credentials: bool = True,
) -> None:
    """
    Execute a DBT model and process logs from the log file.

    Args:
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
        custom_keyfile_path (str): New path to use for the keyfile. Defaults to None.
        use_env_credentials (bool, optional): Whether to update profiles.yml to use
            environment variables for authentication. This is only used if
            custom_keyfile_path is None. Defaults to True.

    Raises:
        ValueError: If dbt_command is invalid.
        FAIL: If dbt execution fails.
    """
    if dbt_command not in ["run", "test", "run and test", "run/test"]:
        raise ValueError(f"Invalid dbt_command: {dbt_command}")

    profiles_path = "profiles.yml"
    with open(profiles_path, "r") as f:
        log(f.read())

    # update_profiles_for_env_credentials()

    # with open(profiles_path, "r") as f:
    #     log(f.read())

    # if custom_keyfile_path is not None:
    #     update_keyfile_path_in_profiles(custom_keyfile_path)
    #     log(
    #         f"Updated profiles.yml to use custom keyfile: {custom_keyfile_path}",
    #         level="info",
    #     )
    # elif use_env_credentials:
    #     update_profiles_for_env_credentials()
    #     log(
    #         "Updated profiles.yml to use environment-based authentication",
    #         level="info",
    #     )

    if table_id:
        if dbt_alias:
            selected_table = f"{dataset_id}.{dataset_id}__{table_id}"
        else:
            selected_table = f"{dataset_id}.{table_id}"
    else:
        selected_table = dataset_id

    _vars = json.loads(_vars) if isinstance(_vars, str) else _vars

    variables = (
        constants_execute.DISABLE_ELEMENTARY_VARS.value
        if disable_elementary and _vars is None
        else {**constants_execute.DISABLE_ELEMENTARY_VARS.value, **_vars}  # type: ignore
    )

    commands_to_run = []
    if "run" in dbt_command:
        commands_to_run.append("run")
    if "test" in dbt_command:
        commands_to_run.append("test")

    log_file_path = os.path.join("logs", "dbt.log")

    model_path = os.path.join(
        "models", dataset_id, f"{dataset_id}__{table_id}.sql"
    )

    if os.path.exists(model_path):
        log(f"{model_path} exists")

    for cmd in commands_to_run:
        cli_args = [cmd, "--select", selected_table, "--target", target]

        if flags and flags.startswith("--full-refresh") and cmd == "run":
            cli_args.insert(1, "--full-refresh")
        elif flags:
            cli_args.extend(flags.split())

        cli_args.extend(["--vars", f"{json.dumps(variables)}"])

        log(f"Executing dbt command: {' '.join(cli_args)}", level="info")

        try:
            dbt_runner = dbtRunner()
            result = dbt_runner.invoke(cli_args)

            if result.success:
                log(
                    f"DBT runner reports success for {cmd} command",
                    level="info",
                )
            else:
                log(
                    f"DBT runner reports failure for {cmd} command. {result.result}",
                    level="warning",
                )

            if os.path.exists(log_file_path):
                log(f"Processing DBT log file: {log_file_path}", level="info")

                logs_df = process_dbt_log_file(log_file_path)

                if not logs_df.empty:
                    log(
                        f"Found {len(logs_df)} log entries in log file",
                        level="info",
                    )
                    log(logs_df)

                    log_summary = log_dbt_from_file(log_file_path)
                    log(log_summary)

                    model_status = extract_model_execution_status_from_logs(
                        logs_df
                    )
                    log(model_status)

                    if len(model_status) > 0:
                        log(
                            f"Model execution status: {model_status}",
                            level="info",
                        )

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

                # if not result.success:
                #     raise FAIL(
                #         f"DBT '{cmd}' command failed and no log file was found."
                #     )

        except Exception as e:
            if not isinstance(e, FAIL):
                error_msg = (
                    f"Unexpected error executing dbt {cmd}: {str(e)}\n\n"
                )
                raise FAIL(error_msg) from e
            else:
                raise e
