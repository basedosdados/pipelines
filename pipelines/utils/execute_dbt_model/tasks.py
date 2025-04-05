# -*- coding: utf-8 -*-
"""
Tasks related to DBT flows.
"""

import json
from datetime import timedelta
from typing import Any, Literal

from dbt.cli.main import dbtRunner
from prefect import task

from pipelines.constants import constants
from pipelines.utils.execute_dbt_model.constants import (
    constants as constants_execute,
)
from pipelines.utils.utils import log


@task(
    checkpoint=False,
    max_retries=constants.RUN_DBT_MODEL_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def run_dbt_model(
    dataset_id: str,
    table_id: str,
    dbt_alias: bool = True,
    dbt_command: Literal["run", "test", "run/test"] = "run",
    target: Literal["prod", "dev"] = "dev",
    flags: str | None = None,
    _vars: dict[str, Any] | None = None,
    disable_elementary: bool = False,
) -> None:
    """
    Run a DBT model.

    Args:
        dataset_id (str): Dataset ID of the dbt model.
        table_id (str, optional): Table ID of the dbt model. If None, the
        whole dataset will be run.
        dbt_alias (bool, optional): If True, the model will be run by
        its alias. Defaults to False.
        dbt_command (str): DBT command. Defaults to "run".
        target (str): The dbt target to use. Defaults to "dev".
        flags (str, optional): Flags to pass to the dbt run command.
        See:
        https://docs.getdbt.com/reference/dbt-jinja-functions/flags/
        _vars (dict[str, Any], optional): Variables to pass to
        dbt. Defaults to None.
        disable_elementary (bool, optional): Disable elementary on-run-end hooks.
    """
    if dbt_command not in ["run", "test", "run and test", "run/test"]:
        raise ValueError(f"Invalid dbt_command: {dbt_command}")

    run_args: list[str] = []

    selected_table = (
        f"{dataset_id}.{dataset_id}__{table_id}"
        if dbt_alias
        else f"{dataset_id}.{table_id}"
    )

    flags_parsed = flags.split(" ") if flags is not None else []

    variables = (
        constants_execute.DISABLE_ELEMENTARY_VARS.value
        if disable_elementary and _vars is None
        else {**constants_execute.DISABLE_ELEMENTARY_VARS.value, **_vars}  # type: ignore
    )

    common_args = [
        f"--select {selected_table}",
        f"--target {target}",
        f"--vars '{json.dumps(variables)}'",
    ]

    if len(flags_parsed) > 0:
        common_args.extend([i for i in flags_parsed if i != "--full-refresh"])

    # --full-refresh flag is positional and part of run subcommand
    if "run" in dbt_command and "--full-refresh" in flags_parsed:
        run_args.insert(1, "--full-refresh")

    dbt_runner = dbtRunner()

    if "run" in dbt_command:
        log(f"Running dbt with arguments: {run_args}")
        dbt_run_result = dbt_runner.invoke(["run", *run_args])
        if not dbt_run_result.success:
            raise Exception(f"Failed to run dbt: {dbt_run_result.result}")

    if "test" in dbt_command:
        test_args = ["test", *common_args]

        log(f"Running dbt test with arguments: {test_args}")

        dbt_test_result = dbt_runner.invoke(test_args)

        if not dbt_test_result.success:
            raise Exception(
                f"Failed to run dbt test: {dbt_test_result.result}"
            )
