"""
DBT-related flow (Prefect 3).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dbt.cli.main import dbtRunner
from prefect import flow, task


@task
def run_dbt_task(
    dataset_id: str,
    table_id: str | None,
    dbt_alias: bool,
    dbt_command: str,
    target: str,
    flags: str | None,
    dbt_vars: dict[str, Any] | str | None,
) -> None:
    if dbt_command not in ["run", "test", "run and test", "run/test"]:
        raise ValueError(f"Invalid dbt_command: {dbt_command}")

    models_folder = Path("models") / dataset_id
    if table_id is not None:
        model_path = (
            models_folder / f"{dataset_id}__{table_id}.sql"
            if dbt_alias
            else models_folder / f"{table_id}.sql"
        )
        selected = model_path.as_posix()
    else:
        model_path = models_folder
        selected = models_folder.as_posix()

    if not model_path.is_dir() and not model_path.exists():
        raise FileNotFoundError(f"Modelo dbt não encontrado: {model_path}")

    variables = (
        json.loads(dbt_vars) if isinstance(dbt_vars, str) else (dbt_vars or {})
    )

    commands: list[str] = []
    if "run" in dbt_command:
        commands.append("run")
    if "test" in dbt_command:
        commands.append("test")

    runner = dbtRunner()
    for cmd in commands:
        cli_args = [cmd, "--select", selected, "--target", target]
        if flags and flags.startswith("--full-refresh") and cmd == "run":
            cli_args.insert(1, "--full-refresh")
        elif flags:
            cli_args.extend(flags.split())
        cli_args.extend(["--vars", json.dumps(variables)])

        print(f"dbt | executing: {' '.join(cli_args)}")
        result = runner.invoke(cli_args)
        if result.exception:
            raise Exception(f"dbt {cmd} exception: {result.exception}")
        if not result.success:
            # pyrefly: ignore [not-iterable]
            for event in result.result or []:
                print(f"dbt | {getattr(event, 'message', event)}")
            raise Exception(f"dbt {cmd} falhou para {selected}")


@task
def download_data_to_gcs_task(dataset_id: str, table_id: str) -> None:
    # Lazy import to avoid Prefect 0 collateral during deploy-time imports
    from pipelines.utils.tasks import download_data_to_gcs

    fn = getattr(download_data_to_gcs, "fn", download_data_to_gcs)
    fn(dataset_id=dataset_id, table_id=table_id)


@flow(
    name="BD template: Executa DBT model",
    flow_run_name="DBT Model run/test: {dataset_id}.{table_id}",
    log_prints=True,
)
def run_dbt_model_flow(
    dataset_id: str,
    table_id: str | None = None,
    dbt_alias: bool = True,
    dbt_command: str = "run",
    flags: str | None = None,
    target: str = "dev",
    dbt_vars: dict[str, Any] | str | None = None,
    download_csv_file: bool = True,
) -> None:
    dbt_done = run_dbt_task.submit(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_alias=dbt_alias,
        dbt_command=dbt_command,
        target=target,
        flags=flags,
        dbt_vars=dbt_vars,
    )

    if download_csv_file and table_id is not None:
        # pyrefly: ignore [no-matching-overload]
        download_data_to_gcs_task.submit(
            dataset_id=dataset_id,
            table_id=table_id,
            wait_for=[dbt_done],
        ).result()
    else:
        dbt_done.result()


# pyrefly: ignore [missing-attribute]
run_dbt_model_flow.deploy_schedules = []
