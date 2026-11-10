import asyncio
import logging as stdlib_logging
from argparse import ArgumentParser
from pathlib import Path

from prefect.deployments import run_deployment

# Prefect 3 deployment triggered for each modified table.
# Slug = "<flow name>/<deployment name>": the flow's @flow(name=...) is
# "BD template: Executa DBT model" and deploy_flows.py registers it under the
# python identifier "run_dbt_model_flow".
DBT_MODEL_DEPLOYMENT = "BD template: Executa DBT model/run_dbt_model_flow"

# Same deployment `pipelines` uses from `build_and_promote` (as a subflow) —
# here triggered as a real, remote flow run instead, since this script runs
# outside any Prefect flow. Slug = "<flow name>/<deployment name>": the
# flow's @flow(name=...) is "BD Utils: Transfere arquivos do bucket
# basedosdados-dev para basedosdados" and deploy_flows.py registers it under
# the python identifier "transfer_files_to_prod_flow". Does copy+schema
# sync+dbt run/test+export+register_table_materialization_task in one flow
# run — see ftwca/investigacoes/pipelines/
# promocao-dev-prod-pipelines-vs-table-approve-2026-11-03.md.
TRANSFER_FILES_DEPLOYMENT = (
    "BD Utils: Transfere arquivos do bucket basedosdados-dev para "
    "basedosdados/transfer_files_to_prod_flow"
)

PREFECT_UI_URL = "https://prefect3.basedosdados.org"


async def _read_flow_run_error_logs(flow_run_id: str) -> list:
    """Fetches ERROR-and-above logs for a Prefect flow run via the async client.

    Args:
        flow_run_id: UUID string of the Prefect flow run.

    Returns:
        List of log objects returned by the Prefect API.
    """
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import (
        LogFilter,
        LogFilterFlowRunId,
        LogFilterLevel,
    )

    async with get_client() as client:
        return await client.read_logs(
            log_filter=LogFilter(
                flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]),
                level=LogFilterLevel(ge_=stdlib_logging.ERROR),
            ),
        )


def print_flow_run_error_logs(flow_run_id: str) -> None:
    """Prints ERROR-and-above logs for a failed Prefect flow run to stdout.

    Fetches logs from the Prefect API and prints them between delimiters so
    they are visible directly in the GitHub Actions log. Does nothing if no
    error logs exist.

    Args:
        flow_run_id: UUID string of the Prefect flow run.
    """
    logs = asyncio.run(_read_flow_run_error_logs(str(flow_run_id)))
    if not logs:
        return
    print("--- Prefect error logs ---")
    for log in logs:
        level_name = stdlib_logging.getLevelName(log.level)
        print(f"[{level_name}] {log.timestamp}: {log.message}")
    print("--- End of Prefect error logs ---")


def get_datasets_tables_from_modified_files(
    modified_files: list[str],  # type: ignore
) -> list[tuple[str, str, bool]]:
    """
    Returns a list of (dataset_id, table_id) from the list of modified files.

    Args:
        modified_files (list[str]): List of modified files.

    Returns:
        list[tuple[str, str, bool]]: List of ``(dataset_id, table_id,
        exists)`` tuples — ``exists`` is ``False`` for a deleted file.
    """
    # Convert to Path
    modified_files: list[Path] = [Path(file) for file in modified_files]
    # Get SQL files. Only dbt models live under `models/` (model-paths in
    # dbt_project.yml); a `.sql` under tests-dbt/, macros/, or analysis/ is a
    # generic test or macro, not a table. Treating it as a phantom model would
    # abort prod materialization before any real table builds, so restrict to
    # models.
    #
    # A published table is exactly `models/<dataset_id>/<model>.sql`. Anything
    # deeper is not one: `dataset_id` is read off the parent directory, so
    # `models/<dataset_id>/<subdir>/<model>.sql` would yield `<subdir>` as the
    # dataset and launch a materialization flow for a dataset that does not
    # exist. Subdirectories hold intermediate models (e.g. the ephemeral
    # per-state models under world_wb_mides/states/, which compile into their
    # parent and have no table of their own) and archived SQL kept for
    # reference (br_inpe_queimadas/code/). Skip both.
    sql_files: list[Path] = [
        file
        for file in modified_files
        if file.suffix == ".sql"
        and len(file.parts) == 3
        and file.parts[0] == "models"
    ]

    datasets_tables: list[tuple[str, str, bool]] = [
        (file.parent.name, file.stem, file.exists()) for file in sql_files
    ]

    # Post-process table_id: some model files are named
    # `{dataset_id}__{table_id}.sql` (the real alias comes from
    # `config(alias=...)` inside the SQL, to avoid model-name collisions
    # across datasets) — strip the `{dataset_id}__` prefix to recover the
    # real table_id. `run_dbt` (pipelines.utils.tasks) detects on its own
    # whether the model file itself needs that prefix, so this is purely
    # about the table_id value, not about telling run_dbt how to select it.
    new_datasets_tables: list[tuple[str, str, bool]] = []

    for dataset_id, table_id, exists in datasets_tables:
        crop_str = f"{dataset_id}__"
        if table_id.startswith(crop_str):
            table_id = table_id[len(crop_str) :]
        new_datasets_tables.append((dataset_id, table_id, exists))

    return new_datasets_tables


def get_datasets_and_tables_for_modified_files(
    modified_files: list[str],
) -> list[tuple[str, str]]:
    """Returns only existing (dataset_id, table_id) tuples from modified files.

    Delegates to ``get_datasets_tables_from_modified_files`` and filters out
    entries whose SQL file no longer exists on disk (e.g. deleted files).

    Args:
        modified_files: List of file paths modified in the pull request.

    Returns:
        List of ``(dataset_id, table_id)`` tuples for tables whose SQL files
        still exist.
    """
    datasets_tables = get_datasets_tables_from_modified_files(modified_files)

    existing_datasets_tables = []

    for dataset_id, table_id, exists in datasets_tables:
        if exists:
            existing_datasets_tables.append((dataset_id, table_id))

    return existing_datasets_tables


def _launch_and_wait(
    deployment_name: str, parameters: dict, label: str
) -> None:
    """Dispara um flow run via `run_deployment` e espera terminar.

    Args:
        deployment_name: slug `<flow>/<deployment>` a disparar.
        parameters: parâmetros do flow run.
        label: identificador pra mensagens de log (ex. `"ds.tb"`).

    Raises:
        Exception: se o flow run não terminar em estado completo.
    """
    flow_run = run_deployment(
        name=deployment_name,
        parameters=parameters,
        timeout=None,
        as_subflow=False,
    )
    flow_run_url = f"{PREFECT_UI_URL}/runs/flow-run/{flow_run.id}"
    print(f" - Flow run launched: {flow_run_url}")

    state = flow_run.state
    if state is None or not state.is_completed():
        state_name = state.name if state is not None else "Unknown"
        print_flow_run_error_logs(str(flow_run.id))
        raise Exception(
            f"Flow run {flow_run.id} for {label} finished "
            f'with state "{state_name}". Check the full logs at {flow_run_url}'
        )

    print(f"Flow run {flow_run.id} ({label}) finished successfully.")


def run_table_approve() -> None:
    """Parses CLI arguments and orchestrates the table-approve pipeline.

    Builds the argument parser, resolves the list of modified SQL tables,
    optionally syncs staging data to the prod bucket, and launches a Prefect 3
    materialization flow run for each table. Raises an exception (failing the
    GitHub Actions job) if any flow run does not complete successfully.
    """
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--dbt-command", type=str, required=True)
    arg_parser.add_argument("--modified-files", type=str, required=False)
    arg_parser.add_argument("--sync-bucket", action="store_true")
    arg_parser.add_argument(
        "--source-bucket-name",
        type=str,
        required=False,
        default="basedosdados-dev",
    )
    # Deprecated: transfer_files_to_prod_flow usa os defaults de
    # copy_staging_to_prod ("basedosdados"/"basedosdados-backup") e não expõe
    # esses buckets como parâmetro — mantidos só pra quem ainda passa isso não
    # falhar no parsing.
    arg_parser.add_argument(
        "--destination-bucket-name",
        type=str,
        required=False,
        default="basedosdados",
    )
    arg_parser.add_argument(
        "--backup-bucket-name",
        type=str,
        required=False,
        default="basedosdados-backup",
    )
    # Deprecated: kept so callers that still pass it do not fail at argument parsing.
    arg_parser.add_argument(
        "--prefect-backend-token", type=str, required=False
    )
    arg_parser.add_argument(
        "--materialization-target", type=str, required=False, default="prod"
    )
    arg_parser.add_argument(
        "--materialization-label",
        type=str,
        required=False,
        default="basedosdados",
    )
    arg_parser.add_argument("--dataset-id", type=str, required=False)
    args = arg_parser.parse_args()

    existing_datasets_tables = (
        []
        if args.modified_files is None
        else get_datasets_and_tables_for_modified_files(
            args.modified_files.split(",")
        )
    )

    # Com --sync-bucket: cada tabela de PR dispara transfer_files_to_prod_flow
    # (copy+sync schema+dbt run/test+export+register, tudo num flow run remoto
    # — a mesma função que o pipelines usa via build_and_promote). Sem
    # --sync-bucket, ou a entrada manual de --dataset-id: não há dado novo a
    # copiar, só dispara run_dbt_model_flow (dbt run/test + export) como
    # sempre.
    if args.sync_bucket:
        for dataset_id, table_id in existing_datasets_tables:
            print(
                f"\n\n\n\n************   START PROMOTING TABLE {dataset_id}.{table_id}...   ************"
            )
            _launch_and_wait(
                TRANSFER_FILES_DEPLOYMENT,
                parameters={
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "source_bucket": args.source_bucket_name,
                    "dbt_command": args.dbt_command,
                    "env": args.materialization_target,
                },
                label=f"{dataset_id}.{table_id}",
            )
        remaining_datasets_tables: list[tuple[str, str]] = []
    else:
        print("Skipping sync bucket because --sync-bucket was not set")
        remaining_datasets_tables = list(existing_datasets_tables)

    if args.dataset_id is not None:
        remaining_datasets_tables.append((args.dataset_id, ""))

    for dataset_id, table_id in remaining_datasets_tables:
        print(f"Launching materialization flow for {dataset_id}.{table_id}...")
        _launch_and_wait(
            DBT_MODEL_DEPLOYMENT,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "dbt_command": args.dbt_command,
                "target": args.materialization_target,
                # Download csv file is true by default.
                # We disable when materialization target is not prod.
                "download_csv_file": args.materialization_target == "prod",
            },
            label=f"{dataset_id}.{table_id}",
        )


if __name__ == "__main__":
    run_table_approve()
