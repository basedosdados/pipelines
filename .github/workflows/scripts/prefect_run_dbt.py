import asyncio
import logging as stdlib_logging
from argparse import ArgumentParser
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    LogFilter,
    LogFilterFlowRunId,
    LogFilterLevel,
)
from prefect.deployments import run_deployment

# Same deployment `pipelines` dispatches via `dispatch_build_and_promote`.
# No "dev-" prefix: `build_and_promote` is always deployed to the prod pool
# (see `deployment_name()` in `pipelines/utils/stage_dispatch.py`).
BUILD_AND_PROMOTE_DEPLOYMENT = "build_and_promote/build_and_promote"

PREFECT_UI_URL = "https://prefect3.basedosdados.org"


async def _read_flow_run_error_logs(flow_run_id: str) -> list:
    """Fetches ERROR-and-above logs for a Prefect flow run via the async client.

    Args:
        flow_run_id: UUID string of the Prefect flow run.

    Returns:
        List of log objects returned by the Prefect API.
    """
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


async def _poll_flow_run_until_final(
    flow_run_id: str,
    poll_interval: float = 10,
    max_consecutive_errors: int = 3,
):
    """Espera um flow run terminar, via polling HTTP simples.

    `prefect.flow_runs.wait_for_flow_run` usa o subsistema de eventos
    (WebSocket) do Prefect, que não funciona neste self-hosted (redireciona
    pra login, HTTP 302) — por isso o polling manual aqui, igual ao que
    `run_deployment`/`arun_deployment` já fazem por baixo dos panos.

    Um erro passageiro da API (timeout, 5xx) numa checagem não derruba o
    polling inteiro — o flow continua rodando no Prefect independente da
    Action conseguir falar com a API nesse instante. Só desiste depois de
    `max_consecutive_errors` falhas seguidas.

    Args:
        flow_run_id: UUID string do flow run.
        poll_interval: segundos entre cada checagem de estado.
        max_consecutive_errors: falhas seguidas da API toleradas antes de
            desistir e propagar o erro.

    Returns:
        O `FlowRun` já em estado final.
    """
    async with get_client() as client:
        consecutive_errors = 0
        while True:
            try:
                flow_run = await client.read_flow_run(flow_run_id)
            except Exception as exc:
                consecutive_errors += 1
                if consecutive_errors > max_consecutive_errors:
                    raise
                print(
                    f" - Transient error polling flow run {flow_run_id} "
                    f"({consecutive_errors}/{max_consecutive_errors}): {exc}"
                )
                await asyncio.sleep(poll_interval)
                continue

            consecutive_errors = 0
            if flow_run.state and flow_run.state.is_final():
                return flow_run
            await asyncio.sleep(poll_interval)


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
    # timeout=0: devolve assim que o flow run é criado, sem esperar — com
    # timeout=None, run_deployment só retornaria (e só então imprimiria a
    # URL) depois do flow run já ter terminado.
    flow_run = run_deployment(
        name=deployment_name,
        parameters=parameters,
        timeout=0,
        as_subflow=False,
    )
    flow_run_url = f"{PREFECT_UI_URL}/runs/flow-run/{flow_run.id}"
    print(f" - Flow run launched: {flow_run_url}")

    try:
        flow_run = asyncio.run(_poll_flow_run_until_final(str(flow_run.id)))
    except Exception as exc:
        raise Exception(
            f"Could not confirm the final state of flow run {flow_run.id} "
            f"for {label} after repeated API errors: {exc}. The flow may "
            f"still be running or have already finished — check {flow_run_url}"
        ) from exc

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

    Builds the argument parser, resolves the list of modified SQL tables, and
    launches a Prefect 3 materialization flow run for each one. Raises an
    exception (failing the GitHub Actions job) if any flow run does not
    complete successfully.
    """
    arg_parser = ArgumentParser()
    arg_parser.add_argument("--modified-files", type=str, required=False)
    args = arg_parser.parse_args()

    existing_datasets_tables = (
        []
        if args.modified_files is None
        else get_datasets_and_tables_for_modified_files(
            args.modified_files.split(",")
        )
    )

    # Cada tabela de PR dispara build_and_promote — o MESMO deployment que o
    # pipelines despacha via dispatch_build_and_promote, não só a mesma
    # função interna. Roda dbt run/test em dev e, promovendo pra prod
    # (promote_to_prod=True, o default), copy+sync schema+dbt run/test em
    # prod+export+register, tudo num único flow run remoto.
    for dataset_id, table_id in existing_datasets_tables:
        print(
            f"\n\n\n\n************   START PROMOTING TABLE {dataset_id}.{table_id}...   ************"
        )
        _launch_and_wait(
            BUILD_AND_PROMOTE_DEPLOYMENT,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "env": "prod",
            },
            label=f"{dataset_id}.{table_id}",
        )


if __name__ == "__main__":
    run_table_approve()
