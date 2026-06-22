"""
Flows for br_tse_eleicoes — Prefect 3.

One deployment per published table (built by the ``_tse_table_flow`` factory),
plus a ``br_tse_eleicoes__refresh`` orchestrator that triggers them in the
topological wave order of ``constants.TABLE_FLOWS`` via ``run_deployment`` +
``wait_for``. ``deploy_flows.py`` registers every module-level ``Flow`` object
found here as its own deployment.

Profiles (see constants):
  * producer    — build (python) → normalize → upload_to_gcs → run_dbt
  * dbt_rollup  — run_dbt only (the model is a pure ``ref()`` GROUP BY)
  * python_agg  — aggregate (python) → upload_to_gcs → run_dbt

Caveat (documented TODO): ``normalize`` is a Phase-2 *global* barrier — it
builds ``norm_candidatos`` and writes the partitioned parquets for every
producer table at once. Running it inside each producer flow keeps every
deployment independently runnable, but for a full ``refresh`` it is redundant;
a future optimization is a dedicated build+normalize deployment the producers
depend on. The wave graph below already encodes the correct ordering.
"""

from __future__ import annotations

import asyncio

from prefect import flow
from prefect.deployments import run_deployment

from pipelines.br_tse_eleicoes.constants import (
    PROFILE_DBT_ROLLUP,
    PROFILE_PRODUCER,
    PROFILE_PYTHON_AGG,
    TABLE_FLOWS,
    TableFlow,
    constants,
)
from pipelines.br_tse_eleicoes.tasks import output_path_for, run_build_step
from pipelines.br_tse_eleicoes.utils import deployment_name, flow_name
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value


def _tse_table_flow(spec: TableFlow):
    @flow(name=flow_name(spec.table_id), log_prints=True)
    def _flow(
        dataset_id: str = DATASET_ID,
        table_id: str = spec.table_id,
        env: str = "dev",
        run_build: bool = True,
        run_normalize: bool = True,
        dbt_alias: bool = False,
    ) -> None:
        rename_flow_run_dataset_table(
            prefix="Refresh: ", dataset_id=dataset_id, table_id=table_id
        )
        bucket = constants.BUCKET.value[env]

        if spec.profile == PROFILE_DBT_ROLLUP:
            # Pure dbt: ref() over the published zona table resolves upstream.
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run/test",
                dbt_alias=dbt_alias,
                target=env,
            )
            return

        if spec.profile in (PROFILE_PRODUCER, PROFILE_PYTHON_AGG):
            if run_build and spec.build_step:
                run_build_step(spec.build_step)
            if spec.profile == PROFILE_PRODUCER and run_normalize:
                run_build_step(constants.NORMALIZE_STEP.value)

            upload_to_gcs(
                data_path=output_path_for(table_id),
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name=bucket,
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run/test",
                dbt_alias=dbt_alias,
                target=env,
            )

    return _flow


# Register one module-level Flow per table (picked up by deploy_flows.py).
for _spec in TABLE_FLOWS:
    globals()[flow_name(_spec.table_id)] = _tse_table_flow(_spec)


# ---------------------------------------------------------------------------
# Orchestrator — triggers per-table deployments in topological wave order
# ---------------------------------------------------------------------------


@flow(name=f"{DATASET_ID}__refresh", log_prints=True)
async def br_tse_eleicoes__refresh(env: str = "dev") -> None:
    """Rebuild the whole dataset in dependency order.

    Each table runs as its own deployment. ``run_deployment`` is async-native in
    Prefect 3 and awaits the child run to completion, so the dependency graph is
    expressed with asyncio: a table's coroutine awaits its upstreams' coroutines
    (the ``wait_for`` edges from ``constants.TABLE_FLOWS``, derived from
    DEPENDENCIES.md) before triggering its own deployment. Independent tables
    run concurrently; the work pool's concurrency limit caps real parallelism.
    """
    runs: dict[str, asyncio.Task] = {}

    async def run_table(spec: TableFlow):
        if spec.wait_for:
            await asyncio.gather(
                *(runs[u] for u in spec.wait_for if u in runs)
            )
        run = await run_deployment(
            name=deployment_name(spec.table_id),
            parameters={"env": env},
        )
        print(f"refresh | {spec.table_id} → {getattr(run, 'state_name', '?')}")
        return run

    # Populate the whole task map first (wave-sorted, so every upstream exists
    # before a dependent references it); then await them all.
    for spec in sorted(TABLE_FLOWS, key=lambda s: s.wave):
        runs[spec.table_id] = asyncio.create_task(run_table(spec))
    await asyncio.gather(*runs.values())


br_tse_eleicoes__refresh.deploy_schedules = []
