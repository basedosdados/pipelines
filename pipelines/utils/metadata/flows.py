"""
Flow `update_temporal_coverage` — utilitário genérico, sob demanda.

Atualiza a cobertura temporal (e demais metadados de materialização) de QUALQUER
`dataset_id`/`table_id`, delegando para `register_table_materialization_task`.

A cobertura é passada explicitamente como um `CoverageSpec` (união discriminada
do domínio: `PartBdpro`/`AllBdpro`/`AllFree`/`NonHistorical`), validada pelo
Pydantic no parsing do parâmetro do flow.
"""

from prefect import flow

from pipelines.utils.automations import decode_params
from pipelines.utils.metadata.domain import CoverageSpec
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import run_dbt
from pipelines.utils.utils import log


@flow(
    name="update_temporal_coverage",
    log_prints=True,
)
def update_temporal_coverage(
    dataset_id: str,
    table_id: str,
    coverage: CoverageSpec,
    env: str = "prod",
    bq_project: str = "basedosdados",
    prefect_mode: str = "prod",
) -> None:
    register_table_materialization_task(
        dataset_id=dataset_id,
        table_id=table_id,
        coverage=coverage,
        env=env,
        bq_project=bq_project,
        prefect_mode=prefect_mode,
    )


# pyrefly: ignore [missing-attribute]
update_temporal_coverage.deploy_schedules = []


@flow(name="mat_test", log_prints=True)
def mat_test_flow(mat_test_params: str) -> None:
    """
    Materializa e testa (dbt run/test em dev e prod) e atualiza a coverage
    da tabela no backend (via `update_temporal_coverage`, chamado como
    subflow) — sempre a mesma sequência, então este flow é genérico e
    reaproveitado por qualquer dataset/tabela como Automação 2
    (flow_download -> mat_test) na cadeia de eventos da issue #1867
    (basedosdados/pipelines#1867): tudo que varia por dataset (dataset_id,
    table_id, coverage, env, bq_project) vem do payload do evento
    (`mat_test_params`, JSON — ver `pipelines/utils/automations.py::encode_params`),
    não de código hardcoded aqui.
    """
    params = decode_params(mat_test_params)
    dataset_id = params["dataset_id"]
    table_id = params["table_id"]

    log(f"[mat_test] materializando {dataset_id}.{table_id}")

    for target in ("dev", "prod"):
        run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            target=target,
        )

    update_temporal_coverage(
        dataset_id=dataset_id,
        table_id=table_id,
        coverage=params["coverage"],
        env=params.get("env", "prod"),
        bq_project=params.get("bq_project", "basedosdados"),
        prefect_mode=params.get("prefect_mode", "prod"),
    )
