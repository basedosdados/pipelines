"""
Flow `update_temporal_coverage` — utilitário genérico, sob demanda.

Atualiza a cobertura temporal (e demais metadados de materialização) de QUALQUER
`dataset_id`/`table_id`, delegando para `register_table_materialization_task`.

A cobertura é passada explicitamente como um `CoverageSpec` (união discriminada
do domínio: `PartBdpro`/`AllBdpro`/`AllFree`/`NonHistorical`), validada pelo
Pydantic no parsing do parâmetro do flow.
"""

from prefect import flow

from pipelines.utils.metadata.domain import CoverageSpec
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)


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
) -> None:
    register_table_materialization_task(
        dataset_id=dataset_id,
        table_id=table_id,
        coverage=coverage,
        env=env,
        bq_project=bq_project,
    )


# pyrefly: ignore [missing-attribute]
update_temporal_coverage.deploy_schedules = []
