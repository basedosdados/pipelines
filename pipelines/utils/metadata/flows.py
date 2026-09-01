"""
Flow `update_temporal_coverage` — utilitário genérico, sob demanda.

Atualiza a cobertura temporal (e demais metadados de materialização) de QUALQUER
`dataset_id`/`table_id`, delegando para `register_table_materialization_task`.

A cobertura é passada explicitamente como um `CoverageSpec` (união discriminada
do domínio: `PartBdpro`/`AllBdpro`/`AllFree`/`NonHistorical`), validada pelo
Pydantic no parsing do parâmetro do flow.
"""

from prefect import flow
from prefect.utilities.asyncutils import run_coro_as_sync

from pipelines.utils.materialize_prod.flows import transfer_files_to_prod_flow
from pipelines.utils.metadata.domain import CoverageSpec
from pipelines.utils.metadata.tasks import (
    register_table_materialization_task,
)
from pipelines.utils.tasks import rename_flow_run_dataset_table, run_dbt
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
def mat_test_flow(
    dataset_id: str,
    table_id: str,
    coverage: CoverageSpec,
    env: str = "prod",
    bq_project: str = "basedosdados",
    prefect_mode: str = "prod",
    targets: list[str] | None = None,
    partition_folders: list[str] | None = None,
    download_billing_project: str = "basedosdados",
) -> None:
    """
    Materializa e testa (dbt run/test) e atualiza a coverage da tabela no
    backend (`register_table_materialization_task`, chamado direto — mantém
    o retry da própria task, que se perderia atrás de um `@flow` wrapper
    sem retry configurado) — sempre a mesma sequência, então este flow é
    genérico e reaproveitado por qualquer dataset/tabela, disparado ao
    final de `flow_download` via `run_deployment()` (issue #1867,
    basedosdados/pipelines#1867 — ver `pipelines/utils/stage_dispatch.py`).

    Todos os parâmetros são tipados de verdade — `coverage: CoverageSpec`
    é validado pelo Pydantic automaticamente no momento em que o flow run
    é criado, mesmo mecanismo que já funciona de graça em
    `update_temporal_coverage`.

    `dataset_id`/`table_id` continuam parâmetros soltos (não dentro de um
    dict maior) pra aparecer visíveis direto na lista de runs do Prefect e
    dar pra nomear o flow run com eles (`rename_flow_run_dataset_table`).
    `rename_flow_run_dataset_table` é uma `@task` async — chamada sem
    `await` de um flow síncrono ela só cria uma coroutine e descarta (o
    rename nunca acontece, sem erro nem log — mesmo bug presente em vários
    outros flows do repositório que chamam essa task, issue #1940); por
    isso o `run_coro_as_sync(...)` em volta, que efetivamente roda a
    coroutine e espera o resultado.

    Sempre roda `run_dbt(target="dev")` primeiro — se falhar, a exceção
    propaga e aborta o flow antes de qualquer coisa tocar prod (mesma
    garantia do `if not materialize_after_dump` no padrão real de flow
    único). Só promove pra prod (`transfer_files_to_prod_flow`, subflow já
    existente em `pipelines/utils/materialize_prod/flows.py`: baixa do
    staging de dev, sobe no staging de prod, roda `run_dbt(target="prod")`)
    quando `"prod"` está em `targets` (default `["dev", "prod"]`) — datasets
    de teste sem `bq_project` real (ex. o piloto em `test_dataset`, confinado
    a `basedosdados-dev`) devem passar `["dev"]`.

    `partition_folders` (opcional, `None` por padrão): pastas de partição
    estilo Hive (ex. `ano=2026/mes=08`) que o `flow_download` upstream
    atualizou nesta execução — repassadas direto pra
    `transfer_files_to_prod_flow`, pra promover só a fatia nova, não o
    staging inteiro. Tabelas sem partição (como o piloto) não precisam
    desse campo.
    """
    run_coro_as_sync(
        rename_flow_run_dataset_table(
            prefix="Mat Test: ", dataset_id=dataset_id, table_id=table_id
        )
    )

    targets = targets or ["dev", "prod"]

    log(
        f"[mat_test] materializando {dataset_id}.{table_id} (targets={targets})"
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="dev",
    )

    if "prod" in targets:
        transfer_files_to_prod_flow(
            dataset_id=dataset_id,
            table_id=table_id,
            folders=partition_folders,
            source_bucket="basedosdados-dev",
            download_billing_project=download_billing_project,
            materialize_after_dump=True,
            update_metadata=False,
            dbt_command="run/test",
        )

    register_table_materialization_task(
        dataset_id=dataset_id,
        table_id=table_id,
        coverage=coverage,
        env=env,
        bq_project=bq_project,
        prefect_mode=prefect_mode,
    )
