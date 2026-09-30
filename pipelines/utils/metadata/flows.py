"""Flows genéricos de materialização e metadado.

`update_temporal_coverage`: utilitário standalone, disparo manual.
`build_and_promote`: deployment único, compartilhado por todos os
datasets/tabelas que o disparam.
"""

from pipelines.utils.flow import flow
from pipelines.utils.materialize_prod.flows import transfer_files_to_prod_flow
from pipelines.utils.metadata.constants import BUILD_AND_PROMOTE_JOB_VARIABLES
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
    """Atualiza a cobertura temporal e demais metadados de materialização
    de uma tabela, delegando pra `register_table_materialization_task`.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        coverage: especificação de cobertura (`CoverageSpec` — união
            discriminada `PartBdpro`/`AllBdpro`/`AllFree`/`NonHistorical`),
            validada pelo Pydantic no parsing do parâmetro do flow.
        env: backend de destino.
        bq_project: projeto BigQuery onde a tabela vive.
        prefect_mode: resolve o projeto de billing (`MODE_PROJECT`).
    """
    register_table_materialization_task(
        dataset_id=dataset_id,
        table_id=table_id,
        coverage=coverage,
        env=env,
        bq_project=bq_project,
        prefect_mode=prefect_mode,
    )


update_temporal_coverage.deploy_schedules = []


@flow(name="build_and_promote", log_prints=True)
def build_and_promote(
    dataset_id: str,
    table_id: str,
    coverage: CoverageSpec,
    env: str = "prod",
    bq_project: str = "basedosdados",
    prefect_mode: str = "prod",
    targets: list[str] | None = None,
    partition_folders: list[str] | None = None,
    download_billing_project: str = "basedosdados",
    update_metadata: bool = False,
) -> None:
    """Materializa, testa e promove uma tabela pra prod, registrando a
    materialização ao final.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        coverage: especificação de cobertura (`CoverageSpec`), validada
            pelo Pydantic no parsing do parâmetro do flow.
        env: backend de destino.
        bq_project: projeto BigQuery onde a tabela vive.
        prefect_mode: resolve o projeto de billing (`MODE_PROJECT`),
            repassado até `register_table_materialization_task`.
        targets: ambientes a promover. `None` (default) vira
            `["dev", "prod"]`.
        partition_folders: pastas de partição estilo Hive (ex.
            `ano=2026/mes=08`) atualizadas nesta execução — repassadas pra
            `transfer_files_to_prod_flow`, pra promover só a fatia nova,
            não o staging inteiro. `None` (default) pra tabela sem
            partição.
        download_billing_project: projeto cobrado pelo download
            requester-pays do staging de dev, dentro de
            `transfer_files_to_prod_flow`.
        update_metadata: só tem efeito quando `"prod"` **não** está em
            `targets` — nesse caso, `transfer_files_to_prod_flow` nunca
            roda, e é ele quem normalmente atualiza o metadado. `True`
            registra a materialização mesmo assim, direto a partir do que
            foi materializado em dev. Quando `"prod"` está em `targets`, o
            metadado sempre atualiza (via `transfer_files_to_prod_flow`)
            e este parâmetro é ignorado.
    """
    rename_flow_run_dataset_table(
        prefix="Build and Promote: ",
        dataset_id=dataset_id,
        table_id=table_id,
    )

    targets = targets or ["dev", "prod"]

    log(
        f"[build_and_promote] materializando {dataset_id}.{table_id} (targets={targets})"
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
            coverage=coverage,
            dbt_command="run/test",
            env=env,
            bq_project=bq_project,
            prefect_mode=prefect_mode,
        )
    elif update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=coverage,
            env=env,
            bq_project=bq_project,
            prefect_mode=prefect_mode,
        )


build_and_promote.job_variables = BUILD_AND_PROMOTE_JOB_VARIABLES
