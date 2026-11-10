"""
Flow de transferência de arquivos do bucket basedosdados-dev para basedosdados
(produção) e materialização da tabela em produção — Prefect 3.
"""

from __future__ import annotations

from pipelines.utils.flow import flow
from pipelines.utils.materialize_prod.tasks import (
    copy_staging_to_prod,
    sync_prod_staging_table,
)
from pipelines.utils.metadata.coverage import CoverageSpec
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.tasks import rename_flow_run_dataset_table, run_dbt


@flow(
    name="BD Utils: Transfere arquivos do bucket basedosdados-dev para basedosdados",
    log_prints=True,
)
def transfer_files_to_prod_flow(
    dataset_id: str = "br_cgu_beneficios_cidadao",
    table_id: str = "novo_bolsa_familia",
    folders: list[str] | None = None,
    source_bucket: str = "basedosdados-dev",
    download_billing_project: str = "basedosdados",
    materialize_after_dump: bool = True,
    dbt_command: str = "run",
    coverage: CoverageSpec | None = None,
    env: str = "prod",
    bq_project: str = "basedosdados",
    prefect_mode: str = "prod",
    dump_mode: str = "append",
) -> None:
    """Copia o staging de dev pro staging de prod (cópia server-side, sem
    baixar/subir pelo worker), materializa, testa e registra a
    materialização de uma tabela.

    Mesma função usada pelo `table-approve` (via
    `.github/workflows/scripts/prefect_run_dbt.py`) — ver
    `ftwca/investigacoes/pipelines/
    promocao-dev-prod-pipelines-vs-table-approve-2026-11-03.md` pro desenho
    completo.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        folders: pastas de partição estilo Hive a promover (ex.
            `["ano=2026/mes=09"]`). `None` promove a tabela inteira —
            inclui backup do staging de prod atual e recriação completa da
            definição da tabela, igual ao comportamento atual do
            `table-approve`. Com `folders`, só os blobs indicados são
            copiados e só colunas novas são sincronizadas ao schema.
        source_bucket: bucket de origem (requester-pays).
        download_billing_project: projeto cobrado pelo acesso
            requester-pays ao `source_bucket`.
        materialize_after_dump: quando `False`, só copia os arquivos e
            encerra — não roda dbt, não registra metadado.
        dbt_command: comando dbt a rodar em prod (`"run"`, `"test"`,
            `"run/test"`, ...).
        coverage: `CoverageSpec` já validado, se disponível — útil pra
            disparo manual sobrescrevendo o que está no backend. Quando
            `None` (padrão), busca do próprio backend (`coverageSpec`, ver
            `pipelines.utils.metadata.coverage`).
        env: backend de destino.
        bq_project: projeto BigQuery onde a tabela vive.
        prefect_mode: resolve o projeto de billing.
        dump_mode: vestigial — a cópia server-side não depende mais do
            modo de escrita declarado (`copy_staging_to_prod`/
            `sync_prod_staging_table` decidem o comportamento a partir de
            `folders`). Mantido só pra não quebrar o caller atual
            (`build_and_promote`).
    """
    rename_flow_run_dataset_table(
        prefix="Materialização Prod: ",
        dataset_id=dataset_id,
        table_id=table_id,
    )

    sample_path = copy_staging_to_prod(
        dataset_id=dataset_id,
        table_id=table_id,
        folders=folders,
        source_bucket=source_bucket,
        billing_project=download_billing_project,
    )

    if not materialize_after_dump:
        return

    # Sem folders: promoção completa (backup + recriação da tabela, igual
    # ao table-approve atual). Com folders: promoção parcial (só sincroniza
    # colunas novas ao schema existente).
    is_full_promotion = folders is None

    # Chamadas diretas (síncronas e bloqueantes): garantem a ordem
    # sync → dbt → register e propagam falhas para o flow run. Usar
    # .submit() aqui faria o flow retornar antes das tasks terminarem e o
    # task runner cancelaria as pendentes ("cancelled by the runtime").
    sync_prod_staging_table(
        dataset_id=dataset_id,
        table_id=table_id,
        sample_path=sample_path,
        bucket_name="basedosdados",
        recreate=is_full_promotion,
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command=dbt_command,
        target="prod",
    )

    register_table_materialization_task(
        dataset_id=dataset_id,
        table_id=table_id,
        coverage=coverage,
        env=env,
        bq_project=bq_project,
        prefect_mode=prefect_mode,
    )


transfer_files_to_prod_flow.deploy_schedules = []  # utilitário, disparo manual
