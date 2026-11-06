"""
Flow de transferência de arquivos do bucket basedosdados-dev para basedosdados
(produção) e materialização da tabela em produção — Prefect 3.
"""

from __future__ import annotations

from pipelines.utils.flow import flow
from pipelines.utils.materialize_prod.tasks import (
    download_files_from_bucket_folders,
)
from pipelines.utils.metadata.coverage import CoverageSpec
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


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
    source_format: str = "csv",
) -> None:
    """Baixa do staging de dev, sobe no staging de prod, materializa,
    testa e registra a materialização de uma tabela.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela no backend/BigQuery.
        folders: pastas de partição estilo Hive a transferir (ex.
            `["ano=2026/mes=09"]`). `None` pra tabela sem partição —
            transfere `staging/{dataset_id}/{table_id}/` inteiro.
        source_bucket: bucket de origem (requester-pays).
        download_billing_project: projeto cobrado pelo acesso
            requester-pays ao `source_bucket`.
        materialize_after_dump: quando `False`, só baixa os arquivos e
            encerra — não sobe pro prod, não roda dbt, não registra
            metadado.
        dbt_command: comando dbt a rodar em prod (`"run"`, `"test"`,
            `"run/test"`, ...).
        coverage: `CoverageSpec` já validado, se disponível — útil pra
            disparo manual sobrescrevendo o que está no backend. Quando
            `None` (padrão), busca do próprio backend (`coverageSpec`, ver
            `pipelines.utils.metadata.coverage`).
        env: backend de destino.
        bq_project: projeto BigQuery onde a tabela vive.
        prefect_mode: resolve o projeto de billing.
        dump_mode: modo de escrita no BigQuery pro upload em prod — deve
            bater com o `dump_mode` usado no upload pro staging de dev
            (`ExtractAndLoad.dump_mode`). Default `"append"` cobre
            invocação manual sem esse valor.
        source_format: formato do arquivo em staging (`"csv"` ou
            `"parquet"`) — deve bater com `ExtractAndLoad.source_format`.
            Default `"csv"` cobre invocação manual sem esse valor; um
            dataset em `"parquet"` que não passar isso corretamente falha
            com `FileNotFoundError` (procuraria `.csv`).
    """
    rename_flow_run_dataset_table(
        prefix="Materialização Prod: ",
        dataset_id=dataset_id,
        table_id=table_id,
    )

    output_filepath = download_files_from_bucket_folders(
        dataset_id=dataset_id,
        table_id=table_id,
        folders=folders,
        source_bucket=source_bucket,
        billing_project=download_billing_project,
    )

    if not materialize_after_dump:
        return

    # Chamadas diretas (síncronas e bloqueantes): garantem a ordem
    # upload → dbt → register e propagam falhas para o flow run. Usar
    # .submit() aqui faria o flow retornar antes das tasks terminarem e o
    # task runner cancelaria as pendentes ("cancelled by the runtime").
    upload_to_gcs(
        data_path=output_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode=dump_mode,
        source_format=source_format,
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
