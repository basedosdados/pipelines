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
    promote_to_prod: bool = True,
    partition_folders: list[str] | None = None,
    download_billing_project: str = "basedosdados",
    update_metadata: bool = False,
    dump_mode: str = "append",
    source_format: str = "csv",
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
        promote_to_prod: se `True`, roda `transfer_files_to_prod_flow`
            (promoção real pra prod). Quem despacha este flow
            (`dispatch_build_and_promote`) já decide esse valor sozinho, a
            partir de onde ele mesmo está rodando — não é algo que o
            dataset configura.
        partition_folders: pastas de partição estilo Hive (ex.
            `ano=2026/mes=08`) atualizadas nesta execução — repassadas pra
            `transfer_files_to_prod_flow`, pra promover só a fatia nova,
            não o staging inteiro. `None` (default) pra tabela sem
            partição.
        download_billing_project: projeto cobrado pelo download
            requester-pays do staging de dev, dentro de
            `transfer_files_to_prod_flow`.
        update_metadata: só tem efeito quando `promote_to_prod` é
            `False` — nesse caso, `transfer_files_to_prod_flow` nunca
            roda, e é ele quem normalmente atualiza o metadado. `True`
            registra a materialização mesmo assim, direto a partir do que
            foi materializado em dev. Quando `promote_to_prod` é `True`, o
            metadado sempre atualiza (via `transfer_files_to_prod_flow`)
            e este parâmetro é ignorado. **Cuidado em invocação manual**:
            `register_table_materialization_task` lê a tabela em
            `bq_project` — se `bq_project`/`prefect_mode` não forem
            também passados como `"basedosdados-dev"`/`"dev"` (o dispatch
            automático nunca passa `update_metadata=True`, só invocação
            manual chega nessa combinação), o registro tenta ler de prod,
            onde a materialização em dev não existe.
        dump_mode: modo de escrita no BigQuery usado no upload pra prod
            dentro de `transfer_files_to_prod_flow` — vem de
            `ExtractAndLoad.dump_mode`, o mesmo usado no upload pro
            staging de dev em `extract_and_load`. Default `"append"`
            cobre invocação manual sem esse valor.
        source_format: formato do arquivo em staging (`"csv"` ou
            `"parquet"`) usado no upload pra prod dentro de
            `transfer_files_to_prod_flow` — vem de
            `ExtractAndLoad.source_format`. Default `"csv"` cobre
            invocação manual sem esse valor; um dataset em `"parquet"`
            que não passar isso corretamente falha com
            `FileNotFoundError` ao promover (procuraria `.csv`).
    """
    rename_flow_run_dataset_table(
        prefix="Build and Promote: ",
        dataset_id=dataset_id,
        table_id=table_id,
    )

    log(
        f"[build_and_promote] materializando {dataset_id}.{table_id} "
        f"(promote_to_prod={promote_to_prod})"
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        target="dev",
    )

    if promote_to_prod:
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
            dump_mode=dump_mode,
            source_format=source_format,
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
