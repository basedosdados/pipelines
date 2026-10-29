"""Materialização de uma tabela junto com as tabelas derivadas dela.

`promote_primary_and_derived_tables` substitui o `dispatch_build_and_promote`
quando a tabela tem derivadas sem extração própria, que só rodam `dbt`
lendo dela.
"""

from pipelines.utils.metadata.constants import constants as metadata_constants
from pipelines.utils.metadata.domain import CoverageSpec
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.stage_dispatch.naming import _dev_prefix
from pipelines.utils.tasks import run_dbt, upload_to_gcs


def promote_primary_and_derived_tables(
    dataset_id: str,
    table_id: str,
    data_path: str,
    coverage: CoverageSpec,
    derived_table_ids: list[str],
    derived_coverage: CoverageSpec | dict[str, CoverageSpec],
    env: str = "prod",
    prefect_mode: str = "prod",
    dump_mode: str = "append",
    source_format: str = "csv",
) -> None:
    """Materializa `table_id` e re-materializa tabelas derivadas junto, inline.

    Pra quando uma tabela "mãe" tem uma ou mais tabelas "filhas" sem
    `extract_and_load` próprio — só `dbt run/test` lendo da mãe já
    materializada (ex. agregados calculados a partir de um `microdados`).
    `dispatch_build_and_promote` não serve aqui: ele despacha
    `build_and_promote` por uma única tabela, que por sua vez baixa o
    staging de dev daquela tabela — as filhas não têm staging nenhum pra
    baixar. Esta função substitui esse dispatch por completo, fazendo
    upload/dbt/registro de todas as tabelas (mãe + filhas) na própria run
    do `extract_and_load`, igual o flow monolítico antigo já fazia.

    Mesma garantia estrutural de `dispatch_build_and_promote`:
    `promote_to_prod` nunca é parâmetro do chamador, vem de `_dev_prefix()`
    — um dataset rodando num deployment de dev nunca promove pra prod de
    verdade.

    Args:
        dataset_id: ID do dataset no backend/BigQuery.
        table_id: ID da tabela mãe — a que `extract_load_data` baixou.
        data_path: caminho local escrito por `extract_load_data` pra
            `table_id` (mesmo valor que iria em `ExtractAndLoad.data_path`).
        coverage: `CoverageSpec` da tabela mãe.
        derived_table_ids: tabelas filhas, sem download próprio — só
            `dbt run/test` lendo da mãe.
        derived_coverage: `CoverageSpec` das filhas. Uma só, aplicada a
            todas, ou um dict `{table_id: CoverageSpec}` quando elas não
            compartilham a mesma cobertura.
        env: backend de destino.
        prefect_mode: resolve o projeto de billing/BigQuery
            (`MODE_PROJECT`) e é repassado a
            `register_table_materialization_task`.
        dump_mode: modo de escrita no BigQuery pro upload da mãe em prod.
        source_format: formato do arquivo em staging (`"csv"`/`"parquet"`)
            pro upload da mãe em prod.
    """
    bq_project = metadata_constants.MODE_PROJECT.value[prefect_mode]
    all_table_ids = [table_id, *derived_table_ids]

    for t in all_table_ids:
        run_dbt(
            dataset_id=dataset_id,
            table_id=t,
            dbt_command="run/test",
            target="dev",
        )

    if _dev_prefix():
        return

    upload_to_gcs(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode=dump_mode,
        source_format=source_format,
    )

    for t in all_table_ids:
        run_dbt(
            dataset_id=dataset_id,
            table_id=t,
            dbt_command="run/test",
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
    for t in derived_table_ids:
        t_coverage = (
            derived_coverage[t]
            if isinstance(derived_coverage, dict)
            else derived_coverage
        )
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=t,
            coverage=t_coverage,
            env=env,
            bq_project=bq_project,
            prefect_mode=prefect_mode,
        )
