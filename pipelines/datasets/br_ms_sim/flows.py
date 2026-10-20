"""
Flows de br_ms_sim — Prefect 3.

Migrado pro pipeline em estágios via `CheckThenExtractLoadPipeline`.
`extract_and_load` usa `run_extract_and_load_with_derived_tables` em vez
de `run_extract_and_load` — motivo completo no docstring daquele método
(`pipelines/utils/stage_dispatch.py`): 4 tabelas agregadas
(`constants.AGGREGATE_TABLES`) não têm extração própria, só `dbt run/test`
lendo do `microdados` recém-carregado.

O `dicionario` (`models/br_ms_sim/br_ms_sim__dicionario.sql`) fica de fora
de propósito — tem ciclo de materialização próprio, não depende do
`microdados` a cada carga.
"""

from pipelines.datasets.br_ms_sim.constants import (
    AGGREGATE_COVERAGE,
    AGGREGATE_TABLES,
    COVERAGE,
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_ms_sim.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import DateFormat
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_TABLE_ID,
    prefect_dataset_id=DATASET_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
    date_format=DateFormat.YEAR,
)


@flow(name=_pipeline.check_update_flow_name, log_prints=True)
def br_ms_sim_check_update() -> bool:
    """Poll do ano mais recente na fonte, dispara extract_and_load."""
    return _pipeline.run_check_update()


br_ms_sim_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# Sem deploy_schedules: o flow monolítico antigo também não tinha cron —
# mesmo comportamento preservado (disparo manual/externo).


@flow(name=_pipeline.extract_and_load_flow_name, log_prints=True)
def br_ms_sim_extract_and_load(download_params: dict) -> None:
    """Baixa o ano do `reference_date` e materializa microdados + agregadas.

    Args:
        download_params: recebido do check_update via `run_deployment()` —
            usa `reference_date` pra decidir o ano a baixar. Também aceita
            disparo manual direto (ex. `{"reference_date": "2015-01-01"}`)
            pra reprocessar um ano específico, no lugar do antigo parâmetro
            `ano` do flow monolítico.
    """
    _pipeline.run_extract_and_load_with_derived_tables(
        download_params,
        coverage=COVERAGE,
        derived_table_ids=AGGREGATE_TABLES,
        derived_coverage=AGGREGATE_COVERAGE,
    )


br_ms_sim_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# `memory` não existe no template do work pool, que só conhece o par abaixo, e
# chave fora do template é descartada em silêncio — o pod ficaria no padrão.
br_ms_sim_extract_and_load.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
_pipeline.extract_load_deployment = br_ms_sim_extract_and_load.fn.__name__
