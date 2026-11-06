"""
Flows de br_ms_sinasc — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`/`utils.py`, constantes em `constants.py` — aqui só
a fiação (`CheckThenExtractLoadPipeline` + `@flow`).
"""

from pipelines.datasets.br_ms_sinasc.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_ms_sinasc.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

_microdados_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
    # Cobertura anual (`YearOnly`) — mesma granularidade do flow antigo.
    date_format="%Y",
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_ms_sinasc_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_ms_sinasc_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Sem schedule, igual o flow monolítico antigo: o DATASUS republica o SINASC
# uma vez por ano, sem data fixa, e revisa anos já fechados (ver README.md —
# "Sem schedule, por quê"). `deploy_schedules` fica `None`.


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_ms_sinasc_microdados_download(download_params: dict) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_ms_sinasc_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_ms_sinasc_microdados_download.fn.__name__
)

# `memory` não existe no template do work pool, que só conhece o par abaixo, e
# chave fora do template é descartada em silêncio — o pod ficaria no padrão.
# Isolado na etapa de download (parte pesada): check_update só lista o FTP.
br_ms_sinasc_microdados_download.job_variables = {
    "memory_limit": "8Gi",
    "memory_request": "2Gi",
}
