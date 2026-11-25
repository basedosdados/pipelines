"""
Flows para br_cgu_pessoal_executivo_federal — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).

Dataset com uma única tabela (`terceirizados`) — instância direta de
`CheckThenExtractLoadPipeline`, sem fábrica.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_pessoal_executivo_federal.constants import (
    DATASET_ID,
    TERCEIRIZADOS_TABLE_ID,
)
from pipelines.datasets.br_cgu_pessoal_executivo_federal.tasks import (
    extract_load_data,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)

# ──────────────────────────────────────────────────────────────────────────────
# terceirizados
# check_update: br_cgu_pessoal_executivo_federal__terceirizados
# extract_and_load: br_cgu_pessoal_executivo_federal__terceirizados
# ──────────────────────────────────────────────────────────────────────────────

_terceirizados_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=TERCEIRIZADOS_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_terceirizados_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_pessoal_executivo_federal_terceirizados_check_update() -> None:
    _terceirizados_pipeline.run_check_update()


br_cgu_pessoal_executivo_federal_terceirizados_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, TERCEIRIZADOS_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo.
br_cgu_pessoal_executivo_federal_terceirizados_check_update.deploy_schedules = [
    Cron("0 0 28 2/4 *", timezone="America/Sao_Paulo")
]


@flow(
    name=_terceirizados_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cgu_pessoal_executivo_federal_terceirizados_download(
    download_params: dict,
) -> None:
    _terceirizados_pipeline.run_extract_and_load(download_params)


br_cgu_pessoal_executivo_federal_terceirizados_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, TERCEIRIZADOS_TABLE_ID)
)
_terceirizados_pipeline.extract_load_deployment = (
    br_cgu_pessoal_executivo_federal_terceirizados_download.fn.__name__
)
