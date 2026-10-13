"""
Flows para br_anp_precos_combustiveis — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Lógica específica do
dataset mora em `tasks.py`, constantes em `constants.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).

Dataset com uma única tabela (`microdados`) — instância direta de
`CheckThenExtractLoadPipeline`, sem fábrica. A lógica de download/transform
(`get_data_source_anp_max_date`, `download_and_transform`, `make_partitions`)
segue existindo em `pipelines/crawler/anp_precos_combustiveis/` —
reaproveitada por `tasks.py`, não reescrita.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_anp_precos_combustiveis.tasks import (
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
# microdados
# check_update: br_anp_precos_combustiveis__microdados
# extract_and_load: br_anp_precos_combustiveis__microdados
# ──────────────────────────────────────────────────────────────────────────────

_microdados_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=DATASET_ID,
    table_id=MICRODADOS_TABLE_ID,
    get_latest_update=get_latest_update,
    extract_load_data=extract_load_data,
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_anp_precos_combustiveis_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_anp_precos_combustiveis_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo.
br_anp_precos_combustiveis_microdados_check_update.deploy_schedules = [
    Cron("0 10 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_anp_precos_combustiveis_microdados_download(
    download_params: dict,
) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_anp_precos_combustiveis_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_anp_precos_combustiveis_microdados_download.fn.__name__
)
