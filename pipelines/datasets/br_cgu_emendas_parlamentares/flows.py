"""
Flows para br_cgu_emendas_parlamentares — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote. Uma única tabela
(`microdados`). Lógica específica do dataset mora em `tasks.py`, constantes
em `constants.py` — aqui só a fiação (`CheckThenExtractLoadPipeline` +
`@flow`).

A fonte não expõe nenhuma data de última atualização — `get_latest_update`
precisa baixar o .zip inteiro e comparar nº de linhas com a tabela em prod
pra decidir se há dado novo (ver banner de `tasks.py`); `extract_and_load`
baixa de novo, de forma independente.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cgu_emendas_parlamentares.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_cgu_emendas_parlamentares.tasks import (
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
)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_cgu_emendas_parlamentares_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_cgu_emendas_parlamentares_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo (main).
br_cgu_emendas_parlamentares_microdados_check_update.deploy_schedules = [
    Cron("30 19 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cgu_emendas_parlamentares_microdados_download(
    download_params: dict,
) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_cgu_emendas_parlamentares_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_cgu_emendas_parlamentares_microdados_download.fn.__name__
)
