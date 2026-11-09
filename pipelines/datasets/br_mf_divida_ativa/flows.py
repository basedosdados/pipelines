"""
Flows para br_mf_divida_ativa — Prefect 3.

PGFN "Dados Abertos da Dívida Ativa da União": three quarterly tables (SIDA /
previdenciário / FGTS). Migrado por completo pro pipeline em estágios (staged
pipeline): check_update -> extract_and_load -> build_and_promote, uma dupla de
flows por tabela. Cada release adiciona UM trimestre novo como snapshot
imutável — é **incremental append**, não full replace; cada
``extract_and_load`` só baixa os trimestres mais novos que a Coverage já
registrada DAQUELA tabela (catch-up — ver ``tasks.py``), particionados por
ano/trimestre. Lógica específica do dataset mora em ``tasks.py``
(reaproveitando ``utils.py``, inalterado na essência), constantes em
``constants.py`` — aqui só a fiação (``CheckThenExtractLoadPipeline`` + `@flow`).

O antigo ``br_mf_divida_ativa_flow`` monolítico (check+download+upload+dbt num
só flow) é substituído por completo por este módulo.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_mf_divida_ativa.constants import (
    FGTS_TABLE_ID,
    NAO_PREVIDENCIARIO_TABLE_ID,
    PREVIDENCIARIO_TABLE_ID,
    constants,
)
from pipelines.datasets.br_mf_divida_ativa.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value
# PGFN republishes quarterly on no fixed day; poll a few days each month at
# 15:00 BRT — mesmo cron do flow monolítico antigo. O check_update no-opa até
# um trimestre genuinamente novo aparecer, então rodar o mesmo cron nas 3
# tabelas é barato (cada uma só sonda via HEAD, não baixa nada).
_SCHEDULE = [Cron(constants.SCHEDULE_CRON.value, timezone="America/Sao_Paulo")]

# ──────────────────────────────────────────────────────────────────────────────
# nao_previdenciario (SIDA)
# check_update: br_mf_divida_ativa__nao_previdenciario
# extract_and_load: br_mf_divida_ativa__nao_previdenciario
# ──────────────────────────────────────────────────────────────────────────────

_nao_previdenciario_pipeline = make_pipeline(NAO_PREVIDENCIARIO_TABLE_ID)


@flow(
    name=_nao_previdenciario_pipeline.check_update_flow_name, log_prints=True
)
def br_mf_divida_ativa_nao_previdenciario_check_update() -> None:
    _nao_previdenciario_pipeline.run_check_update()


br_mf_divida_ativa_nao_previdenciario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, NAO_PREVIDENCIARIO_TABLE_ID
)
br_mf_divida_ativa_nao_previdenciario_check_update.deploy_schedules = _SCHEDULE


@flow(
    name=_nao_previdenciario_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_mf_divida_ativa_nao_previdenciario_download(
    download_params: dict,
) -> None:
    _nao_previdenciario_pipeline.run_extract_and_load(download_params)


br_mf_divida_ativa_nao_previdenciario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, NAO_PREVIDENCIARIO_TABLE_ID
)
_nao_previdenciario_pipeline.extract_load_deployment = (
    br_mf_divida_ativa_nao_previdenciario_download.fn.__name__
)
# SIDA é ~40-50M linhas/trimestre, processadas em chunks de 400k — pico de RAM
# modesto, mas dá folga pros buffers pandas->arrow + upload GCS (igual ao
# job_variables do flow monolítico antigo).
br_mf_divida_ativa_nao_previdenciario_download.job_variables = {
    "memory": "8Gi"
}


# ──────────────────────────────────────────────────────────────────────────────
# previdenciario
# check_update: br_mf_divida_ativa__previdenciario
# extract_and_load: br_mf_divida_ativa__previdenciario
# ──────────────────────────────────────────────────────────────────────────────

_previdenciario_pipeline = make_pipeline(PREVIDENCIARIO_TABLE_ID)


@flow(name=_previdenciario_pipeline.check_update_flow_name, log_prints=True)
def br_mf_divida_ativa_previdenciario_check_update() -> None:
    _previdenciario_pipeline.run_check_update()


br_mf_divida_ativa_previdenciario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, PREVIDENCIARIO_TABLE_ID
)
br_mf_divida_ativa_previdenciario_check_update.deploy_schedules = _SCHEDULE


@flow(
    name=_previdenciario_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_mf_divida_ativa_previdenciario_download(download_params: dict) -> None:
    _previdenciario_pipeline.run_extract_and_load(download_params)


br_mf_divida_ativa_previdenciario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, PREVIDENCIARIO_TABLE_ID
)
_previdenciario_pipeline.extract_load_deployment = (
    br_mf_divida_ativa_previdenciario_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# fgts
# check_update: br_mf_divida_ativa__fgts
# extract_and_load: br_mf_divida_ativa__fgts
# ──────────────────────────────────────────────────────────────────────────────

_fgts_pipeline = make_pipeline(FGTS_TABLE_ID)


@flow(name=_fgts_pipeline.check_update_flow_name, log_prints=True)
def br_mf_divida_ativa_fgts_check_update() -> None:
    _fgts_pipeline.run_check_update()


br_mf_divida_ativa_fgts_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, FGTS_TABLE_ID
)
br_mf_divida_ativa_fgts_check_update.deploy_schedules = _SCHEDULE


@flow(name=_fgts_pipeline.extract_and_load_flow_name, log_prints=True)
def br_mf_divida_ativa_fgts_download(download_params: dict) -> None:
    _fgts_pipeline.run_extract_and_load(download_params)


br_mf_divida_ativa_fgts_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, FGTS_TABLE_ID
)
_fgts_pipeline.extract_load_deployment = (
    br_mf_divida_ativa_fgts_download.fn.__name__
)
