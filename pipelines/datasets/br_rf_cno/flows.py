"""Flows for br_rf_cno (Cadastro Nacional de Obras — Receita Federal) —
Prefect 3.

Migrado pro padrão de 1 flow por tabela: check_update -> extract_and_load ->
build_and_promote, uma dupla de flows por tabela
(`CheckThenExtractLoadPipeline` via `pipeline_factory`, ver tasks.py) — mesma
estrutura de `br_ibge_ipca`/`br_ms_cnes`.

A fonte publica um único `cno.zip` (~306 MB) não decomponível que gera as 4
tabelas publicadas (microdados, vinculos, areas, cnaes) de uma vez (ver
README/constants.py). Isso já tinha sido migrado com 1 check_update + 1
extract_and_load pro dataset inteiro, pra evitar redownload redundante do
zip — mas a decisão mudou: a regra de 1 flow por tabela vale mesmo aqui,
aceitando o redownload repetido (~306 MB x4 por ciclo), já que as 4 tabelas
não dependem de verdade umas das outras (nenhum teste dbt cross-table as
amarra).

check_update lê o header `Last-Modified` do zip (cheap, sem download) e
compara contra a `Coverage` da própria tabela, só disparando
extract_and_load — que baixa o zip inteiro de novo — quando a fonte
realmente tem dado novo.

Deploy: `.github/workflows/scripts/deploy_flows.py` descobre os 8 flows
abaixo automaticamente; o pool de dev ignora os schedules de check_update, o
de prod os ativa (deployed paused).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_rf_cno.constants import (
    AREAS_TABLE_ID,
    CNAES_TABLE_ID,
    DATASET_ID,
    MICRODADOS_TABLE_ID,
    VINCULOS_TABLE_ID,
)
from pipelines.datasets.br_rf_cno.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# microdados
# check_update: br_rf_cno__microdados
# extract_and_load: br_rf_cno__microdados
# ──────────────────────────────────────────────────────────────────────────────

_microdados_pipeline = make_pipeline(MICRODADOS_TABLE_ID)


@flow(name=_microdados_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_microdados_check_update() -> None:
    _microdados_pipeline.run_check_update()


br_rf_cno_microdados_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, MICRODADOS_TABLE_ID
)
# Mesmo cron do antigo flow por tabela, de antes do download compartilhado
# (ver README's *Schedules*).
br_rf_cno_microdados_check_update.deploy_schedules = [
    Cron("5 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_microdados_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_microdados_download(download_params: dict) -> None:
    _microdados_pipeline.run_extract_and_load(download_params)


br_rf_cno_microdados_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, MICRODADOS_TABLE_ID
)
_microdados_pipeline.extract_load_deployment = (
    br_rf_cno_microdados_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# vinculos
# check_update: br_rf_cno__vinculos
# extract_and_load: br_rf_cno__vinculos
# ──────────────────────────────────────────────────────────────────────────────

_vinculos_pipeline = make_pipeline(VINCULOS_TABLE_ID)


@flow(name=_vinculos_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_vinculos_check_update() -> None:
    _vinculos_pipeline.run_check_update()


br_rf_cno_vinculos_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, VINCULOS_TABLE_ID
)
# Mesmo cron do antigo flow por tabela, de antes do download compartilhado
# (ver README's *Schedules*).
br_rf_cno_vinculos_check_update.deploy_schedules = [
    Cron("15 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_vinculos_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_vinculos_download(download_params: dict) -> None:
    _vinculos_pipeline.run_extract_and_load(download_params)


br_rf_cno_vinculos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, VINCULOS_TABLE_ID
)
_vinculos_pipeline.extract_load_deployment = (
    br_rf_cno_vinculos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# areas
# check_update: br_rf_cno__areas
# extract_and_load: br_rf_cno__areas
# ──────────────────────────────────────────────────────────────────────────────

_areas_pipeline = make_pipeline(AREAS_TABLE_ID)


@flow(name=_areas_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_areas_check_update() -> None:
    _areas_pipeline.run_check_update()


br_rf_cno_areas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, AREAS_TABLE_ID
)
# Mesmo cron do antigo flow por tabela, de antes do download compartilhado
# (ver README's *Schedules*).
br_rf_cno_areas_check_update.deploy_schedules = [
    Cron("25 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_areas_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_areas_download(download_params: dict) -> None:
    _areas_pipeline.run_extract_and_load(download_params)


br_rf_cno_areas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, AREAS_TABLE_ID
)
_areas_pipeline.extract_load_deployment = br_rf_cno_areas_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# cnaes
# check_update: br_rf_cno__cnaes
# extract_and_load: br_rf_cno__cnaes
# ──────────────────────────────────────────────────────────────────────────────

_cnaes_pipeline = make_pipeline(CNAES_TABLE_ID)


@flow(name=_cnaes_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cno_cnaes_check_update() -> None:
    _cnaes_pipeline.run_check_update()


br_rf_cno_cnaes_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CNAES_TABLE_ID
)
# Mesmo cron do antigo flow por tabela, de antes do download compartilhado
# (ver README's *Schedules*).
br_rf_cno_cnaes_check_update.deploy_schedules = [
    Cron("35 4 * * 1-5", timezone="America/Sao_Paulo")
]


@flow(name=_cnaes_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cno_cnaes_download(download_params: dict) -> None:
    _cnaes_pipeline.run_extract_and_load(download_params)


br_rf_cno_cnaes_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CNAES_TABLE_ID
)
_cnaes_pipeline.extract_load_deployment = br_rf_cno_cnaes_download.fn.__name__
