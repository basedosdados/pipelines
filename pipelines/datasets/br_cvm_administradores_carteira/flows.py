"""
Flows para br_cvm_administradores_carteira — Prefect 3.

Migrado pro pipeline em estágios (staged pipeline): check_update ->
extract_and_load -> build_and_promote, uma dupla de flows por tabela
(responsavel, pessoa_fisica, pessoa_juridica — sem dependência real de
ordem entre elas no dbt, nenhum `ref()` cruzado entre as próprias tabelas
do dataset em `models/br_cvm_administradores_carteira/*.sql`).

Check diferente do padrão usual — ver `tasks.py` e
`ftwca/investigacoes/pipelines/backlog.md`: a fonte é uma URL estática
única da CVM, sem nenhuma data exposta pra comparar. O check baixa o ZIP e
compara a contagem de linhas contra o que já está publicado no BigQuery.

O antigo flow monolítico `_run_cvm_administradores_carteira` (e a pasta
`pipelines/crawler/cvm_administradores_carteira/` como um todo) foi
removido: estava obsoleto desde esta migração pro pipeline em estágios e
não tinha mais nenhum consumidor.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_cvm_administradores_carteira.constants import (
    DATASET_ID,
    PESSOA_FISICA_TABLE_ID,
    PESSOA_JURIDICA_TABLE_ID,
    RESPONSAVEL_TABLE_ID,
)
from pipelines.datasets.br_cvm_administradores_carteira.tasks import (
    make_pipeline,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# responsavel
# ──────────────────────────────────────────────────────────────────────────────

_responsavel_pipeline = make_pipeline(RESPONSAVEL_TABLE_ID)


@flow(name=_responsavel_pipeline.check_update_flow_name, log_prints=True)
def br_cvm_administradores_carteira_responsavel_check_update() -> None:
    _responsavel_pipeline.run_check_update()


br_cvm_administradores_carteira_responsavel_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, RESPONSAVEL_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo.
br_cvm_administradores_carteira_responsavel_check_update.deploy_schedules = [
    Cron("12 6 * * 1-5", timezone="America/Sao_Paulo")
]
# Tabelas minúsculas (milhares de linhas, poucos MB) — 2Gi sobra, não precisa
# do default de 4Gi do work pool.
br_cvm_administradores_carteira_responsavel_check_update.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}


@flow(name=_responsavel_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cvm_administradores_carteira_responsavel_download(
    download_params: dict,
) -> None:
    _responsavel_pipeline.run_extract_and_load(download_params)


br_cvm_administradores_carteira_responsavel_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, RESPONSAVEL_TABLE_ID
)
br_cvm_administradores_carteira_responsavel_download.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}
_responsavel_pipeline.extract_load_deployment = (
    br_cvm_administradores_carteira_responsavel_download.fn.__name__
)

# ──────────────────────────────────────────────────────────────────────────────
# pessoa_fisica
# ──────────────────────────────────────────────────────────────────────────────

_pessoa_fisica_pipeline = make_pipeline(PESSOA_FISICA_TABLE_ID)


@flow(name=_pessoa_fisica_pipeline.check_update_flow_name, log_prints=True)
def br_cvm_administradores_carteira_pessoa_fisica_check_update() -> None:
    _pessoa_fisica_pipeline.run_check_update()


br_cvm_administradores_carteira_pessoa_fisica_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, PESSOA_FISICA_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo.
br_cvm_administradores_carteira_pessoa_fisica_check_update.deploy_schedules = [
    Cron("50 6 * * 1-5", timezone="America/Sao_Paulo")
]
# Tabelas minúsculas (milhares de linhas, poucos MB) — 2Gi sobra, não precisa
# do default de 4Gi do work pool.
br_cvm_administradores_carteira_pessoa_fisica_check_update.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}


@flow(name=_pessoa_fisica_pipeline.extract_and_load_flow_name, log_prints=True)
def br_cvm_administradores_carteira_pessoa_fisica_download(
    download_params: dict,
) -> None:
    _pessoa_fisica_pipeline.run_extract_and_load(download_params)


br_cvm_administradores_carteira_pessoa_fisica_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, PESSOA_FISICA_TABLE_ID)
)
br_cvm_administradores_carteira_pessoa_fisica_download.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}
_pessoa_fisica_pipeline.extract_load_deployment = (
    br_cvm_administradores_carteira_pessoa_fisica_download.fn.__name__
)

# ──────────────────────────────────────────────────────────────────────────────
# pessoa_juridica
# ──────────────────────────────────────────────────────────────────────────────

_pessoa_juridica_pipeline = make_pipeline(PESSOA_JURIDICA_TABLE_ID)


@flow(name=_pessoa_juridica_pipeline.check_update_flow_name, log_prints=True)
def br_cvm_administradores_carteira_pessoa_juridica_check_update() -> None:
    _pessoa_juridica_pipeline.run_check_update()


br_cvm_administradores_carteira_pessoa_juridica_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, PESSOA_JURIDICA_TABLE_ID)
)
# Mesmo cron do flow monolítico antigo.
br_cvm_administradores_carteira_pessoa_juridica_check_update.deploy_schedules = [
    Cron("0 6 * * 1-5", timezone="America/Sao_Paulo")
]
# Tabelas minúsculas (milhares de linhas, poucos MB) — 2Gi sobra, não precisa
# do default de 4Gi do work pool.
br_cvm_administradores_carteira_pessoa_juridica_check_update.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}


@flow(
    name=_pessoa_juridica_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_cvm_administradores_carteira_pessoa_juridica_download(
    download_params: dict,
) -> None:
    _pessoa_juridica_pipeline.run_extract_and_load(download_params)


br_cvm_administradores_carteira_pessoa_juridica_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, PESSOA_JURIDICA_TABLE_ID)
)
br_cvm_administradores_carteira_pessoa_juridica_download.job_variables = {
    "memory_limit": "2Gi",
    "memory_request": "1Gi",
}
_pessoa_juridica_pipeline.extract_load_deployment = (
    br_cvm_administradores_carteira_pessoa_juridica_download.fn.__name__
)
