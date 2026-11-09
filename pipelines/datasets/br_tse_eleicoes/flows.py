"""
Flows para br_tse_eleicoes — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, um par de flows por
tabela (candidatos, bens_candidato, despesas_candidato, receitas_candidato) —
sem dependência real entre elas (ver banner em tasks.py), então a regra padrão
de 1 flow por tabela se aplica sem exceção.

check_update baixa o ZIP da fonte pra `YEAR` (constants.py) e compara a
contagem de linhas contra o que já está em prod pra aquele ano
(`utils.BrTseEleicoes.get_data_source_max_date`) — reusa `Table.Update.latest`
como alvo de comparação (`compare_against="table_update"`), não `Coverage`,
porque o valor comparado é um timestamp de execução
(`datetime.today()`/`last_modified`), não uma competência. extract_and_load
refaz o download de forma independente (custo duplicado aceito) e formata os
dados; a materialização (dbt run/test dev+prod, registro de cobertura) fica a
cargo do `build_and_promote` genérico, disparado por
`CheckThenExtractLoadPipeline`.

`YEAR` precisa ser atualizado manualmente a cada novo ciclo eleitoral — mesma
manutenção manual que o flow monolítico antigo já exigia (lá, o default do
parâmetro `year` do flow).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_tse_eleicoes.constants import DATASET_ID
from pipelines.datasets.br_tse_eleicoes.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# candidatos
# check_update: br_tse_eleicoes__candidatos
# extract_and_load: br_tse_eleicoes__candidatos
# ──────────────────────────────────────────────────────────────────────────────

CANDIDATOS_TABLE_ID = "candidatos"
_candidatos_pipeline = make_pipeline(CANDIDATOS_TABLE_ID)


@flow(name=_candidatos_pipeline.check_update_flow_name, log_prints=True)
def br_tse_eleicoes_candidatos_check_update() -> None:
    _candidatos_pipeline.run_check_update()


br_tse_eleicoes_candidatos_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, CANDIDATOS_TABLE_ID
)
# Mesmo cron do flow monolítico antigo.
br_tse_eleicoes_candidatos_check_update.deploy_schedules = [
    Cron("0 5 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_candidatos_pipeline.extract_and_load_flow_name, log_prints=True)
def br_tse_eleicoes_candidatos_download(download_params: dict) -> None:
    _candidatos_pipeline.run_extract_and_load(download_params)


br_tse_eleicoes_candidatos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, CANDIDATOS_TABLE_ID
)
_candidatos_pipeline.extract_load_deployment = (
    br_tse_eleicoes_candidatos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# bens_candidato
# check_update: br_tse_eleicoes__bens_candidato
# extract_and_load: br_tse_eleicoes__bens_candidato
# ──────────────────────────────────────────────────────────────────────────────

BENS_CANDIDATO_TABLE_ID = "bens_candidato"
_bens_candidato_pipeline = make_pipeline(BENS_CANDIDATO_TABLE_ID)


@flow(name=_bens_candidato_pipeline.check_update_flow_name, log_prints=True)
def br_tse_eleicoes_bens_candidato_check_update() -> None:
    _bens_candidato_pipeline.run_check_update()


br_tse_eleicoes_bens_candidato_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, BENS_CANDIDATO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo.
br_tse_eleicoes_bens_candidato_check_update.deploy_schedules = [
    Cron("30 5 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_bens_candidato_pipeline.extract_and_load_flow_name, log_prints=True
)
def br_tse_eleicoes_bens_candidato_download(download_params: dict) -> None:
    _bens_candidato_pipeline.run_extract_and_load(download_params)


br_tse_eleicoes_bens_candidato_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, BENS_CANDIDATO_TABLE_ID
)
_bens_candidato_pipeline.extract_load_deployment = (
    br_tse_eleicoes_bens_candidato_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# despesas_candidato
# check_update: br_tse_eleicoes__despesas_candidato
# extract_and_load: br_tse_eleicoes__despesas_candidato
# ──────────────────────────────────────────────────────────────────────────────

DESPESAS_CANDIDATO_TABLE_ID = "despesas_candidato"
_despesas_candidato_pipeline = make_pipeline(DESPESAS_CANDIDATO_TABLE_ID)


@flow(
    name=_despesas_candidato_pipeline.check_update_flow_name, log_prints=True
)
def br_tse_eleicoes_despesas_candidato_check_update() -> None:
    _despesas_candidato_pipeline.run_check_update()


br_tse_eleicoes_despesas_candidato_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, DESPESAS_CANDIDATO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo.
br_tse_eleicoes_despesas_candidato_check_update.deploy_schedules = [
    Cron("0 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_despesas_candidato_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_tse_eleicoes_despesas_candidato_download(
    download_params: dict,
) -> None:
    _despesas_candidato_pipeline.run_extract_and_load(download_params)


br_tse_eleicoes_despesas_candidato_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, DESPESAS_CANDIDATO_TABLE_ID
)
_despesas_candidato_pipeline.extract_load_deployment = (
    br_tse_eleicoes_despesas_candidato_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# receitas_candidato
# check_update: br_tse_eleicoes__receitas_candidato
# extract_and_load: br_tse_eleicoes__receitas_candidato
# ──────────────────────────────────────────────────────────────────────────────

RECEITAS_CANDIDATO_TABLE_ID = "receitas_candidato"
_receitas_candidato_pipeline = make_pipeline(RECEITAS_CANDIDATO_TABLE_ID)


@flow(
    name=_receitas_candidato_pipeline.check_update_flow_name, log_prints=True
)
def br_tse_eleicoes_receitas_candidato_check_update() -> None:
    _receitas_candidato_pipeline.run_check_update()


br_tse_eleicoes_receitas_candidato_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, RECEITAS_CANDIDATO_TABLE_ID
)
# Mesmo cron do flow monolítico antigo.
br_tse_eleicoes_receitas_candidato_check_update.deploy_schedules = [
    Cron("30 6 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_receitas_candidato_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_tse_eleicoes_receitas_candidato_download(
    download_params: dict,
) -> None:
    _receitas_candidato_pipeline.run_extract_and_load(download_params)


br_tse_eleicoes_receitas_candidato_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, RECEITAS_CANDIDATO_TABLE_ID
)
_receitas_candidato_pipeline.extract_load_deployment = (
    br_tse_eleicoes_receitas_candidato_download.fn.__name__
)
