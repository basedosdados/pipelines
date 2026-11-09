"""Flows do br_fnde_fundeb — Prefect 3.

Migrado por completo pro pipeline em estágios (staged pipeline):
check_update -> extract_and_load -> build_and_promote, uma dupla de flows por
tabela (indicador_estadual, indicador_municipal). Lógica específica do
dataset mora em `tasks.py`/`utils.py`, constantes em `constants.py` — aqui só
a fiação (`CheckThenExtractLoadPipeline` + `@flow`).

O `dicionario` não entra neste flow — as 112 linhas são fixas e carregadas à
parte, manualmente (ver README do conjunto, seção "Carga e atualização").
Embora `custom_dictionary_coverage` em `indicador_estadual`/
`indicador_municipal` referencie `ref('br_fnde_fundeb__dicionario')`, essa
tabela não é produzida por este pipeline — já existe, persistente, em dev e
prod, carregada antes de este flow existir — então o `build_and_promote`
genérico (um deployment por tabela, assíncrono) não corre o risco de testar
`indicador_estadual`/`indicador_municipal` antes de `dicionario` existir, ao
contrário do caso de `br_bcb_ifdata` (onde as 4 tabelas, dicionário incluso,
são recriadas juntas a cada run).
"""

from prefect.schedules import Cron

from pipelines.datasets.br_fnde_fundeb.constants import constants
from pipelines.datasets.br_fnde_fundeb.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value
TABLE_STATE = constants.TABLE_STATE.value
TABLE_MUNICIPALITY = constants.TABLE_MUNICIPALITY.value

# ──────────────────────────────────────────────────────────────────────────────
# indicador_estadual
# check_update: br_fnde_fundeb__indicador_estadual
# extract_and_load: br_fnde_fundeb__indicador_estadual
# ──────────────────────────────────────────────────────────────────────────────

_indicador_estadual_pipeline = make_pipeline(TABLE_STATE)


@flow(
    name=_indicador_estadual_pipeline.check_update_flow_name, log_prints=True
)
def br_fnde_fundeb_indicador_estadual_check_update() -> None:
    _indicador_estadual_pipeline.run_check_update()


br_fnde_fundeb_indicador_estadual_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, TABLE_STATE
)
# Mesmo cron do flow monolítico antigo (main): dias 5, 12, 19 e 26.
br_fnde_fundeb_indicador_estadual_check_update.deploy_schedules = [
    Cron("17 10 5,12,19,26 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_indicador_estadual_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_fnde_fundeb_indicador_estadual_download(download_params: dict) -> None:
    _indicador_estadual_pipeline.run_extract_and_load(download_params)


br_fnde_fundeb_indicador_estadual_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, TABLE_STATE
)
_indicador_estadual_pipeline.extract_load_deployment = (
    br_fnde_fundeb_indicador_estadual_download.fn.__name__
)
# Mesma folga de memória do flow monolítico antigo — o histórico (não baixado
# aqui) passa de 1 GB descomprimido; o exercício corrente é bem menor.
br_fnde_fundeb_indicador_estadual_download.job_variables = {"memory": "4Gi"}


# ──────────────────────────────────────────────────────────────────────────────
# indicador_municipal
# check_update: br_fnde_fundeb__indicador_municipal
# extract_and_load: br_fnde_fundeb__indicador_municipal
# ──────────────────────────────────────────────────────────────────────────────

_indicador_municipal_pipeline = make_pipeline(TABLE_MUNICIPALITY)


@flow(
    name=_indicador_municipal_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_fnde_fundeb_indicador_municipal_check_update() -> None:
    _indicador_municipal_pipeline.run_check_update()


br_fnde_fundeb_indicador_municipal_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, TABLE_MUNICIPALITY
)
# Mesmo cron do flow monolítico antigo (main): dias 5, 12, 19 e 26.
br_fnde_fundeb_indicador_municipal_check_update.deploy_schedules = [
    Cron("17 10 5,12,19,26 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_indicador_municipal_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_fnde_fundeb_indicador_municipal_download(
    download_params: dict,
) -> None:
    _indicador_municipal_pipeline.run_extract_and_load(download_params)


br_fnde_fundeb_indicador_municipal_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, TABLE_MUNICIPALITY
)
_indicador_municipal_pipeline.extract_load_deployment = (
    br_fnde_fundeb_indicador_municipal_download.fn.__name__
)
# Mesma folga de memória do flow monolítico antigo — o histórico (não baixado
# aqui) passa de 1 GB descomprimido; o exercício corrente é bem menor.
br_fnde_fundeb_indicador_municipal_download.job_variables = {"memory": "4Gi"}
