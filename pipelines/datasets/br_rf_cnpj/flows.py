"""
Flows para br_rf_cnpj — Prefect 3.

Migrado pro pipeline em estágios (staged pipeline): check_update ->
extract_and_load -> build_and_promote, uma dupla de flows por tabela
(empresas, socios, estabelecimentos, simples, dicionario). Lógica
específica do dataset mora em `tasks.py` — aqui só a fiação
(`CheckThenExtractLoadPipeline` + `@flow`).

NOTA (gap de migração — não resolvido aqui): o flow monolítico antigo, só
pra `estabelecimentos`, também atualizava `br_bd_diretorios_brasil.empresa`
(`run_dbt` + `download_data_to_gcs` + `register_table_materialization_task`
com `AllBdpro`) logo depois de promover `estabelecimentos` pra prod — e
`br_bd_diretorios_brasil` não tem nenhum flow próprio que faça isso. Nesta
arquitetura, essa promoção acontece dentro do `build_and_promote` genérico
(`pipelines/utils/metadata/flows.py`), despachado de forma assíncrona
(`run_deployment(..., timeout=0)`) só depois que `extract_and_load`
termina — não existe mais, dentro de `br_rf_cnpj`, um ponto síncrono de
onde disparar esse refresh depois que a promoção de verdade aconteceu
(rodar o refresh durante `extract_and_load` leria `estabelecimentos` antes
dele estar materializado em prod). Reproduzir isso direito exige mexer no
`build_and_promote` compartilhado (afeta todo dataset já migrado) ou dar a
`br_bd_diretorios_brasil` seu próprio check_update orientado pelo
`Table.Update` de `br_rf_cnpj.estabelecimentos` — ambos fora do escopo
desta migração (só arquivos de `br_rf_cnpj`). Esse refresh NÃO foi
reproduzido aqui.
"""

from prefect.schedules import Cron

from pipelines.datasets.br_rf_cnpj.tasks import DATASET_ID, make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# dicionario
# check_update: br_rf_cnpj__dicionario
# extract_and_load: br_rf_cnpj__dicionario
# ──────────────────────────────────────────────────────────────────────────────

_dicionario_pipeline = make_pipeline("dicionario")


@flow(name=_dicionario_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_dicionario_check_update() -> None:
    _dicionario_pipeline.run_check_update()


br_rf_cnpj_dicionario_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "dicionario"
)
# Mesmo cron do flow monolítico antigo.
br_rf_cnpj_dicionario_check_update.deploy_schedules = [
    Cron("35 14 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_dicionario_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_dicionario_download(download_params: dict) -> None:
    _dicionario_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_dicionario_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "dicionario"
)
_dicionario_pipeline.extract_load_deployment = (
    br_rf_cnpj_dicionario_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# empresas
# check_update: br_rf_cnpj__empresas
# extract_and_load: br_rf_cnpj__empresas
# ──────────────────────────────────────────────────────────────────────────────

_empresas_pipeline = make_pipeline("empresas")


@flow(name=_empresas_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_empresas_check_update() -> None:
    _empresas_pipeline.run_check_update()


br_rf_cnpj_empresas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "empresas"
)
# Mesmo cron do flow monolítico antigo.
br_rf_cnpj_empresas_check_update.deploy_schedules = [
    Cron("0 6 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_empresas_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_empresas_download(download_params: dict) -> None:
    _empresas_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_empresas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "empresas"
)
_empresas_pipeline.extract_load_deployment = (
    br_rf_cnpj_empresas_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# socios
# check_update: br_rf_cnpj__socios
# extract_and_load: br_rf_cnpj__socios
# ──────────────────────────────────────────────────────────────────────────────

_socios_pipeline = make_pipeline("socios")


@flow(name=_socios_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_socios_check_update() -> None:
    _socios_pipeline.run_check_update()


br_rf_cnpj_socios_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "socios"
)
# Mesmo cron do flow monolítico antigo.
br_rf_cnpj_socios_check_update.deploy_schedules = [
    Cron("0 7 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_socios_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_socios_download(download_params: dict) -> None:
    _socios_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_socios_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "socios"
)
_socios_pipeline.extract_load_deployment = (
    br_rf_cnpj_socios_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# simples
# check_update: br_rf_cnpj__simples
# extract_and_load: br_rf_cnpj__simples
# ──────────────────────────────────────────────────────────────────────────────

_simples_pipeline = make_pipeline("simples")


@flow(name=_simples_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_simples_check_update() -> None:
    _simples_pipeline.run_check_update()


br_rf_cnpj_simples_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "simples"
)
# Mesmo cron do flow monolítico antigo.
br_rf_cnpj_simples_check_update.deploy_schedules = [
    Cron("0 8 * * *", timezone="America/Sao_Paulo")
]


@flow(name=_simples_pipeline.extract_and_load_flow_name, log_prints=True)
def br_rf_cnpj_simples_download(download_params: dict) -> None:
    _simples_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_simples_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "simples"
)
_simples_pipeline.extract_load_deployment = (
    br_rf_cnpj_simples_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimentos
# check_update: br_rf_cnpj__estabelecimentos
# extract_and_load: br_rf_cnpj__estabelecimentos
#
# Ver nota no topo do arquivo: o refresh de `br_bd_diretorios_brasil.empresa`
# que o flow monolítico antigo fazia depois de promover esta tabela não foi
# reproduzido.
# ──────────────────────────────────────────────────────────────────────────────

_estabelecimentos_pipeline = make_pipeline("estabelecimentos")


@flow(name=_estabelecimentos_pipeline.check_update_flow_name, log_prints=True)
def br_rf_cnpj_estabelecimentos_check_update() -> None:
    _estabelecimentos_pipeline.run_check_update()


br_rf_cnpj_estabelecimentos_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, "estabelecimentos"
)
# Mesmo cron do flow monolítico antigo.
br_rf_cnpj_estabelecimentos_check_update.deploy_schedules = [
    Cron("0 9 * * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_estabelecimentos_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_rf_cnpj_estabelecimentos_download(download_params: dict) -> None:
    _estabelecimentos_pipeline.run_extract_and_load(download_params)


br_rf_cnpj_estabelecimentos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD, "estabelecimentos"
)
_estabelecimentos_pipeline.extract_load_deployment = (
    br_rf_cnpj_estabelecimentos_download.fn.__name__
)
