"""
Flows para br_ms_cnes — Prefect 3.

Migrado por completo pro pipeline orientado a eventos (issue #1867):
check_update -> download -> mat_test, uma dupla de flows por tabela.
Lógica específica do dataset mora em `tasks.py`, constantes em
`constants.py` — aqui só a fiação (`CheckThenDownloadPipeline` + `@flow`).

O antigo `_cnes_flow`/`_run_cnes` monolítico segue existindo em
`pipelines/crawler/datasus/flows.py`, ainda usado por
`br_ms_sia`/`br_ms_sih`/`br_ms_sinan` (não migrados ainda) — não removido
daqui.
"""

from prefect import flow

from pipelines.datasets.br_ms_cnes.constants import (
    DADOS_COMPLEMENTARES_TABLE_ID,
    DATASET_ID,
    EQUIPAMENTO_TABLE_ID,
    EQUIPE_TABLE_ID,
    ESTABELECIMENTO_ENSINO_TABLE_ID,
    ESTABELECIMENTO_FILANTROPICO_TABLE_ID,
    ESTABELECIMENTO_TABLE_ID,
    GESTAO_METAS_TABLE_ID,
    HABILITACAO_TABLE_ID,
    INCENTIVOS_TABLE_ID,
    LEITO_TABLE_ID,
    PROFISSIONAL_TABLE_ID,
    REGRA_CONTRATUAL_TABLE_ID,
    SERVICO_ESPECIALIZADO_TABLE_ID,
)
from pipelines.datasets.br_ms_cnes.tasks import make_pipeline
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

# ──────────────────────────────────────────────────────────────────────────────
# profissional
# check_update: br_ms_cnes__profissional
# download: br_ms_cnes__profissional
# ──────────────────────────────────────────────────────────────────────────────

_profissional_pipeline = make_pipeline(PROFISSIONAL_TABLE_ID)


@flow(name=_profissional_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_profissional_check_update() -> None:
    _profissional_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_profissional_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_profissional_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_profissional_download(download_params: dict) -> None:
    _profissional_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_profissional_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_profissional_pipeline.download_deployment = (
    br_ms_cnes_profissional_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimento
# check_update: br_ms_cnes__estabelecimento
# download: br_ms_cnes__estabelecimento
# ──────────────────────────────────────────────────────────────────────────────

_estabelecimento_pipeline = make_pipeline(ESTABELECIMENTO_TABLE_ID)


@flow(name=_estabelecimento_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_estabelecimento_check_update() -> None:
    _estabelecimento_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_estabelecimento_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_estabelecimento_download(download_params: dict) -> None:
    _estabelecimento_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_estabelecimento_pipeline.download_deployment = (
    br_ms_cnes_estabelecimento_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# equipe
# check_update: br_ms_cnes__equipe
# download: br_ms_cnes__equipe
# ──────────────────────────────────────────────────────────────────────────────

_equipe_pipeline = make_pipeline(EQUIPE_TABLE_ID)


@flow(name=_equipe_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_equipe_check_update() -> None:
    _equipe_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipe_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_equipe_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_equipe_download(download_params: dict) -> None:
    _equipe_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipe_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_equipe_pipeline.download_deployment = br_ms_cnes_equipe_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# leito
# check_update: br_ms_cnes__leito
# download: br_ms_cnes__leito
# ──────────────────────────────────────────────────────────────────────────────

_leito_pipeline = make_pipeline(LEITO_TABLE_ID)


@flow(name=_leito_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_leito_check_update() -> None:
    _leito_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_leito_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_leito_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_leito_download(download_params: dict) -> None:
    _leito_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_leito_download.deploy_tags = deploy_tags(DATASET_ID, Etapa.DOWNLOAD)
_leito_pipeline.download_deployment = br_ms_cnes_leito_download.fn.__name__


# ──────────────────────────────────────────────────────────────────────────────
# equipamento
# check_update: br_ms_cnes__equipamento
# download: br_ms_cnes__equipamento
# ──────────────────────────────────────────────────────────────────────────────

_equipamento_pipeline = make_pipeline(EQUIPAMENTO_TABLE_ID)


@flow(name=_equipamento_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_equipamento_check_update() -> None:
    _equipamento_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipamento_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_equipamento_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_equipamento_download(download_params: dict) -> None:
    _equipamento_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipamento_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_equipamento_pipeline.download_deployment = (
    br_ms_cnes_equipamento_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimento_ensino
# check_update: br_ms_cnes__estabelecimento_ensino
# download: br_ms_cnes__estabelecimento_ensino
# ──────────────────────────────────────────────────────────────────────────────

_estabelecimento_ensino_pipeline = make_pipeline(
    ESTABELECIMENTO_ENSINO_TABLE_ID
)


@flow(
    name=_estabelecimento_ensino_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_cnes_estabelecimento_ensino_check_update() -> None:
    _estabelecimento_ensino_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_ensino_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_estabelecimento_ensino_pipeline.download_flow_name, log_prints=True
)
def br_ms_cnes_estabelecimento_ensino_download(
    download_params: dict,
) -> None:
    _estabelecimento_ensino_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_ensino_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_estabelecimento_ensino_pipeline.download_deployment = (
    br_ms_cnes_estabelecimento_ensino_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dados_complementares
# check_update: br_ms_cnes__dados_complementares
# download: br_ms_cnes__dados_complementares
# ──────────────────────────────────────────────────────────────────────────────

_dados_complementares_pipeline = make_pipeline(DADOS_COMPLEMENTARES_TABLE_ID)


@flow(
    name=_dados_complementares_pipeline.check_update_flow_name, log_prints=True
)
def br_ms_cnes_dados_complementares_check_update() -> None:
    _dados_complementares_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_dados_complementares_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_dados_complementares_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_dados_complementares_download(
    download_params: dict,
) -> None:
    _dados_complementares_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_dados_complementares_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_dados_complementares_pipeline.download_deployment = (
    br_ms_cnes_dados_complementares_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimento_filantropico
# check_update: br_ms_cnes__estabelecimento_filantropico
# download: br_ms_cnes__estabelecimento_filantropico
# ──────────────────────────────────────────────────────────────────────────────

_estabelecimento_filantropico_pipeline = make_pipeline(
    ESTABELECIMENTO_FILANTROPICO_TABLE_ID
)


@flow(
    name=_estabelecimento_filantropico_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_cnes_estabelecimento_filantropico_check_update() -> None:
    _estabelecimento_filantropico_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_filantropico_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_estabelecimento_filantropico_pipeline.download_flow_name,
    log_prints=True,
)
def br_ms_cnes_estabelecimento_filantropico_download(
    download_params: dict,
) -> None:
    _estabelecimento_filantropico_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_filantropico_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_estabelecimento_filantropico_pipeline.download_deployment = (
    br_ms_cnes_estabelecimento_filantropico_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# gestao_metas
# check_update: br_ms_cnes__gestao_metas
# download: br_ms_cnes__gestao_metas
# ──────────────────────────────────────────────────────────────────────────────

_gestao_metas_pipeline = make_pipeline(GESTAO_METAS_TABLE_ID)


@flow(name=_gestao_metas_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_gestao_metas_check_update() -> None:
    _gestao_metas_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_gestao_metas_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_gestao_metas_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_gestao_metas_download(download_params: dict) -> None:
    _gestao_metas_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_gestao_metas_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_gestao_metas_pipeline.download_deployment = (
    br_ms_cnes_gestao_metas_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# habilitacao
# check_update: br_ms_cnes__habilitacao
# download: br_ms_cnes__habilitacao
# ──────────────────────────────────────────────────────────────────────────────

_habilitacao_pipeline = make_pipeline(HABILITACAO_TABLE_ID)


@flow(name=_habilitacao_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_habilitacao_check_update() -> None:
    _habilitacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_habilitacao_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_habilitacao_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_habilitacao_download(download_params: dict) -> None:
    _habilitacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_habilitacao_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_habilitacao_pipeline.download_deployment = (
    br_ms_cnes_habilitacao_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# incentivos
# check_update: br_ms_cnes__incentivos
# download: br_ms_cnes__incentivos
# ──────────────────────────────────────────────────────────────────────────────

_incentivos_pipeline = make_pipeline(INCENTIVOS_TABLE_ID)


@flow(name=_incentivos_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_incentivos_check_update() -> None:
    _incentivos_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_incentivos_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_incentivos_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_incentivos_download(download_params: dict) -> None:
    _incentivos_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_incentivos_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_incentivos_pipeline.download_deployment = (
    br_ms_cnes_incentivos_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# regra_contratual
# check_update: br_ms_cnes__regra_contratual
# download: br_ms_cnes__regra_contratual
# ──────────────────────────────────────────────────────────────────────────────

_regra_contratual_pipeline = make_pipeline(REGRA_CONTRATUAL_TABLE_ID)


@flow(name=_regra_contratual_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_regra_contratual_check_update() -> None:
    _regra_contratual_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_regra_contratual_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_regra_contratual_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_regra_contratual_download(download_params: dict) -> None:
    _regra_contratual_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_regra_contratual_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_regra_contratual_pipeline.download_deployment = (
    br_ms_cnes_regra_contratual_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# servico_especializado
# check_update: br_ms_cnes__servico_especializado
# download: br_ms_cnes__servico_especializado
# ──────────────────────────────────────────────────────────────────────────────

_servico_especializado_pipeline = make_pipeline(SERVICO_ESPECIALIZADO_TABLE_ID)


@flow(
    name=_servico_especializado_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_cnes_servico_especializado_check_update() -> None:
    _servico_especializado_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_servico_especializado_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_servico_especializado_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_servico_especializado_download(
    download_params: dict,
) -> None:
    _servico_especializado_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_servico_especializado_download.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_servico_especializado_pipeline.download_deployment = (
    br_ms_cnes_servico_especializado_download.fn.__name__
)
