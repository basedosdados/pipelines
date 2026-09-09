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
from pipelines.datasets.br_ms_cnes.tasks import (
    make_check_for_update,
    make_download_data,
)
from pipelines.utils.stage_dispatch import (
    CheckThenDownloadPipeline,
    Etapa,
    deploy_tags,
)


def _make_pipeline(table_id: str) -> CheckThenDownloadPipeline:
    return CheckThenDownloadPipeline(
        dataset_id=DATASET_ID,
        table_id=table_id,
        check_for_update=make_check_for_update(table_id),
        download_data=make_download_data(table_id),
        # Mesma granularidade do flow antigo (`_run_cnes`, que já compara
        # coverage com date_format="%Y-%m" — arquivos DATASUS são mensais).
        date_format="%Y-%m",
    )


_profissional_pipeline = _make_pipeline(PROFISSIONAL_TABLE_ID)
_estabelecimento_pipeline = _make_pipeline(ESTABELECIMENTO_TABLE_ID)
_equipe_pipeline = _make_pipeline(EQUIPE_TABLE_ID)
_leito_pipeline = _make_pipeline(LEITO_TABLE_ID)
_equipamento_pipeline = _make_pipeline(EQUIPAMENTO_TABLE_ID)
_estabelecimento_ensino_pipeline = _make_pipeline(
    ESTABELECIMENTO_ENSINO_TABLE_ID
)
_dados_complementares_pipeline = _make_pipeline(DADOS_COMPLEMENTARES_TABLE_ID)
_estabelecimento_filantropico_pipeline = _make_pipeline(
    ESTABELECIMENTO_FILANTROPICO_TABLE_ID
)
_gestao_metas_pipeline = _make_pipeline(GESTAO_METAS_TABLE_ID)
_habilitacao_pipeline = _make_pipeline(HABILITACAO_TABLE_ID)
_incentivos_pipeline = _make_pipeline(INCENTIVOS_TABLE_ID)
_regra_contratual_pipeline = _make_pipeline(REGRA_CONTRATUAL_TABLE_ID)
_servico_especializado_pipeline = _make_pipeline(
    SERVICO_ESPECIALIZADO_TABLE_ID
)


# ──────────────────────────────────────────────────────────────────────────────
# profissional
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_profissional_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_profissional_check_update_flow() -> None:
    _profissional_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_profissional_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_profissional_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_profissional_download_flow(download_params: dict) -> None:
    _profissional_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_profissional_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_profissional_pipeline.download_deployment = (
    br_ms_cnes_profissional_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimento
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_estabelecimento_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_estabelecimento_check_update_flow() -> None:
    _estabelecimento_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_estabelecimento_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_estabelecimento_download_flow(download_params: dict) -> None:
    _estabelecimento_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_estabelecimento_pipeline.download_deployment = (
    br_ms_cnes_estabelecimento_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# equipe
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_equipe_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_equipe_check_update_flow() -> None:
    _equipe_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipe_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_equipe_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_equipe_download_flow(download_params: dict) -> None:
    _equipe_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipe_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_equipe_pipeline.download_deployment = (
    br_ms_cnes_equipe_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# leito
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_leito_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_leito_check_update_flow() -> None:
    _leito_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_leito_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_leito_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_leito_download_flow(download_params: dict) -> None:
    _leito_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_leito_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_leito_pipeline.download_deployment = (
    br_ms_cnes_leito_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# equipamento
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_equipamento_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_equipamento_check_update_flow() -> None:
    _equipamento_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipamento_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_equipamento_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_equipamento_download_flow(download_params: dict) -> None:
    _equipamento_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_equipamento_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_equipamento_pipeline.download_deployment = (
    br_ms_cnes_equipamento_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimento_ensino
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_estabelecimento_ensino_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_cnes_estabelecimento_ensino_check_update_flow() -> None:
    _estabelecimento_ensino_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_ensino_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(
    name=_estabelecimento_ensino_pipeline.download_flow_name, log_prints=True
)
def br_ms_cnes_estabelecimento_ensino_download_flow(
    download_params: dict,
) -> None:
    _estabelecimento_ensino_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_ensino_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_estabelecimento_ensino_pipeline.download_deployment = (
    br_ms_cnes_estabelecimento_ensino_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# dados_complementares
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_dados_complementares_pipeline.check_update_flow_name, log_prints=True
)
def br_ms_cnes_dados_complementares_check_update_flow() -> None:
    _dados_complementares_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_dados_complementares_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_dados_complementares_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_dados_complementares_download_flow(
    download_params: dict,
) -> None:
    _dados_complementares_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_dados_complementares_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_dados_complementares_pipeline.download_deployment = (
    br_ms_cnes_dados_complementares_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# estabelecimento_filantropico
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_estabelecimento_filantropico_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_cnes_estabelecimento_filantropico_check_update_flow() -> None:
    _estabelecimento_filantropico_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_filantropico_check_update_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE)
)


@flow(
    name=_estabelecimento_filantropico_pipeline.download_flow_name,
    log_prints=True,
)
def br_ms_cnes_estabelecimento_filantropico_download_flow(
    download_params: dict,
) -> None:
    _estabelecimento_filantropico_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_estabelecimento_filantropico_download_flow.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.DOWNLOAD)
)
_estabelecimento_filantropico_pipeline.download_deployment = (
    br_ms_cnes_estabelecimento_filantropico_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# gestao_metas
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_gestao_metas_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_gestao_metas_check_update_flow() -> None:
    _gestao_metas_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_gestao_metas_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_gestao_metas_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_gestao_metas_download_flow(download_params: dict) -> None:
    _gestao_metas_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_gestao_metas_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_gestao_metas_pipeline.download_deployment = (
    br_ms_cnes_gestao_metas_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# habilitacao
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_habilitacao_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_habilitacao_check_update_flow() -> None:
    _habilitacao_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_habilitacao_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_habilitacao_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_habilitacao_download_flow(download_params: dict) -> None:
    _habilitacao_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_habilitacao_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_habilitacao_pipeline.download_deployment = (
    br_ms_cnes_habilitacao_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# incentivos
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_incentivos_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_incentivos_check_update_flow() -> None:
    _incentivos_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_incentivos_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_incentivos_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_incentivos_download_flow(download_params: dict) -> None:
    _incentivos_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_incentivos_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_incentivos_pipeline.download_deployment = (
    br_ms_cnes_incentivos_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# regra_contratual
# ──────────────────────────────────────────────────────────────────────────────


@flow(name=_regra_contratual_pipeline.check_update_flow_name, log_prints=True)
def br_ms_cnes_regra_contratual_check_update_flow() -> None:
    _regra_contratual_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_regra_contratual_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_regra_contratual_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_regra_contratual_download_flow(download_params: dict) -> None:
    _regra_contratual_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_regra_contratual_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_regra_contratual_pipeline.download_deployment = (
    br_ms_cnes_regra_contratual_download_flow.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# servico_especializado
# ──────────────────────────────────────────────────────────────────────────────


@flow(
    name=_servico_especializado_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_ms_cnes_servico_especializado_check_update_flow() -> None:
    _servico_especializado_pipeline.run_check_update()


# pyrefly: ignore [missing-attribute]
br_ms_cnes_servico_especializado_check_update_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)


@flow(name=_servico_especializado_pipeline.download_flow_name, log_prints=True)
def br_ms_cnes_servico_especializado_download_flow(
    download_params: dict,
) -> None:
    _servico_especializado_pipeline.run_download(download_params)


# pyrefly: ignore [missing-attribute]
br_ms_cnes_servico_especializado_download_flow.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.DOWNLOAD
)
_servico_especializado_pipeline.download_deployment = (
    br_ms_cnes_servico_especializado_download_flow.fn.__name__
)
