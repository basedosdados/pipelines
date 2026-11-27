"""Flows de br_bndes_fundos_garantidores (staged pipeline, uma dupla por tabela)."""

from prefect.schedules import Cron

from pipelines.datasets.br_bndes_fundos_garantidores.constants import (
    constants,
)
from pipelines.datasets.br_bndes_fundos_garantidores.tasks import make_pipeline
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import Etapa, deploy_tags

DATASET_ID = constants.DATASET_ID.value

# ──────────────────────────────────────────────────────────────────────────────
# operacoes_fgi_tradicional
# check_update: br_bndes_fundos_garantidores__operacoes_fgi_tradicional
# extract_and_load: br_bndes_fundos_garantidores__operacoes_fgi_tradicional
# ──────────────────────────────────────────────────────────────────────────────

_TRADICIONAL = "operacoes_fgi_tradicional"
_tradicional_pipeline = make_pipeline(_TRADICIONAL)


@flow(name=_tradicional_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_fundos_garantidores_operacoes_fgi_tradicional_check_update() -> (
    None
):
    _tradicional_pipeline.run_check_update()


br_bndes_fundos_garantidores_operacoes_fgi_tradicional_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, _TRADICIONAL
)
br_bndes_fundos_garantidores_operacoes_fgi_tradicional_check_update.deploy_schedules = [
    Cron(
        constants.CHECK_UPDATE_CRON.value[_TRADICIONAL],
        timezone="America/Sao_Paulo",
    )
]


@flow(name=_tradicional_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bndes_fundos_garantidores_operacoes_fgi_tradicional_download(
    download_params: dict,
) -> None:
    _tradicional_pipeline.run_extract_and_load(download_params)


br_bndes_fundos_garantidores_operacoes_fgi_tradicional_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, _TRADICIONAL)
)
_tradicional_pipeline.extract_load_deployment = (
    br_bndes_fundos_garantidores_operacoes_fgi_tradicional_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_fgi_peac_2022
# check_update: br_bndes_fundos_garantidores__operacoes_fgi_peac_2022
# extract_and_load: br_bndes_fundos_garantidores__operacoes_fgi_peac_2022
# ──────────────────────────────────────────────────────────────────────────────

_PEAC_2022 = "operacoes_fgi_peac_2022"
_peac_2022_pipeline = make_pipeline(_PEAC_2022)


@flow(name=_peac_2022_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_fundos_garantidores_operacoes_fgi_peac_2022_check_update() -> (
    None
):
    _peac_2022_pipeline.run_check_update()


br_bndes_fundos_garantidores_operacoes_fgi_peac_2022_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE, _PEAC_2022
)
br_bndes_fundos_garantidores_operacoes_fgi_peac_2022_check_update.deploy_schedules = [
    Cron(
        constants.CHECK_UPDATE_CRON.value[_PEAC_2022],
        timezone="America/Sao_Paulo",
    )
]


@flow(name=_peac_2022_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bndes_fundos_garantidores_operacoes_fgi_peac_2022_download(
    download_params: dict,
) -> None:
    _peac_2022_pipeline.run_extract_and_load(download_params)


br_bndes_fundos_garantidores_operacoes_fgi_peac_2022_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, _PEAC_2022)
)
_peac_2022_pipeline.extract_load_deployment = (
    br_bndes_fundos_garantidores_operacoes_fgi_peac_2022_download.fn.__name__
)


# ──────────────────────────────────────────────────────────────────────────────
# operacoes_fg_sebrae
# check_update: br_bndes_fundos_garantidores__operacoes_fg_sebrae
# extract_and_load: br_bndes_fundos_garantidores__operacoes_fg_sebrae
# ──────────────────────────────────────────────────────────────────────────────

_FG_SEBRAE = "operacoes_fg_sebrae"
_fg_sebrae_pipeline = make_pipeline(_FG_SEBRAE)


@flow(name=_fg_sebrae_pipeline.check_update_flow_name, log_prints=True)
def br_bndes_fundos_garantidores_operacoes_fg_sebrae_check_update() -> None:
    _fg_sebrae_pipeline.run_check_update()


br_bndes_fundos_garantidores_operacoes_fg_sebrae_check_update.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.CHECK_UPDATE, _FG_SEBRAE)
)
br_bndes_fundos_garantidores_operacoes_fg_sebrae_check_update.deploy_schedules = [
    Cron(
        constants.CHECK_UPDATE_CRON.value[_FG_SEBRAE],
        timezone="America/Sao_Paulo",
    )
]


@flow(name=_fg_sebrae_pipeline.extract_and_load_flow_name, log_prints=True)
def br_bndes_fundos_garantidores_operacoes_fg_sebrae_download(
    download_params: dict,
) -> None:
    _fg_sebrae_pipeline.run_extract_and_load(download_params)


br_bndes_fundos_garantidores_operacoes_fg_sebrae_download.deploy_tags = (
    deploy_tags(DATASET_ID, Etapa.EXTRACT_AND_LOAD, _FG_SEBRAE)
)
_fg_sebrae_pipeline.extract_load_deployment = (
    br_bndes_fundos_garantidores_operacoes_fg_sebrae_download.fn.__name__
)
