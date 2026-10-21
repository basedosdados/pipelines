"""Flows de br_me_caged — Prefect 3, modelo em estágios."""

import datetime

from prefect.schedules import Cron

from pipelines.datasets.br_me_caged.constants import (
    constants as caged_constants,
)
from pipelines.datasets.br_me_caged.tasks import (
    get_latest_update,
    get_table_last_date,
    microdados_movimentacao_excluida_extract_load_data,
    microdados_movimentacao_extract_load_data,
    microdados_movimentacao_fora_prazo_extract_load_data,
    resolve_year_months,
)
from pipelines.utils.flow import flow
from pipelines.utils.stage_dispatch import (
    CheckThenExtractLoadPipeline,
    Etapa,
    deploy_tags,
)
from pipelines.utils.utils import log


def run_download(
    pipeline: CheckThenExtractLoadPipeline, download_params: dict
) -> None:
    """Carrega os meses pendentes de uma tabela, ou termina sem nenhum."""
    years_months = resolve_year_months(
        backfill_year_months=download_params.get("year_months"),
        table_last_date=get_table_last_date(
            dataset_id=pipeline.dataset_id, table_id=pipeline.table_id
        ),
        source_last_date=datetime.datetime.fromisoformat(
            download_params["reference_date"]
        ),
    )

    if not years_months:
        log(
            f"{pipeline.table_id}: nenhum mês a carregar até "
            f"{download_params['reference_date']}; sem upload nem dbt"
        )
        return

    pipeline.run_extract_and_load(
        download_params={
            "reference_date": download_params["reference_date"],
            "year_months": years_months,
        }
    )


_microdados_movimentacao_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=caged_constants.DATASET_ID.value,
    table_id=caged_constants.MOVIMENTACAO_TABLE_ID.value,
    get_latest_update=get_latest_update,
    extract_load_data=microdados_movimentacao_extract_load_data,
    date_format="%Y-%m",
)


@flow(
    name=_microdados_movimentacao_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_check_update() -> None:
    _microdados_movimentacao_pipeline.run_check_update()


br_me_caged_microdados_movimentacao_check_update.deploy_tags = deploy_tags(
    caged_constants.DATASET_ID.value,
    Etapa.CHECK_UPDATE,
    caged_constants.MOVIMENTACAO_TABLE_ID.value,
)

br_me_caged_microdados_movimentacao_check_update.deploy_schedules = [
    Cron("5 8,17 1-4,26-31 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_microdados_movimentacao_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_download(
    download_params: dict,
) -> None:
    run_download(
        pipeline=_microdados_movimentacao_pipeline,
        download_params=download_params,
    )


br_me_caged_microdados_movimentacao_download.deploy_tags = deploy_tags(
    caged_constants.DATASET_ID.value,
    Etapa.EXTRACT_AND_LOAD,
    caged_constants.MOVIMENTACAO_TABLE_ID.value,
)

_microdados_movimentacao_pipeline.extract_load_deployment = (
    br_me_caged_microdados_movimentacao_download.fn.__name__
)


_microdados_movimentacao_fora_prazo_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=caged_constants.DATASET_ID.value,
    table_id=caged_constants.FORA_PRAZO_TABLE_ID.value,
    get_latest_update=get_latest_update,
    extract_load_data=microdados_movimentacao_fora_prazo_extract_load_data,
    date_format="%Y-%m",
)


@flow(
    name=_microdados_movimentacao_fora_prazo_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_fora_prazo_check_update() -> None:
    _microdados_movimentacao_fora_prazo_pipeline.run_check_update()


br_me_caged_microdados_movimentacao_fora_prazo_check_update.deploy_tags = (
    deploy_tags(
        caged_constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        caged_constants.FORA_PRAZO_TABLE_ID.value,
    )
)
br_me_caged_microdados_movimentacao_fora_prazo_check_update.deploy_schedules = [
    Cron("10 8,17 1-4,26-31 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_microdados_movimentacao_fora_prazo_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_fora_prazo_download(
    download_params: dict,
) -> None:
    run_download(
        pipeline=_microdados_movimentacao_fora_prazo_pipeline,
        download_params=download_params,
    )


br_me_caged_microdados_movimentacao_fora_prazo_download.deploy_tags = (
    deploy_tags(
        caged_constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        caged_constants.FORA_PRAZO_TABLE_ID.value,
    )
)
_microdados_movimentacao_fora_prazo_pipeline.extract_load_deployment = (
    br_me_caged_microdados_movimentacao_fora_prazo_download.fn.__name__
)


_microdados_movimentacao_excluida_pipeline = CheckThenExtractLoadPipeline(
    dataset_id=caged_constants.DATASET_ID.value,
    table_id=caged_constants.EXCLUIDA_TABLE_ID.value,
    get_latest_update=get_latest_update,
    extract_load_data=microdados_movimentacao_excluida_extract_load_data,
    date_format="%Y-%m",
)


@flow(
    name=_microdados_movimentacao_excluida_pipeline.check_update_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_excluida_check_update() -> None:
    _microdados_movimentacao_excluida_pipeline.run_check_update()


br_me_caged_microdados_movimentacao_excluida_check_update.deploy_tags = (
    deploy_tags(
        caged_constants.DATASET_ID.value,
        Etapa.CHECK_UPDATE,
        caged_constants.EXCLUIDA_TABLE_ID.value,
    )
)
br_me_caged_microdados_movimentacao_excluida_check_update.deploy_schedules = [
    Cron("45 8,17 1-4,26-31 * *", timezone="America/Sao_Paulo")
]


@flow(
    name=_microdados_movimentacao_excluida_pipeline.extract_and_load_flow_name,
    log_prints=True,
)
def br_me_caged_microdados_movimentacao_excluida_download(
    download_params: dict,
) -> None:
    run_download(
        pipeline=_microdados_movimentacao_excluida_pipeline,
        download_params=download_params,
    )


br_me_caged_microdados_movimentacao_excluida_download.deploy_tags = (
    deploy_tags(
        caged_constants.DATASET_ID.value,
        Etapa.EXTRACT_AND_LOAD,
        caged_constants.EXCLUIDA_TABLE_ID.value,
    )
)
_microdados_movimentacao_excluida_pipeline.extract_load_deployment = (
    br_me_caged_microdados_movimentacao_excluida_download.fn.__name__
)
