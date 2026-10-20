"""
Tasks for br_inmet_bdmep.
"""

from glob import glob

import pandas as pd
from prefect import task

from pipelines.constants import constants as repo_constants
from pipelines.datasets.br_inmet_bdmep.constants import (
    COVERAGE,
    ConstantsMicrodados,
)
from pipelines.datasets.br_inmet_bdmep.utils import (
    download_inmet,
    get_clima_info,
    get_date_from_path,
    get_latest_dowload_link,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
def extract_last_date_from_source():
    """
    Extrai última data de atualização dos dados históricos do site do INMET.
    Faz download do ZIP do ano corrente como efeito colateral — necessário
    para que `get_base_inmet` consiga ler os arquivos depois.
    """
    latest_dowload_link = get_latest_dowload_link()
    download_inmet(latest_dowload_link)

    paths = glob(ConstantsMicrodados.PATH_REGEX.value)
    datas = [get_date_from_path(path) for path in paths]
    return max(datas).date()


@task
def get_base_inmet() -> str:
    """
    Lê os CSVs do INMET já baixados, consolida em um único DataFrame e
    salva particionado por ano. Retorna o diretório raiz do output.
    """
    files = glob(ConstantsMicrodados.PATH_REGEX.value)
    base = pd.concat(
        [get_clima_info(file) for file in files], ignore_index=True
    )
    base = base[ConstantsMicrodados.COLUMNS_ORDER.value]

    year = base.data.max().year
    path_output = ConstantsMicrodados.PATH_OUTPUT.value / f"ano={year}"
    path_output.mkdir(parents=True, exist_ok=True)
    file_path = path_output / f"microdados_{year}.csv"
    base.to_csv(file_path, index=False)

    return str(ConstantsMicrodados.PATH_OUTPUT.value)


# ──────────────────────────────────────────────────────────────────────────────
# microdados — ver constants.py
#
# `get_latest_update` baixa o ZIP do ano corrente (todas as estações),
# igual `extract_load_data` — repetido de propósito, ver
# `CheckThenExtractLoadPipeline` (`stage_dispatch.py`).
# ──────────────────────────────────────────────────────────────────────────────


def microdados_get_latest_update() -> SourceInspection:
    """Baixa o ZIP do ano corrente (efeito colateral de
    `extract_last_date_from_source`) e devolve a data mais recente entre
    os arquivos baixados."""
    reference_date = extract_last_date_from_source()
    # pyrefly: ignore [bad-argument-type]
    return SourceInspection(reference_date=reference_date)


def microdados_download(download_params: dict) -> ExtractAndLoad:
    """Baixa de novo o ZIP do ano corrente (barato o bastante pra repetir,
    ver banner acima) e consolida os CSVs num único arquivo particionado
    por `ano=` (`get_base_inmet`)."""
    extract_last_date_from_source()
    filepath = get_base_inmet()

    return ExtractAndLoad(
        coverage=COVERAGE.model_dump(),
        data_path=filepath,
    )
