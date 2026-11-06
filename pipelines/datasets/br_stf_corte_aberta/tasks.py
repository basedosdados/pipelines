"""
Tasks for br_stf_corte_aberta.
"""

from datetime import datetime

from prefect import task

from pipelines.constants import constants as repo_constants
from pipelines.datasets.br_stf_corte_aberta.constants import (
    constants as stf_constants,
)
from pipelines.datasets.br_stf_corte_aberta.utils import (
    check_for_data,
    column_bool,
    fix_columns_data,
    partition_data,
    read_csv,
    rename_ordening_columns,
    replace_columns,
    web_scrapping,
)
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection
from pipelines.utils.utils import log


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
def get_data_source_stf_max_date():
    data_obj = check_for_data()
    data_obj = datetime.strptime(data_obj, "%Y-%m-%d").date()
    return data_obj


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
# make_partition
def make_partitions(df):
    # pyrefly: ignore [bad-argument-type]
    partition_data(df, "data_decisao", stf_constants.STF_OUTPUT.value)
    return stf_constants.STF_OUTPUT.value


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
def download_and_transform():
    log("Iniciando a leitura do csv")
    df = read_csv()
    log("Iniciando a correção das colunas de data")
    df = fix_columns_data(df)
    log("Iniciando a correção da coluna booleana")
    df = column_bool(df)
    log("Iniciando a renomeação e ordenação das colunas")
    df = rename_ordening_columns(df)
    log("Iniciando a substituição de variáveis")
    df = replace_columns(df)
    return df


@task(
    retries=repo_constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=repo_constants.TASK_RETRY_DELAY.value,
)
def task_check_for_data():
    return check_for_data()


# ──────────────────────────────────────────────────────────────────────────────
# decisoes — ver constants.py
#
# A variante "check_and_download" (reaproveitar no extract_and_load o mesmo
# arquivo já baixado pelo check_update) foi cancelada. Decisão: todo dataset
# usa check_update -> extract_and_load, mesmo quando o check precisa baixar o
# dado pra descobrir a data de referência.
#
# `get_data_source_stf_max_date` (crawler/stf_corte_aberta/tasks.py) já
# dispara o `web_scrapping()` (Selenium: abre o painel e clica no botão de
# exportar) por dentro de `check_for_data()` — é o único jeito de descobrir a
# data mais recente nessa fonte, não tem endpoint/listagem leve. Então
# `get_latest_update` já "baixa o que for necessário".
#
# `extract_load_data` chama `web_scrapping()` de novo, de forma independente
# (as duas etapas rodam em flow runs/containers separados, não compartilham o
# arquivo baixado em `/tmp/input/` durante o check) — custo duplicado
# aceito, é a mesma fonte/mesmo botão de exportar.
# ──────────────────────────────────────────────────────────────────────────────


def get_latest_update() -> SourceInspection:
    reference_date = get_data_source_stf_max_date()
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    web_scrapping()
    df = download_and_transform()
    output_path = make_partitions(df=df)

    return ExtractAndLoad(
        data_path=output_path,
    )
