"""
Tasks for br_anp_precos_combustiveis.

Custo duplicado aceito: a fonte só expõe a data de referência baixando o
próprio recorte de 4 semanas (`ultimas-4-semanas-glp.csv`) — não há sinal de
data sem baixar algo primeiro. `get_latest_update` baixa só esse recorte
(`get_data_source_anp_max_date`); `extract_load_data` baixa os 3 CSVs de
novo, de forma independente (`download_and_transform`) — a variante
"check_and_download" que evitaria essa segunda chamada foi cancelada.
"""

from datetime import date, timedelta

from prefect import task

from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    DATASET_ID,
    MICRODADOS_TABLE_ID,
)
from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    constants as anp_constants,
)
from pipelines.datasets.br_anp_precos_combustiveis.utils import (
    datas_coleta,
    download_files,
    open_csvs,
    partition_data,
    tratar_precos,
)
from pipelines.utils.data_tests.rows import check_rows_written
from pipelines.utils.metadata.client import MetadataClient
from pipelines.utils.stage_dispatch import ExtractAndLoad, SourceInspection


@task
def get_data_source_anp_max_date() -> date:
    download_files(
        anp_constants.URLS_DATA.value, anp_constants.PATH_INPUT.value
    )
    return datas_coleta(anp_constants.URL_GLP.value)[1]


@task
def verificar_lacuna() -> None:
    """falha se a tabela parou antes da primeira semana do arquivo da anp."""
    primeira = datas_coleta(anp_constants.URL_GLP.value)[0]
    ultima_tabela = MetadataClient(env="prod").get_coverage_max_date(
        DATASET_ID, MICRODADOS_TABLE_ID
    )
    if ultima_tabela and primeira - ultima_tabela > timedelta(days=7):
        raise ValueError(
            f"Lacuna entre {ultima_tabela} (última data da tabela) e {primeira} "
            "(primeira data do arquivo da ANP): recuperar pelos arquivos "
            "semestrais (ver README)."
        )


@task(retries=5, retry_delay_seconds=10)
def download_and_transform():
    download_files(anp_constants.URLS.value, anp_constants.PATH_INPUT.value)

    precos_combustiveis = open_csvs(
        anp_constants.URL_DIESEL_GNV.value,
        anp_constants.URL_GASOLINA_ETANOL.value,
        anp_constants.URL_GLP.value,
    )

    return tratar_precos(precos_combustiveis)


@task(retries=5, retry_delay_seconds=10)
def make_partitions(df):
    partition_data(
        df,
        # pyrefly: ignore [bad-argument-type]
        column_name="data_coleta",
        output_directory=anp_constants.PATH_OUTPUT.value,
    )
    return anp_constants.PATH_OUTPUT.value


def get_latest_update() -> SourceInspection:
    reference_date = get_data_source_anp_max_date()
    return SourceInspection(reference_date=reference_date)


def extract_load_data(download_params: dict) -> ExtractAndLoad:
    df = download_and_transform()
    output_path = make_partitions(df=df)

    check_rows_written(rows_read=len(df), output_dir=output_path)

    return ExtractAndLoad(
        data_path=output_path,
    )
