"""
Tasks for br_rf_cafir
"""

import datetime
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_rf_cafir.utils import (
    download_csv_files,
    parse_api_metadata,
    process_csv_file,
    requests_url,
)
from pipelines.utils.utils import log


@task
def build_paths() -> tuple[Path, Path]:
    tmp_folder = Path("tmp")
    input_folder = tmp_folder / "input" / "br_rf_cafir"
    output_folder = tmp_folder / "output" / "br_rf_cafir"

    input_folder.mkdir(parents=True, exist_ok=True)
    output_folder.mkdir(parents=True, exist_ok=True)

    return (input_folder, output_folder)


@task(
    retries=2,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def get_api_metadata(url: str | None = None) -> pd.DataFrame:
    """
    Faz uma requisição para a URL fornecida e extrai metadados de arquivos CSV.
    Args:
        url (str): A URL da API para fazer a requisição.
    Returns:
        pd.DataFrame: Um DataFrame contendo os nomes dos arquivos e suas respectivas datas de atualização.
    Raises:
        ValueError: Se a quantidade de arquivos extraídos for diferente da quantidade de datas de atualização.
    """
    # pyrefly: ignore [bad-argument-type]
    response = requests_url(url)
    df_metadata = parse_api_metadata(response_text=response.text)

    return df_metadata


@task
def get_last_reference_date(df_metadata: pd.DataFrame) -> datetime.date:
    max_date = df_metadata["data_referencia"].max()
    return max_date.date()


@task(
    retries=2,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def decide_files_to_download(
    df_metadata: pd.DataFrame,
    reference_date: datetime.date | None = None,
) -> pd.DataFrame:
    """
    Decide quais arquivos baixar a depender da necessidade de atualização

    Parâmetros:
    df_metadata (pd.DataFrame): DataFrame contendo informações dos arquivos, incluindo a data de atualização e o nome do arquivo.
    reference_date (datetime.date, opcional): Data de referência específica para filtrar os arquivos. O Padrão é "%yyyy-%mm-%dd".

    Retorna:
    pd.DataFrame: Dataframe filtrado com lista de nomes de arquivos que atendem aos critérios fornecidos e as datas correspondente.

    Levanta:
    ValueError: Se não houver arquivos disponíveis para a data específica fornecida.
    """
    if reference_date is None:
        max_date = df_metadata["data_referencia"].max().date()
        log(
            f"A data máxima extraida da API da Receita Federal que será utilizada para comparar com os metadados da BD: {max_date}"
        )

        return df_metadata[df_metadata["data_referencia"].dt.date == max_date]

    else:
        filtered_df = df_metadata[
            df_metadata["data_referencia"].dt.date == reference_date
        ]
        if filtered_df.empty:
            raise ValueError(
                f"Não há arquivos disponíveis para a data {reference_date}. Verifique o FTP da Receita Federal."
            )
        # pyrefly: ignore [bad-return]
        return filtered_df


@task
def extract_file_records(
    df_metadata: pd.DataFrame,
    filename_col: str = "nome_arquivo",
    reference_date_col: str = "data_referencia",
    last_modified_date_col: str = "data_modificacao",
) -> list[dict]:
    """
    Converte as linhas do DataFrame de metadados filtrado em uma lista de
    dicts com valores escalares, para que cada task de download/processamento
    paralela receba apenas o seu próprio file_name/reference_date
    """
    records = df_metadata[
        [filename_col, reference_date_col, last_modified_date_col]
    ].to_dict("records")
    for record in records:
        record[reference_date_col] = record[reference_date_col].date()
        record[last_modified_date_col] = record[last_modified_date_col].date()
    return records


@task(
    retries=3,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def download_file(file_name: str, url: str, input_folder: Path) -> str:
    log(f"Baixando arquivo: {file_name} de {url}")
    download_csv_files(
        file_name=file_name,
        url=url + file_name,
        input_folder=input_folder,
    )
    return file_name


@task(
    retries=3,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def process_file(
    file_name: str,
    reference_date: datetime.date,
    last_modified_date: datetime.date,
    input_folder: Path,
    output_folder: Path,
) -> Path:
    return process_csv_file(
        file_path=input_folder / file_name,
        reference_date=reference_date,
        last_modified_date=last_modified_date,
        output_folder=output_folder,
    )
