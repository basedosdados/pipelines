"""
Tasks for br_ms_cnes
"""

import datetime
import os
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_rf_cafir.constants import (
    constants as br_rf_cafir_constants,
)
from pipelines.datasets.br_rf_cafir.utils import (
    download_csv_files,
    parse_api_metadata,
    preserve_zeros,
    remove_ascii_zero_from_df,
    requests_url,
    strip_string,
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

        log(
            f"A data máxima extraida da API da Receita Federal que será utilizada para gerar partições no Storage: {max_date}"
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


@task(
    retries=3,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def download_files(
    url: str,
    input_folder: Path,
    df_metadata: pd.DataFrame,
    filename_col: str = "nome_arquivo",
    reference_date_col: str = "data_referencia",
):
    log(
        f"------ Os seguintes arquivos foram selecionados para download: {df_metadata[filename_col].to_list()}"
    )

    for _, row in df_metadata.iterrows():
        file_name = row[filename_col]
        reference_date = row[reference_date_col]
        log(
            f"------ Extraindo dados para data de referência: {reference_date}"
        )

        log(f"Baixando arquivo: {file_name} de {url}")

        # Monta url completa do arquivo
        complete_url = url + file_name
        # Baixa arquivo
        download_csv_files(
            file_name=file_name,
            url=complete_url,
            input_folder=input_folder,
        )


@task(
    retries=3,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def process_files(
    input_folder: Path,
    output_folder: Path,
    df_metadata: pd.DataFrame,
    filename_col: str = "nome_arquivo",
    reference_date_col: str = "data_referencia",
    # update_date_col: str = "data_modificacao",
):
    for file_path in input_folder.glob("*.csv"):
        file_name = file_path.name
        file_row = df_metadata[
            df_metadata[filename_col].str.startswith(file_name)
        ]

        if not file_row.empty:
            reference_date = file_row[reference_date_col]
            # update_date = file_row[update_date_col]

            log(f"Lendo arquivo: {file_name} de : {file_path}")
            # Le o arquivo txt
            df = pd.read_fwf(
                file_path,
                widths=br_rf_cafir_constants.WIDTHS.value,
                names=br_rf_cafir_constants.COLUMN_NAMES.value,
                dtype=br_rf_cafir_constants.DTYPE.value,
                converters={
                    col: preserve_zeros
                    for col in br_rf_cafir_constants.COLUMN_NAMES.value
                },
                encoding="ISO-8859-1",
            )

            # Remove ascii /x00 (zero) - pois dá erro na na materialização no BQ
            df = remove_ascii_zero_from_df(df)

            # Tira os espacos em branco
            # pyrefly: ignore [not-callable]
            df = df.applymap(strip_string)
            # df[update_date_col] = update_date

            log(f"Salvando arquivo: {file_name}")

            # Constroi partições
            partitions_path = (
                output_folder / "imoveis_rurais" / f"data={reference_date}"
            )
            partitions_path.mkdir(parents=True, exist_ok=True)

            # NOTE: Com modificação do formato de divulgação do FTP os arquivos passaram a ser divulgados csvs particionados por UF
            # A partir de 2025, a nomenclaruta dos no Storage arquivos mudou para: "imoveis_rurais_uf_numero.csv" no lugar de "imoveris_rurais_numero.csv"

            save_path = partitions_path / (
                "imoveis_rurais_"
                # Extrai uf e numeração do nome do arquivo
                + file_name.split(".")[-2]
                + ".csv"
            )

            df.to_csv(
                save_path,
                index=False,
                sep=",",
                na_rep="",
                encoding="utf-8",
                escapechar="\\",
            )

            log(f"Arquivo salvo: {save_path.as_posix().split('/')[-1]}")

            del df

            log(
                f"----- Removendo o arquivo: {os.listdir(input_folder)} do diretório de input"
            )
            # Remove o arquivo de input
            os.remove(input_folder / file_name)
        else:
            raise Warning(
                f"Não há arquivo correspondente a {file_path} nos metadados."
            )
    return output_folder / "imoveis_rurais"
