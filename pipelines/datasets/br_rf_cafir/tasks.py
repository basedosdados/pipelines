"""
Tasks for br_ms_cnes
"""

import datetime
import os

import pandas as pd
from prefect import task

from pipelines.constants import constants
from pipelines.datasets.br_rf_cafir.constants import (
    constants as br_rf_cafir_constants,
)
from pipelines.datasets.br_rf_cafir.utils import (
    decide_files_to_download,
    download_csv_files,
    parse_api_metadata,
    preserve_zeros,
    remove_ascii_zero_from_df,
    strip_string,
)
from pipelines.utils.utils import log


@task(
    max_retries=2,
    retry_delay=datetime.timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def task_parse_api_metadata(url: str) -> pd.DataFrame:
    return parse_api_metadata(url=url)


@task(
    max_retries=2,
    retry_delay=datetime.timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def task_decide_files_to_download(
    df: pd.DataFrame,
    data_especifica: datetime.date | None = None,
    data_maxima: bool = True,
) -> tuple[list[str], list[datetime.date]]:
    return decide_files_to_download(
        df=df, data_especifica=data_especifica, data_maxima=data_maxima
    )


@task(
    max_retries=3,
    retry_delay=datetime.timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def task_download_files(
    url: str,
    file_list: list[str],
    data_atualizacao: list[datetime.date],
) -> str:
    """Essa task faz o download dos arquivos do FTP, faz o parse dos dados e salva os arquivos em um diretório temporário.

    Returns:
        str: Caminho do diretório temporário
    """

    date = data_atualizacao
    log(f"------ Extraindo dados para data: {date}")

    files_list = file_list
    log(
        f"------ Os seguintes arquivos foram selecionados para download: {files_list}"
    )

    for file in files_list:
        log(f"Baixando arquivo: {file} de {url}")

        # monta url
        complete_url = url + file

        # baixa arquivo
        download_csv_files(
            file_name=file,
            url=complete_url,
            download_directory=br_rf_cafir_constants.PATH.value[0],
        )

        # constroi caminho do arquivo
        file_path = br_rf_cafir_constants.PATH.value[0] + "/" + file
        log(f"Lendo arquivo: {file} de : {file_path}")

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

        # Remove ascii /x00 (zero) - crasha tabela na materialização no BQ
        df = remove_ascii_zero_from_df(df)

        # tira os espacos em branco
        df = df.applymap(strip_string)

        log(f"Salvando arquivo: {file}")

        # constroi diretório
        os.makedirs(
            br_rf_cafir_constants.PATH.value[1]
            + f"/imoveis_rurais/data={date}/",
            exist_ok=True,
        )

        # NOTE: Com modificação do formato de divulgação do FTP os arquivos passaram a ser divulgados csvs particionados por UF
        # A partir de 2025, a nomenclaruta dos no Storage arquivos mudou para: "imoveis_rurais_uf_numero.csv" no lugar de "imoveris_rurais_numero.csv"

        save_path = (
            br_rf_cafir_constants.PATH.value[1]
            + f"/imoveis_rurais/data={date}/"
            + "imoveis_rurais_"
            # extrai uf e numeração do nome do arquivo
            + file.split(".")[-2]
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

        log(f"Arquivo salvo: {save_path.split('/')[-1]}")

        del df

        log(
            f"----- Removendo o arquivo: {os.listdir(br_rf_cafir_constants.PATH.value[0])} do diretório de input"
        )

        # remove o arquivo de input
        os.remove(os.path.join(br_rf_cafir_constants.PATH.value[0], file))

    return br_rf_cafir_constants.PATH.value[1] + "/imoveis_rurais"
