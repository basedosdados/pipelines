# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""


import os
from datetime import datetime, timedelta

import pandas as pd
from prefect import task

from pipelines.datasets.br_rf_cafir.constants import constants as br_rf_cafir_constants
from pipelines.datasets.br_rf_cafir.utils import (
    download_csv_files,
    preserve_zeros,
    remove_accent,
    parse_api_metadata,
    decide_files_to_download,
    remove_non_ascii_from_df,
    strip_string,
)
from pipelines.utils.utils import log
from pipelines.constants import constants


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def parse_api_metadata(url: str, headers:dict) -> pd.DataFrame:
    return parse_api_metadata(url=url, headers=headers)

@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def decide_files_to_download(df: pd.DataFrame, data_especifica: datetime.date = None, data_maxima: bool = True) -> tuple[list[str],list[datetime]]:
    return decide_files_to_download(df=df, data_especifica=data_especifica, data_maxima=data_maxima)


@task(
    max_retries=3,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def parse_data(url: str, file_list: list[str], data_atualizacao:[datetime.date]) -> str:
    """Essa task faz o download dos arquivos do FTP, faz o parse dos dados e salva os arquivos em um diretório temporário.

    Returns:
        str: Caminho do diretório temporário
    """

    date = data_atualizacao
    log(f"###### Extraindo dados para data: {date}")

    files_list = file_list
    log(f"###### Extraindo files: {files_list}")

    # inicializa counter para ser usado na nomeação dos arquivos repetindo o padrão de divulgação dos dados
    counter = 0
    log(f"###### -----COUNTER: {counter}")

    list_n_cols = []

    for file in files_list:
        counter += 1
        log(f"###### X-----COUNTER: {counter}")

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
                col: preserve_zeros for col in br_rf_cafir_constants.COLUMN_NAMES.value
            },
            encoding="latin-1",
        )

        list_n_cols.append(df.shape[1])

        # remove acentos
        df["nome"] = df["nome"].apply(remove_accent)
        df["endereco"] = df["endereco"].apply(remove_accent)

        # remove não ascii
        df = remove_non_ascii_from_df(df)

        # tira os espacos em branco
        df = df.applymap(strip_string)

        log(f"Saving file: {file}")

        # constroi diretório
        os.makedirs(
            br_rf_cafir_constants.PATH.value[1] + f"/imoveis_rurais/data={date}/",
            exist_ok=True,
        )
        save_path = (
            br_rf_cafir_constants.PATH.value[1]
            + f"/imoveis_rurais/data={date}/"
            + "imoveis_rurais_"
            + str(counter)
            + ".csv"
        )
        log(f"save path: {save_path}")

        # save new file as csv
        df.to_csv(save_path, index=False, sep=",", na_rep="", encoding="utf-8",escapechar='\\')

        # resolve ASCII 0 no momento da leitura do BQ. Ler e salvar de novo.
        df = pd.read_csv(save_path, dtype=str)
        df.to_csv(save_path, index=False, sep=",", na_rep="", encoding="utf-8",escapechar='\\')

        log(f"----- Removendo o arquivo: {os.listdir(br_rf_cafir_constants.PATH.value[0])}")

        # remove o arquivo de input
        os.system("rm -rf " + br_rf_cafir_constants.PATH.value[0] + "/" + "*")


    log(f"list_n_cols: O NUMERO DE COLUNAS É {list_n_cols}")

    # gera paths
    files_path = (
        br_rf_cafir_constants.PATH.value[1]
        + "/"
        + br_rf_cafir_constants.TABLE.value[0]
        + "/"
    )

    return files_path
