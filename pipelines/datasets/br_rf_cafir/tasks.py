# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""


import os
from datetime import datetime

import pandas as pd
from prefect import task

from pipelines.datasets.br_rf_cafir.constants import constants as br_rf_cafir_constants
from pipelines.datasets.br_rf_cafir.utils import (
    download_csv_files,
    parse_date_parse_files,
    preserve_zeros,
    remove_accent,
    remove_non_ascii_from_df,
    strip_string,
)
from pipelines.utils.utils import log


@task
def parse_files_parse_date(url) -> tuple[list[datetime], list[str]]:
    """Extrai os nomes dos arquivos e a data de disponibilização dos dados no FTP

    Args:
        url (string): URL do FTP

    Returns:
        Tuple: Retorna uma tupla com duas listas. A primeira contém uma lista de datas de atualização dos dados e a segunda contém uma lista com os nomes dos arquivos.
    """
    log("########  download_files_parse_data  ########")

    date_files = parse_date_parse_files(url)

    return date_files


@task
def parse_data(url: str, other_task_output: tuple[list[datetime], list[str]]) -> str:
    """Essa task faz o download dos arquivos do FTP, faz o parse dos dados e salva os arquivos em um diretório temporário.

    Returns:
        str: Caminho do diretório temporário
    """

    date = other_task_output[0]
    log(f"###### Extraindo dados para data: {date}")

    files_list = other_task_output[1]
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
        df.to_csv(save_path, index=False, sep=",", na_rep="", encoding="utf-8")

        # resolve ASCII 0 no momento da leitura do BQ. Ler e salvar de novo.
        df = pd.read_csv(save_path, dtype=str)
        df.to_csv(save_path, index=False, sep=",", na_rep="", encoding="utf-8")

        log(f"no dir input tem: {os.listdir(br_rf_cafir_constants.PATH.value[0])}")

        # remove o arquivo de input
        os.system("rm -rf " + br_rf_cafir_constants.PATH.value[0] + "/" + "*")

        # verificar se os arquivos foram removidos
        log(f"no dir input tem: {os.listdir(br_rf_cafir_constants.PATH.value[0])}")

    log(f"list_n_cols: O NUMERO DE COLUNAS É {list_n_cols}")

    # gera paths
    files_path = (
        br_rf_cafir_constants.PATH.value[1]
        + "/"
        + br_rf_cafir_constants.TABLE.value[0]
        + "/"
    )

    return files_path
