# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_estban project
"""
import requests
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import numpy as np
import os
import zipfile
from tqdm import tqdm
from datetime import datetime

from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_b3_cotacoes.constants import (
    constants as br_b3_cotacoes_constants,
)


def download_and_unzip(url, path):
    """download and unzip a zip file

    Args:
        url (str): a url


    Returns:
        list: unziped files in a given folder
    """

    os.system(f"mkdir -p {path}")

    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=path)

    return path


# ------- macro etapa 1 download de dados
# ------- download and unzip csv


def download_unzip_csv(url, path, chunk_size: int = 1000):
    print(f"Baixando o arquivo {url}")
    download_url = url
    save_path = os.path.join(path, f"{os.path.basename(url)}.zip")
    r = requests.get(download_url, stream=True, timeout=60)
    with open(save_path, "wb") as fd:
        for chunk in tqdm(
            r.iter_content(chunk_size=chunk_size), desc="Baixando o arquivo"
        ):
            fd.write(chunk)

    try:
        with zipfile.ZipFile(save_path) as z:
            z.extractall(path)
        print("Dados extraídos com sucesso!")

    except zipfile.BadZipFile:
        print(f"O arquivo {os.path.basename(url)} não é um arquivo ZIP válido.")

    os.remove(save_path)


# ------- macro etapa 3 tratando os dados através do chunk
# ------- process chunk
def process_chunk(chunk):
    log(
        "********************************INICIANDO O TRATAMENTO DOS DADOS********************************"
    )
    chunk.rename(columns={br_b3_cotacoes_constants.RENAME.value}, inplace=True)

    chunk = chunk.replace(np.nan, "")

    chunk["codigo_participante_vendedor"] = chunk["codigo_participante_vendedor"].apply(
        lambda x: str(x).replace(".0", "")
    )

    chunk["codigo_participante_comprador"] = chunk[
        "codigo_participante_comprador"
    ].apply(lambda x: str(x).replace(".0", ""))

    chunk["preco_negocio"] = chunk["preco_negocio"].apply(
        lambda x: str(x).replace(",", ".")
    )

    chunk["data_referencia"] = pd.to_datetime(
        chunk["data_referencia"], format="%Y-%m-%d"
    )

    chunk["data_negocio"] = pd.to_datetime(chunk["data_negocio"], format="%Y-%m-%d")

    chunk["preco_negocio"] = chunk["preco_negocio"].astype(float)

    chunk["codigo_identificador_negocio"] = chunk[
        "codigo_identificador_negocio"
    ].astype(str)

    chunk["hora_fechamento"] = chunk["hora_fechamento"].astype(str)

    chunk["hora_fechamento"] = np.where(
        chunk["hora_fechamento"].str.len() == 8,
        "0" + chunk["hora_fechamento"],
        chunk["hora_fechamento"],
    )

    chunk["hora_fechamento"] = (
        chunk["hora_fechamento"].str[0:2]
        + ":"
        + chunk["hora_fechamento"].str[2:4]
        + ":"
        + chunk["hora_fechamento"].str[4:6]
        + "."
        + chunk["hora_fechamento"].str[6:]
    )

    chunk = chunk[br_b3_cotacoes_constants.ORDEM.value]

    return chunk


# ------- macro etapa 3 abrindo e concatenando o chunk
# ------- read files
def read_files(file_path, chunk_size: int = 100000):
    # Crie um DataFrame vazio para armazenar os chunks processados
    processed_data = pd.DataFrame()

    # Use um loop para iterar sobre os chunks do arquivo
    for chunk in tqdm(
        pd.read_csv(file_path, sep=";", encoding="latin-1", chunksize=chunk_size)
    ):
        processed_chunk = process_chunk(chunk)
        processed_data = pd.concat(
            [processed_data, processed_chunk], ignore_index=True
        )  # Concatene os chunks processados
        partition_data(
            processed_data,
            "data_referencia",
            br_b3_cotacoes_constants.B3_PATH_OUTPUT.value,
        )

        return br_b3_cotacoes_constants.B3_PATH_OUTPUT.value


# ------- macro etapa 4 particionando os arquivos por data
# ------- partition data
def partition_data(df: pd.DataFrame, column_name: list[str], output_directory: str):
    """
    Particiona os dados em subconjuntos de acordo com os valores únicos de uma coluna.
    Salva cada subconjunto em um arquivo CSV separado.
    df: DataFrame a ser particionado
    column_name: nome da coluna a ser usada para particionar os dados
    output_directory: diretório onde os arquivos CSV serão salvos
    """
    unique_values = df[column_name].unique()
    log(f"Valores únicos: {unique_values}")
    for value in unique_values:
        value_str = str(value)[:10]
        date_value = datetime.strptime(value_str, "%Y-%m-%d").date()
        log(date_value)
        formatted_value = date_value.strftime("%Y-%m-%d")
        log(formatted_value)
        partition_path = os.path.join(
            output_directory, f"{column_name}={formatted_value}"
        )
        log(f"Salvando dados em {partition_path}")
        if not os.path.exists(partition_path):
            os.makedirs(partition_path)
        df_partition = df[df[column_name] == value].copy()
        df_partition.drop([column_name], axis=1, inplace=True)
        log(f"df_partition: {df_partition}")
        csv_path = os.path.join(partition_path, "data.csv")
        df_partition.to_csv(csv_path, index=False, encoding="utf-8", na_rep="")
        log(f"Arquivo {csv_path} salvo com sucesso!")

        return csv_path
