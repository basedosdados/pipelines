# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_estban project
"""
import requests
import pandas as pd
import numpy as np
import os
import zipfile
from tqdm import tqdm
from datetime import datetime
from pipelines.utils.utils import log
from pipelines.datasets.br_b3_cotacoes.constants import (
    constants as br_b3_cotacoes_constants,
)

# ------- macro etapa 1 download de dados com chunk
# ------- download and unzip csv


def download_chunk_and_unzip_csv(url, path, chunk_size: int = 1000):
    print(f"Baixando o arquivo {url}")
    os.system(f"mkdir -p {path}")
    save_path = os.path.join(path, f"{os.path.basename(url)}.zip")
    r = requests.get(url, stream=True, timeout=60)
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


# ------- macro etapa 3 particionando os arquivos por data
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
    for value in unique_values:
        value_str = str(value)[:10]
        date_value = datetime.strptime(value_str, "%Y-%m-%d").date()

        formatted_value = date_value.strftime("%Y-%m-%d")

        partition_path = os.path.join(
            output_directory, f"{column_name}={formatted_value}"
        )

        if not os.path.exists(partition_path):
            os.makedirs(partition_path)

        df_partition = df[df[column_name] == value].copy()

        df_partition.drop([column_name], axis=1, inplace=True)

        csv_path = os.path.join(partition_path, "data.csv")
        mode = "a" if os.path.exists(csv_path) else "w"
        df_partition.to_csv(
            csv_path,
            sep=",",
            index=False,
            encoding="utf-8",
            na_rep="",
            mode=mode,
            header=mode == "w",
        )


# ------- macro etapa 2 tratando os dados através do chunk
# ------- process chunk
def process_chunk_csv(input_path, chunk_size: int = 100000):
    log(
        "********************************ABRINDO O ARQUIVO********************************"
    )
    caminho_arquivo_csv = os.path.join(input_path)
    print(f"caminho_arquivo_csv: {caminho_arquivo_csv}")

    for chunk in tqdm(
        pd.read_csv(
            caminho_arquivo_csv,
            sep=";",
            encoding="utf-8",
            chunksize=chunk_size,
            dtype=str,
        ),
        desc="lendo o arquivo CSV",
    ):
        chunk.rename(columns=br_b3_cotacoes_constants.RENAME.value, inplace=True)
        chunk = chunk.replace(np.nan, "")
        chunk["codigo_participante_vendedor"] = chunk[
            "codigo_participante_vendedor"
        ].apply(lambda x: str(x).replace(".0", ""))
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

        partition_data(
            chunk,
            column_name="data_referencia",
            output_directory=br_b3_cotacoes_constants.B3_PATH_OUTPUT.value,
        )

    # os.remove(caminho_arquivo_csv)
    return chunk
