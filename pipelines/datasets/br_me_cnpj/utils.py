# -*- coding: utf-8 -*-
"""
General purpose functions for the br_me_cnpj project
"""

###############################################################################
from pipelines.datasets.br_me_cnpj.constants import (
    constants as constants_cnpj,
)
from pipelines.utils.utils import log
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os
import zipfile
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
import csv
from typing import List

# from pipelines.utils.tasks import dump_batches_to_file


ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value
situacoes_cadastrais = constants_cnpj.SITUACOES_CADASTRAIS.value


def data_url(url, headers):
    link_data = requests.get(url, headers=headers)
    soup = BeautifulSoup(link_data.text, "html.parser")
    span_element = soup.find_all("td", align="right")
    data_completa = span_element[1].text.strip()
    data_str = data_completa[0:10]
    data = datetime.strptime(data_str, "%Y-%m-%d")

    return data


def download_unzip_csv(
    urls, data_coleta, pasta_destino, zips=None, chunk_size: int = 1000
):
    if isinstance(urls, list):
        for url, file in zip(urls, zips):
            log(f"Baixando o arquivo {file}")
            download_url = url
            save_path = os.path.join(pasta_destino, f"{file}.zip")

            r = requests.get(download_url, headers=headers, stream=True, timeout=50)
            with open(save_path, "wb") as fd:
                for chunk in tqdm(
                    r.iter_content(chunk_size=chunk_size), desc="Baixando o arquivo"
                ):
                    fd.write(chunk)

            try:
                with zipfile.ZipFile(save_path) as z:
                    z.extractall(pasta_destino)
                log("Dados extraídos com sucesso!")

            except zipfile.BadZipFile:
                log(f"O arquivo {file} não é um arquivo ZIP válido.")

            os.system(
                f'cd {pasta_destino}; find . -type f ! -iname "*ESTABELE" -delete'
            )
    elif isinstance(urls, str):
        log(f"Baixando o arquivo {urls}")
        download_url = urls
        save_path = os.path.join(pasta_destino, f"{zips}.zip")

        r = requests.get(download_url, headers=headers, stream=True, timeout=10)
        with open(save_path, "wb") as fd:
            for chunk in tqdm(
                r.iter_content(chunk_size=chunk_size), desc="Baixando o arquivo"
            ):
                fd.write(chunk)

        try:
            with zipfile.ZipFile(save_path) as z:
                z.extractall(pasta_destino)
            log("Dados extraídos com sucesso!")

        except zipfile.BadZipFile:
            log(f"O arquivo {zips} não é um arquivo ZIP válido.")

        os.system(f'cd {pasta_destino}; find . -type f ! -iname "*ESTABELE" -delete')

    else:
        raise ValueError("O argumento 'files' possui um tipo inadequado.")


# ! Particionando os dados em parquet
def process_csv_partition_parquet(
    input_path: str, output_path: str, data_coleta: str, i: int, chunk_size: int = 1000
):
    colunas = constants_cnpj.COLUNAS_ESTABELECIMENTO.value
    for nome_arquivo in os.listdir(input_path):
        if "estabele" in nome_arquivo.lower():
            caminho_arquivo_csv = os.path.join(input_path, nome_arquivo)
            log(f"Carregando o arquivo: {nome_arquivo}")
            df_list = []
            for chunk_df in tqdm(
                pd.read_csv(
                    caminho_arquivo_csv,
                    encoding="iso-8859-1",
                    sep=";",
                    header=None,
                    names=colunas,
                    dtype=str,
                    chunksize=10000,
                ),
                desc="Lendo o arquivo CSV",
            ):
                # Processar o chunk_df
                df_list.append(chunk_df)
            log("Leu todo o CSV")
            df_columns = pd.concat(df_list)
            log("Juntou todos os chucks")
            partition_parquet(df_columns, output_path, data_coleta, i)
            log("Partição feita.")
            os.remove(caminho_arquivo_csv)
            del df_list


def partition_parquet(df, output_path, data_coleta, i):
    for uf in ufs:
        for situacao in situacoes_cadastrais:
            df_particao = df[
                (df["sigla_uf"] == uf) & (df["situacao_cadastral"] == situacao)
            ].copy()
            df_particao.drop(["sigla_uf", "situacao_cadastral"], axis=1, inplace=True)
            particao = f"{output_path}data={data_coleta}/sigla_uf={uf}/situacao_cadastral={situacao}/estabelecimentos_{i}.parquet"
            df_particao.to_parquet(particao, index=False)
        log(f"Arquivo de estabelecimentos_{i} salvo para o estado: {uf}")

    log(f"Arquivo de estabelecimentos_{i} particionado")


def destino_output(tabela, sufixo, data_coleta):
    output_path = f"/tmp/data/br_me_cnpj/output/{sufixo}/"
    # Pasta de destino para salvar o arquivo CSV
    if tabela != "Simples":
        if tabela != "Estabelecimentos":
            output_dir = f"/tmp/data/br_me_cnpj/output/{sufixo}/data={data_coleta}/"
            os.makedirs(output_dir, exist_ok=True)
        else:
            for uf in ufs:
                for situacao in situacoes_cadastrais:
                    output_dir = f"/tmp/data/br_me_cnpj/output/estabelecimentos/data={data_coleta}/sigla_uf={uf}/situacao_cadastral={situacao}/"
                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir)
    else:
        output_dir = output_path
        os.makedirs(output_dir, exist_ok=True)
    log("Pasta destino output construido")
    return output_path


def extract_estabelecimentos(caminho_arquivo_zip, pasta_destino):
    # Extraindo dados
    caminho_arquivo_csv = None
    with zipfile.ZipFile(caminho_arquivo_zip, "r") as z:
        for nome_arquivo in z.namelist():
            if "estabele" in nome_arquivo.lower():
                caminho_arquivo_csv = os.path.join(pasta_destino, nome_arquivo)
                with open(caminho_arquivo_csv, "wb") as f:
                    f.write(z.read(nome_arquivo))
                log(f"Arquivo CSV '{nome_arquivo}' extraído com sucesso.")
                os.remove(caminho_arquivo_zip)
                log("Caminho ZIP deletado")
                break

    # Verifica se foi encontrado um arquivo CSV dentro do ZIP
    if caminho_arquivo_csv is None:
        log("Nenhum arquivo CSV foi encontrado dentro do arquivo ZIP.")

    return caminho_arquivo_csv
