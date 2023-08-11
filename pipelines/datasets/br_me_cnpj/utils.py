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
    urls, data_coleta, zips=None, chunk_size: int = 1000, mkdir: bool = True
):
    if mkdir:
        pasta_destino = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
        os.makedirs(pasta_destino, exist_ok=True)
        log("Pasta destino input construído")

    if isinstance(urls, list):
        for url, file in zip(urls, zips):
            log(f"Baixando o arquivo {file}")
            download_url = url
            save_path = os.path.join(pasta_destino, f"{file}.zip")

            r = requests.get(download_url, headers=headers, stream=True, timeout=50)
            with open(save_path, "wb") as fd:
                for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
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
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
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
def parquet_partition(input_path, output_path, data_coleta, i, chunk_size: int = 10000):
    columns = constants_cnpj.COLUNAS_ESTABELECIMENTO.value
    df_columns = pd.DataFrame(columns=columns)
    for nome_arquivo in os.listdir(input_path):
        if "estabele" in nome_arquivo.lower():
            caminho_arquivo_csv = os.path.join(input_path, nome_arquivo)
            log(f"Carregando o arquivo: {nome_arquivo}")
            for chunk_df in tqdm(
                pd.read_csv(
                    caminho_arquivo_csv,
                    encoding="iso-8859-1",
                    sep=";",
                    header=None,
                    dtype=str,
                    chunksize=chunk_size,
                )
            ):
                # Processar o chunk_df
                df_columns = pd.concat([df_columns, chunk_df], ignore_index=True)

            log("Leu todo dataframe")
            escreve_particoes_parquet(df_columns, output_path, data_coleta, i)
            log("Partição feita.")
            os.remove(caminho_arquivo_csv)


def escreve_particoes_parquet(df, output_path, data_coleta, i):
    for uf in ufs:
        for situacao in situacoes_cadastrais:
            df_particao = df[
                (df["sigla_uf"] == uf) & (df["situacao_cadastral"] == situacao)
            ].copy()
            df_particao.drop(["sigla_uf", "situacao_cadastral"], axis=1, inplace=True)
            particao = f"{output_path}data={data_coleta}/sigla_uf={uf}/situacao_cadastral={situacao}/estabelecimentos_{i}.parquet"
            df_particao.to_parquet(particao, index=False)
        log(f"Arquivo de estabelecimentos_{i} salvo, para o estado: {uf}")

    log(f"Arquivo de estabelecimentos_{i} tratado")


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


def baixa_arquivo_zip(url, pasta_destino, nome_arquivo):
    response_zip = requests.get(url)
    if response_zip.status_code == 200:
        caminho_arquivo_zip = os.path.join(pasta_destino, f"{nome_arquivo}.zip")
        with open(caminho_arquivo_zip, "wb") as f:
            f.write(response_zip.content)
        log(f"Arquivo {nome_arquivo} ZIP baixado com sucesso.")
        return caminho_arquivo_zip
    else:
        log(f"Falha ao baixar o arquivo ZIP: {nome_arquivo}")
        return None


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


# def realiza_acao_lote(numero_lote):
#     # Esta função será chamada após processar cada lote de arquivos CSV
#     log(f"Ação de lote realizada para o lote {numero_lote}")
