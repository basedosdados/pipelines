# -*- coding: utf-8 -*-
"""
General purpose functions for the br_me_cnpj project
"""
import os
import time
import zipfile
from asyncio import Semaphore, gather
from datetime import datetime

from httpx import AsyncClient, HTTPError, head
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import requests
from bs4 import BeautifulSoup
from loguru import logger
from requests.exceptions import ConnectionError, Timeout
from tqdm import tqdm

from pipelines.datasets.br_me_cnpj.constants import constants as constants_cnpj
from pipelines.utils.utils import log

ufs = constants_cnpj.UFS.value
headers = constants_cnpj.HEADERS.value
timeout = constants_cnpj.TIMEOUT.value

def data_url(url:str, headers:dict)-> tuple[datetime,datetime]:


    link_data = requests.get(url, headers=headers, timeout=timeout, verify=False)
    link_data.raise_for_status()

    soup = BeautifulSoup(link_data.text, "html.parser")


    max_folder_date = max(
    [
        datetime.strptime(link["href"].strip("/"), "%Y-%m").strftime('%Y-%m')
        for link in soup.find_all("a", href=True)
        if link["href"].strip("/").startswith("202")
    ]
    )

    today_date = datetime.today().strftime('%Y-%m-%d')
    log(f"A data máxima extraida da API da Receita Federal que será utilizada para comparar com os metadados da BD é: {max_folder_date}")
    log(f"A data de hoje gerada para criar partições no Storage é: {today_date} ")

    return max_folder_date, today_date




# ! Cria o caminho do output
def destino_output(sufixo:str, data_coleta: datetime)-> str:

    output_path = f"/tmp/data/br_me_cnpj/output/{sufixo}/"
    # Pasta de destino para salvar o arquivo CSV
    if sufixo != "simples":
        if sufixo != "estabelecimentos":
            output_dir = f"/tmp/data/br_me_cnpj/output/{sufixo}/data={data_coleta}/"
            os.makedirs(output_dir, exist_ok=True)
        else:
            for uf in ufs:
                output_dir = f"/tmp/data/br_me_cnpj/output/estabelecimentos/data={data_coleta}/sigla_uf={uf}/"
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
    else:
        output_dir = output_path
        os.makedirs(output_dir, exist_ok=True)
    log("Pasta destino output construido")
    return output_path


# ! Adiciona zero a esquerda nas colunas
def fill_left_zeros(df: datetime, column, num_digits:int)-> pd.DataFrame:
    df[column] = df[column].astype(str).str.zfill(num_digits)
    return df


# ! Download assincrono e em chunck`s do zip
def chunk_range(content_length: int, chunk_size: int) -> list[tuple[int, int]]:
    """Split the content length into a list of chunk ranges"""
    return [(i, min(i + chunk_size - 1, content_length - 1)) for i in range(0, content_length, chunk_size)]

# from https://stackoverflow.com/a/64283770
async def download(url, chunk_size=20 * 1024 * 1024, max_retries=5, max_parallel=5, timeout=5 * 60):
    try:
        request_head = head(url)
        log(request_head)

        assert request_head.status_code == 200
        assert request_head.headers["accept-ranges"] == "bytes"

        content_length = int(request_head.headers["content-length"])
        log(f"Downloading {url} with {content_length} bytes / {chunk_size} chunks and {max_parallel} parallel downloads")

        semaphore = Semaphore(max_parallel)

        async with AsyncClient() as client:
            tasks = [
                download_chunk(client, url, (start, end), max_retries, timeout, semaphore)
                for start, end in chunk_range(content_length, chunk_size)
            ]
            return b"".join(await gather(*tasks))

    except HTTPError as e:
        log(f"Initial request failed: {e}")
        return b""


async def download_chunk(client, url, chunk_range, max_retries, timeout, semaphore):
    async with semaphore:
        for attempt in range(max_retries):
            try:
                headers = {"Range": f"bytes={chunk_range[0]}-{chunk_range[1]}"}
                response = await client.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                return response.content
            except HTTPError as e:
                delay = 2 ** attempt
                log(f"Chunk {chunk_range} failed on attempt {attempt + 1}. Retrying in {delay} seconds...")
                await asyncio.sleep(delay)

        raise HTTPError(f"Chunk {chunk_range} failed after {max_retries} retries")


# ! Executa o download do zip file
async def download_unzip_csv(url, pasta_destino):
    log(f"Baixando o arquivo {url}")
    save_path = os.path.join(pasta_destino, f"{os.path.basename(url)}.zip")
    content = await download(url)
    with open(save_path, "wb") as fd:
        fd.write(content)

    try:
        with zipfile.ZipFile(save_path) as z:
            z.extractall(pasta_destino)
        log("Dados extraídos com sucesso!")
    except zipfile.BadZipFile:
        log(f"O arquivo {os.path.basename(url)} não é um arquivo ZIP válido.")

    os.remove(save_path)



# ! Salva os dados CSV Estabelecimentos
def process_csv_estabelecimentos(
    input_path: str, output_path: str, data_coleta: str, i: int, chunk_size: int = 1000
):
    ordem = constants_cnpj.COLUNAS_ESTABELECIMENTO_ORDEM.value
    colunas = constants_cnpj.COLUNAS_ESTABELECIMENTO.value
    save_path = f"{output_path}data={data_coleta}/"
    for nome_arquivo in os.listdir(input_path):
        if "estabele" in nome_arquivo.lower():
            caminho_arquivo_csv = os.path.join(input_path, nome_arquivo)
            log(f"Carregando o arquivo: {nome_arquivo}")
            for chunk in tqdm(
                pd.read_csv(
                    caminho_arquivo_csv,
                    encoding="iso-8859-1",
                    sep=";",
                    header=None,
                    names=colunas,
                    dtype=str,
                    chunksize=chunk_size,
                ),
                desc="Lendo o arquivo CSV",
            ):
                # Arrumando as colunas datas
                date_cols = [
                    "data_situacao_cadastral",
                    "data_inicio_atividade",
                    "data_situacao_especial",
                ]
                chunk[date_cols] = chunk[date_cols].apply(
                    pd.to_datetime, format="%Y%m%d", errors="coerce"
                )
                chunk[date_cols] = chunk[date_cols].apply(
                    lambda x: x.dt.strftime("%Y-%m-%d")
                )

                # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
                chunk = fill_left_zeros(chunk, "cnpj_basico", 8)
                # Preenchimento de zeros à esquerda no campo 'cnpj_ordem'
                chunk = fill_left_zeros(chunk, "cnpj_ordem", 4)
                # Preenchimento de zeros à esquerda no campo 'cnpj_dv'
                chunk = fill_left_zeros(chunk, "cnpj_dv", 2)
                # Gerando a coluna 'cnpj' e 'id_municipio'
                chunk["cnpj"] = (
                    chunk["cnpj_basico"] + chunk["cnpj_ordem"] + chunk["cnpj_dv"]
                )
                chunk["id_municipio"] = ""
                chunk = chunk.loc[:, ordem]
                for uf in ufs:
                    df_particao = chunk[chunk["sigla_uf"] == uf].copy()
                    df_particao.drop(["sigla_uf"], axis=1, inplace=True)
                    particao_path = os.path.join(save_path, f"sigla_uf={uf}")
                    particao_filename = f"estabelecimentos_{i}.csv"
                    particao_file_path = os.path.join(particao_path, particao_filename)

                    mode = "a" if os.path.exists(particao_file_path) else "w"
                    df_particao.to_csv(
                        particao_file_path,
                        index=False,
                        encoding="iso-8859-1",
                        mode=mode,
                        header=mode == "w",
                    )

            log(f"Arquivo estabelecimento_{i} salvo")
            os.remove(caminho_arquivo_csv)


# ! Salva os dados CSV Empresas
def process_csv_empresas(
    input_path: str, output_path: str, data_coleta: str, i: int, chunk_size: int = 1000
):
    colunas = constants_cnpj.COLUNAS_EMPRESAS.value
    save_path = f"{output_path}data={data_coleta}/empresas_{i}.csv"
    for nome_arquivo in os.listdir(input_path):
        if nome_arquivo.lower().endswith("csv"):
            caminho_arquivo_csv = os.path.join(input_path, nome_arquivo)
            log(f"Carregando o arquivo: {nome_arquivo}")
            with open(os.path.join(save_path), "wb") as fd:
                for chunk in tqdm(
                    pd.read_csv(
                        caminho_arquivo_csv,
                        encoding="iso-8859-1",
                        sep=";",
                        header=None,
                        names=colunas,
                        dtype=str,
                        chunksize=chunk_size,
                    ),
                    desc="Lendo o arquivo CSV",
                ):
                    # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
                    chunk = fill_left_zeros(chunk, "cnpj_basico", 8)
                    # Preenchimento de zeros à esquerda no campo 'natureza_juridica'
                    chunk = fill_left_zeros(chunk, "natureza_juridica", 4)
                    # Convertendo a coluna 'capital_social' para float e mudando o separator
                    chunk["capital_social"] = (
                        chunk["capital_social"].str.replace(",", ".").astype(float)
                    )

                    chunk.to_csv(fd, index=False, encoding="iso-8859-1")

            log(f"Arquivo empresas_{i} salvo")
            os.remove(caminho_arquivo_csv)


# ! Salva os dados CSV Socios
def process_csv_socios(
    input_path: str, output_path: str, data_coleta: str, i: int, chunk_size: int = 1000
):
    colunas = constants_cnpj.COLUNAS_SOCIOS.value
    save_path = f"{output_path}data={data_coleta}/socios_{i}.csv"
    for nome_arquivo in os.listdir(input_path):
        if nome_arquivo.lower().endswith("csv"):
            caminho_arquivo_csv = os.path.join(input_path, nome_arquivo)
            log(f"Carregando o arquivo: {nome_arquivo}")
            with open(os.path.join(save_path), "wb") as fd:
                for chunk in tqdm(
                    pd.read_csv(
                        caminho_arquivo_csv,
                        encoding="iso-8859-1",
                        sep=";",
                        header=None,
                        names=colunas,
                        dtype=str,
                        chunksize=chunk_size,
                    ),
                    desc="Lendo o arquivo CSV",
                ):
                    # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
                    chunk = fill_left_zeros(chunk, "cnpj_basico", 8)
                    # Retirando valores de cpf's nulos
                    chunk["cpf_representante_legal"] = chunk[
                        "cpf_representante_legal"
                    ].replace("***000000**", "")
                    # Ajustando a coluna data
                    for col in chunk.columns:
                        if col.startswith("data_"):
                            chunk[col] = chunk[col].replace("0", "")
                            chunk[col] = pd.to_datetime(
                                chunk[col], format="%Y%m%d", errors="coerce"
                            )

                    chunk.to_csv(fd, index=False, encoding="iso-8859-1")

            log(f"Arquivo socios_{i} salvo")
            os.remove(caminho_arquivo_csv)


# ! Salva os dados CSV Simples
def process_csv_simples(
    input_path: str,
    output_path: str,
    data_coleta: str,
    sufixo: str,
    chunk_size: int = 1000,
):
    colunas = constants_cnpj.COLUNAS_SIMPLES.value
    save_path = f"{output_path}{sufixo}.csv"
    for nome_arquivo in os.listdir(input_path):
        if "simples.csv" in nome_arquivo.lower():
            caminho_arquivo_csv = os.path.join(input_path, nome_arquivo)
            log(f"Carregando o arquivo: {nome_arquivo}")
            with open(os.path.join(save_path), "wb") as fd:
                for chunk in tqdm(
                    pd.read_csv(
                        caminho_arquivo_csv,
                        encoding="iso-8859-1",
                        sep=";",
                        header=None,
                        names=colunas,
                        dtype=str,
                        chunksize=chunk_size,
                    ),
                    desc="Lendo o arquivo CSV",
                ):
                    for col in chunk.columns:
                        if col.startswith("data_"):
                            chunk[col] = chunk[col].replace({"0": "", "00000000": ""})
                            chunk[col] = pd.to_datetime(
                                chunk[col], format="%Y%m%d", errors="coerce"
                            )
                    # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
                    chunk = fill_left_zeros(chunk, "cnpj_basico", 8)
                    # Transformando colunas em dummy
                    chunk["opcao_simples"] = chunk["opcao_simples"].replace(
                        {"N": "0", "S": "1"}
                    )
                    chunk["opcao_mei"] = chunk["opcao_mei"].replace(
                        {"N": "0", "S": "1"}
                    )
                    chunk.to_csv(fd, index=False, encoding="iso-8859-1")

            log(f"Arquivo {sufixo} salvo")
            os.remove(caminho_arquivo_csv)
