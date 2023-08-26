# -*- coding: utf-8 -*-
"""
General purpose functions for the br_me_cnpj project
"""
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

ufs = constants_cnpj.UFS.value
headers = constants_cnpj.HEADERS.value


# ! Checa a data do site
def data_url(url, headers):
    link_data = requests.get(url, headers=headers)
    soup = BeautifulSoup(link_data.text, "html.parser")
    span_element = soup.find_all("td", align="right")

    # Extrai a segunda ocorrência
    data_completa = span_element[1].text.strip()
    # Extrai a parte da data no formato "YYYY-MM-DD"
    data_str = data_completa[0:10]
    # Converte a string da data em um objeto de data
    data = datetime.strptime(data_str, "%Y-%m-%d")

    return data


# ! Checa a data do site
def data_url_bd(url, headers):
    link_data = requests.get(url, headers=headers)
    soup = BeautifulSoup(link_data.text, "html.parser")

    # Extrai a segunda ocorrência
    data_str = soup.find_all("p", class_="chakra-text css-92vgdp")[1].text
    # Converte a string da data em um objeto de data
    data = datetime.strptime(data_str, "%Y-%m-%d")
    return data


# ! Cria o caminho do output
def destino_output(sufixo, data_coleta):
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
def fill_left_zeros(df, column, num_digits):
    df[column] = df[column].astype(str).str.zfill(num_digits)
    return df


# ! Executa o download do zip file
def download_unzip_csv(url, pasta_destino, chunk_size: int = 1000):
    log(f"Baixando o arquivo {url}")
    save_path = os.path.join(pasta_destino, f"{os.path.basename(url)}.zip")

    r = requests.get(url, headers=headers, stream=True, timeout=60)
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
