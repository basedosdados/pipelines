# -*- coding: utf-8 -*-
from constants import constants

import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from tqdm import tqdm


CNEFE_FILE_NAMES = constants.CNEFE_FILE_NAMES.value
URL = constants.CNEFE_FTP_URL.value


# Função para baixar cada arquivo do FTP
def download_files_from_ftp(url: str) -> BytesIO:
    """
    Baixa um arquivo do FTP e retorna seu conteúdo em memória.

    Parâmetros:
    url (str): URL do arquivo a ser baixado.

    Retorna:
    BytesIO: Conteúdo do arquivo baixado em memória.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 Kibibyte
    t = tqdm(total=total_size, unit='iB', unit_scale=True)
    file_data = BytesIO()
    for data in response.iter_content(block_size):
        t.update(len(data))
        file_data.write(data)
    t.close()
    return file_data

# Função para descompactar e processar o arquivo de São Paulo (SP) em chunks
def process_sp_file(file: BytesIO, uf:str) -> None:
    """
    Descompacta, processa e salva o arquivo de São Paulo (SP) em chunks.

    Parâmetros:
    file (BytesIO): Conteúdo do arquivo ZIP em memória.
    """
    output_dir = f"/tmp/br_ibge_censo_2022/output/sigla_uf={uf}"
    os.makedirs(output_dir, exist_ok=True)
    chunk_number = 0

    with zipfile.ZipFile(file) as z:
        with z.open(z.namelist()[0]) as f:
            for chunk in pd.read_csv(f, chunksize=1000000, sep=';', dtype=str):
                chunk_number += 1
                chunk.to_parquet(os.path.join(output_dir, f"{uf}_chunk_{chunk_number}.parquet"), compression="gzip")
                print(f"Chunk {chunk_number} de {uf} salvo com sucesso.")

# Função para descompactar o arquivo na sessão
def unzip_file_in_session(file: BytesIO) -> pd.DataFrame:
    """
    Descompacta um arquivo ZIP em memória e carrega seu conteúdo em um DataFrame do pandas.

    Parâmetros:
    file (BytesIO): Conteúdo do arquivo ZIP em memória.

    Retorna:
    pd.DataFrame: DataFrame com os dados do arquivo descompactado.
    """
    with zipfile.ZipFile(file) as z:
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f,sep=';', dtype=str)
    return df

# Função para salvar o DataFrame em formato parquet com compressão gzip
def save_parquet(df: pd.DataFrame, mkdir: bool, table_id: str) -> None:
    """
    Salva um DataFrame em formato parquet com compressão gzip em um diretório específico.

    Parâmetros:
    df (pd.DataFrame): DataFrame a ser salvo.
    mkdir (bool): Se True, cria o diretório se ele não existir.
    table_id (str): Identificador da tabela usado para nomear o arquivo e diretório.
    """
    output_dir = f"/tmp/br_ibge_censo_2022/output/sigla_uf={table_id}"
    if mkdir:
        os.makedirs(output_dir, exist_ok=True)
    df.to_parquet(os.path.join(output_dir, f"{table_id}.parquet"), compression="gzip")

# Baixar, descompactar e salvar os arquivos
for uf, filename in CNEFE_FILE_NAMES.items():
    url = f"{URL}/{filename}"
    print(f'----- Baixando o arquivo: {url}')

    try:
        zip_file = download_files_from_ftp(url)
        if uf in ['SP', 'RJ', 'MG', 'BA', 'RS']:
            process_sp_file(zip_file, uf)
        else:
            df = unzip_file_in_session(zip_file)
            save_parquet(df, mkdir=True, table_id=uf)
        print(f"Arquivo {filename} baixado e salvo com sucesso.")
    except Exception as e:
        print(f"Erro ao processar o arquivo {filename}: {e}")
