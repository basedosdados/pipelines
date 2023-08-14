# -*- coding: utf-8 -*-
"""
Tasks for br_me_cnpj
"""
###############################################################################
from pipelines.utils.utils import extract_last_date, log

from prefect import task
from pipelines.datasets.br_me_cnpj.constants import (
    constants as constants_cnpj,
)
from pipelines.datasets.br_me_cnpj.utils import (
    data_url,
    download_unzip_csv,
    process_csv_partition,
    destino_output,
    partition_parquet,
    extract_estabelecimentos,
)
import os
import requests
import zipfile
import pandas as pd
from datetime import datetime  # Importe diretamente a função strptime
from tqdm import tqdm


ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value


@task
def check_for_updates(dataset_id, table_id):
    # Dado que data_url e extract_last_date retornam um objeto datetime
    data_obj = data_url(url, headers).strftime("%Y-%m-%d")
    data_bq_obj = extract_last_date(
        dataset_id, table_id, "yy-mm-dd", "basedosdados-dev"
    ).strftime("%Y-%m-%d")
    log(f"Última data do site: {data_obj}")

    # Compara as datas
    if data_obj > data_bq_obj:
        return True
    else:
        return False


@task
def main(tabelas):
    arquivos_baixados = []  # Serve para checar o download
    data_coleta = data_url(url, headers).date()

    tabela = tabelas[0]  # Acessa o único elemento da lista
    sufixo = tabela.lower()

    # Destino input
    input_path = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
    os.makedirs(input_path, exist_ok=True)
    log("Pasta destino input construído")
    # Destino output
    output_path = destino_output(tabela, sufixo, data_coleta)

    for i in range(0, 10):
        nome_arquivo = f"{tabela}{i}"
        url_download = f"https://dadosabertos.rfb.gov.br/CNPJ/{tabela}{i}.zip"
        if nome_arquivo not in arquivos_baixados:
            arquivos_baixados.append(
                nome_arquivo
            )  # Adiciona o nome à lista de baixados
            download_unzip_csv(url_download, data_coleta, input_path)
            process_csv_partition(input_path, output_path, data_coleta, i)

    return output_path


@task
def clean_data_make_partitions_empresas(caminhos_arquivos_zip):
    # Data do upload dos dados
    data_coleta = data_url(url, headers).date()
    # Pasta de destino para salvar o arquivo CSV
    output_path = "/tmp/data/br_me_cnpj/output/empresas/"

    # Pasta de destino para salvar o arquivo CSV
    output_dir = f"/tmp/data/br_me_cnpj/output/empresas/data={data_coleta}/"
    # Certifica-se de que o diretório de destino existe ou cria o diretório se não existir
    os.makedirs(output_dir, exist_ok=True)

    for i, caminho_arquivo_zip in enumerate(caminhos_arquivos_zip):
        pasta_destino = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
        # caminho_arquivo_zip = os.path.join(pasta_destino, f"Empresas{i}.zip")
        caminho_arquivo_csv = None

        with zipfile.ZipFile(caminho_arquivo_zip, "r") as z:
            for nome_arquivo in z.namelist():
                if nome_arquivo.lower().endswith("csv"):
                    caminho_arquivo_csv = os.path.join(pasta_destino, nome_arquivo)
                    with open(caminho_arquivo_csv, "wb") as f:
                        f.write(z.read(nome_arquivo))
                    log(f"Arquivo CSV '{nome_arquivo}' extraído com sucesso.")
                    break

        # Verifica se foi encontrado um arquivo CSV dentro do ZIP
        if caminho_arquivo_csv is None:
            log("Nenhum arquivo CSV foi encontrado dentro do arquivo ZIP.")

        if caminho_arquivo_csv:
            # Ler o arquivo CSV e realizar o tratamento necessário
            df = pd.read_csv(
                caminho_arquivo_csv, encoding="iso-8859-1", sep=";", header=None
            )

            # Renomeando as colunas
            df.columns = constants_cnpj.COLUNAS_EMPRESAS.value

            # Export the processed DataFrame to a CSV file
            df.to_csv(f"{output_dir}empresas_{i}.csv", index=False)
            log(f"Arquivo CSV de empresa_{i} salvo")

            # Remover o arquivo CSV extraído da pasta de input
            os.remove(caminho_arquivo_csv)
    return output_path


@task
def clean_data_make_partitions_socios(caminhos_arquivos_zip):
    # Data do upload dos dados
    data_coleta = data_url(url, headers).date()
    # Pasta de destino para salvar o arquivo CSV
    output_path = "/tmp/data/br_me_cnpj/output/socios/"
    output_dir = f"/tmp/data/br_me_cnpj/output/socios/data={data_coleta}/"
    # Certifica-se de que o diretório de destino existe ou cria o diretório se não existir
    os.makedirs(output_dir, exist_ok=True)

    for i, caminho_arquivo_zip in enumerate(caminhos_arquivos_zip):
        pasta_destino = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
        # caminho_arquivo_zip = os.path.join(pasta_destino, f"Socios{i}.zip")
        caminho_arquivo_csv = None
        with zipfile.ZipFile(caminho_arquivo_zip, "r") as z:
            for nome_arquivo in z.namelist():
                if nome_arquivo.lower().endswith("csv"):
                    caminho_arquivo_csv = os.path.join(pasta_destino, nome_arquivo)
                    with open(caminho_arquivo_csv, "wb") as f:
                        f.write(z.read(nome_arquivo))
                    log(f"Arquivo CSV '{nome_arquivo}' extraído com sucesso.")
                    break

        # Verifica se foi encontrado um arquivo CSV dentro do ZIP
        if caminho_arquivo_csv is None:
            log("Nenhum arquivo CSV foi encontrado dentro do arquivo ZIP.")

        if caminho_arquivo_csv:
            # Agora, você pode ler o arquivo CSV e realizar o tratamento necessário
            df = pd.read_csv(
                caminho_arquivo_csv, encoding="iso-8859-1", sep=";", header=None
            )

            # Renomear colunas
            df.columns = constants_cnpj.COLUNAS_SOCIOS.value

            log(f"Tratamento finalizado para a tabela socios_{i}")

            # Export the processed DataFrame to a CSV file
            df.to_csv(f"{output_dir}socios_{i}.csv", index=False)
            log(f"Arquivo csv de socios_{i} baixado")

            # Remover o arquivo CSV extraído da pasta de input
            os.remove(caminho_arquivo_csv)
    return output_path


@task
def clean_data_make_partitions_simples(caminhos_arquivos_zip):
    # Data do upload dos dados
    data_coleta = data_url(url, headers).date()

    # Pasta de destino para salvar o arquivo CSV
    output_dir = "/tmp/data/br_me_cnpj/output/simples/"

    # Certifica-se de que o diretório de destino existe ou cria o diretório se não existir
    os.makedirs(output_dir, exist_ok=True)

    pasta_destino = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
    caminho_arquivo_zip = caminhos_arquivos_zip[0]
    caminho_arquivo_csv = None

    with zipfile.ZipFile(caminho_arquivo_zip, "r") as z:
        for nome_arquivo in z.namelist():
            if "simples.csv" in nome_arquivo.lower():
                caminho_arquivo_csv = os.path.join(pasta_destino, nome_arquivo)
                with open(caminho_arquivo_csv, "wb") as f:
                    f.write(z.read(nome_arquivo))
                log(f"Arquivo CSV '{nome_arquivo}' extraído com sucesso'.")
                break

    # Verifica se foi encontrado um arquivo CSV dentro do ZIP
    if caminho_arquivo_csv is None:
        log("Nenhum arquivo CSV foi encontrado dentro do arquivo ZIP.")

    if caminho_arquivo_csv:
        # Ler o arquivo CSV e realizar o tratamento necessário
        df = pd.read_csv(
            caminho_arquivo_csv, encoding="iso-8859-1", sep=";", header=None
        )
        # Renomeando as colunas
        df.columns = constants_cnpj.COLUNAS_SIMPLES.value
        # Realiza as transformações nos dados
        df["opcao_simples"] = df["opcao_simples"].replace({"N": "0", "S": "1"})
        df["opcao_mei"] = df["opcao_mei"].replace({"N": "0", "S": "1"})

        for col in df.columns:
            if col.startswith("data_"):
                df[col] = df[col].replace({"0": "", "00000000": ""})
                df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce")

        # Export the processed DataFrame to a CSV file
        output_csv = f"{output_dir}simples.csv"
        df.to_csv(output_csv, index=False)
        log("Arquivo csv de simples baixado")
        # Remover o arquivo CSV extraído da pasta de input
        os.remove(caminho_arquivo_csv)
    return output_dir
