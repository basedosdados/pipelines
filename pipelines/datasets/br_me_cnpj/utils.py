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

# from pipelines.utils.tasks import dump_batches_to_file


ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value


def data_url(url, headers):
    link_data = requests.get(url, headers=headers)
    soup = BeautifulSoup(link_data.text, "html.parser")
    span_element = soup.find_all("td", align="right")
    data_completa = span_element[1].text.strip()
    data_str = data_completa[0:10]
    data = datetime.strptime(data_str, "%Y-%m-%d")

    return data


def convert_columns_to_string(df, columns):
    for col in columns:
        df[col].fillna("", inplace=True)
        if col in df.columns and df[col].notna().all():
            df[col] = df[col].astype(str)
    return df


def destino_output(tabela, sufixo, data_coleta):
    output_path = f"/tmp/data/br_me_cnpj/output/{sufixo}/"
    # Pasta de destino para salvar o arquivo CSV
    if tabela != "Simples":
        if tabela != "Estabelecimentos":
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


def processa_tabela(tabela, arquivos_baixados, data_coleta):
    sufixo = tabela.lower()

    if tabela in arquivos_baixados:
        return arquivos_baixados

    pasta_destino = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
    os.makedirs(pasta_destino, exist_ok=True)
    log("Pasta destino input construído")

    output_path = destino_output(tabela, sufixo, data_coleta)
    log(output_path)

    for i in range(1, 10):
        nome_arquivo = f"{tabela}{i}"
        url_download = f"https://dadosabertos.rfb.gov.br/CNPJ/{tabela}{i}.zip"
        # tamanho_lote = 4  # Definindo o tamanho do lote desejado
        # lote_atual = 0
        # df_lote = pd.DataFrame()
        if nome_arquivo not in arquivos_baixados:
            arquivos_baixados.append(nome_arquivo)
            caminho_arquivo_zip = baixa_arquivo_zip(
                url_download, pasta_destino, nome_arquivo
            )
            caminho_arquivo_csv = extract_estabelecimentos(
                caminho_arquivo_zip, pasta_destino
            )

        if caminho_arquivo_csv:
            chunksize = 150000  # Tamanho do lote

            # Lê o arquivo CSV em partes de tamanho chunksize
            for df_arquivo_chunk in pd.read_csv(
                caminho_arquivo_csv,
                encoding="iso-8859-1",
                sep=";",
                header=None,
                chunksize=chunksize,
            ):
                tamanho_lote = 150000  # Tamanho do lote para processamento
                lotes_arquivo = [
                    df_arquivo_chunk[start : start + tamanho_lote]
                    for start in range(0, len(df_arquivo_chunk), tamanho_lote)
                ]

                for lote in lotes_arquivo:
                    lote_processado = trata_dataframe(lote)  # Processa o lote
                    escreve_particoes_parquet(
                        lote_processado, output_path, data_coleta, i
                    )  # Salva em Parquet

                # df_arquivo = pd.read_csv(caminho_arquivo_csv, encoding="iso-8859-1", sep=";", header=None)

                # # Dividir o df_arquivo em lotes menores
                # tamanho_lote_arquivo = len(df_arquivo) // tamanho_lote
                # lotes_arquivo = [df_arquivo[l:l+tamanho_lote_arquivo] for l in range(0, len(df_arquivo), tamanho_lote_arquivo)]

                # for lote in lotes_arquivo:
                #     df_lote = pd.concat([df_lote, trata_dataframe(lote)])  # Adiciona o lote tratado ao lote maior
                #     realiza_acao_lote(lote_atual)
                #     lote_atual += 1
                #     if lote_atual == tamanho_lote:
                #         escreve_particoes_parquet(df_lote, output_path, data_coleta, i)
                #         df_lote = pd.DataFrame()  # Limpa o lote
                #         lote_atual = 0
                #         break

    return output_path


def realiza_acao_lote(numero_lote):
    # Esta função será chamada após processar cada lote de arquivos CSV
    log(f"Ação de lote realizada para o lote {numero_lote}")


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


def trata_dataframe(df):
    df.columns = constants_cnpj.COLUNAS_ESTABELECIMENTO.value
    col = [
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "id_pais",
        "data_situacao_especial",
    ]
    convert_columns_to_string(df, col)
    return df


def escreve_particoes_parquet(df, output_path, data_coleta, i):
    for uf in ufs:
        df_particao = df[df["sigla_uf"] == uf].copy()
        df_particao.drop(["sigla_uf"], axis=1, inplace=True)
        particao = f"{output_path}data={data_coleta}/sigla_uf={uf}/estabelecimentos_{i}.parquet"
        df_particao.to_parquet(particao, index=False)
    log(f"Arquivo de estabelecimentos_{i} baixado")
