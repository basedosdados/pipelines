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
    fill_left_zeros,
    remove_decimal_suffix,
    convert_columns_to_string,
)
import os
import requests
import zipfile
import pandas as pd
from datetime import datetime  # Importe diretamente a função strptime

ufs = constants_cnpj.UFS.value
url = constants_cnpj.URL.value
headers = constants_cnpj.HEADERS.value


@task
def check_for_updates(dataset_id, table_id):
    # Supondo que data_url e extract_last_date retornam um objeto datetime
    data_obj = data_url(url, headers).strftime("%Y-%m-%d")
    data_bq_obj = extract_last_date(
        dataset_id, table_id, "yy-mm-dd", "basedosdados-dev"
    ).strftime("%Y-%m-%d")

    log(f"Última data do site: {data_obj}")
    # Converte as datas para objetos datetime
    # data_obj = datetime.strptime(data, "%Y-%m-%d")
    # data_bq_obj = datetime.strptime(data_bq, "%Y-%m-%d")

    # Compara as datas
    if data_obj > data_bq_obj:
        return True
    else:
        return False


@task
def download_and_save_zip(tabelas):
    # Lista auxiliar para armazenar os arquivos ZIP já baixados e os seus caminhos
    arquivos_baixados = []
    caminhos_arquivos_zip = []
    data_coleta = data_url(url, headers).date()

    for tabela in tabelas:
        # Verifica se o arquivo já foi baixado
        if tabela not in arquivos_baixados:
            # Pasta de destino para salvar o arquivo ZIP e os arquivos extraídos
            pasta_destino = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"

            # Certifica-se de que o diretório de destino existe ou cria o diretório se não existir
            os.makedirs(pasta_destino, exist_ok=True)

            for i in range(1, 2):
                if tabela != "Simples":
                    # URL do arquivo ZIP para download
                    url_download = (
                        f"https://dadosabertos.rfb.gov.br/CNPJ/{tabela}{i}.zip"
                    )
                    nome_arquivo = f"{tabela}{i}"
                else:
                    url_download = f"https://dadosabertos.rfb.gov.br/CNPJ/{tabela}.zip"
                    nome_arquivo = f"{tabela}"

                # Faz o download do arquivo ZIP se ainda não foi baixado
                if nome_arquivo not in arquivos_baixados:
                    response_zip = requests.get(url_download)

                    # Verifica se o download foi bem-sucedido (código 200)
                    if response_zip.status_code == 200:
                        # Define o caminho completo para o arquivo ZIP
                        if tabela != "Simples":
                            caminho_arquivo_zip = os.path.join(
                                pasta_destino, f"{tabela}{i}.zip"
                            )
                        else:
                            caminho_arquivo_zip = os.path.join(
                                pasta_destino, f"{tabela}.zip"
                            )
                        # Salva o arquivo ZIP na pasta de destino
                        with open(caminho_arquivo_zip, "wb") as f:
                            f.write(response_zip.content)
                        log(f"Arquivo {nome_arquivo} ZIP baixado com sucesso.")

                        # Adiciona o arquivo na lista de arquivos baixados e seu caminho
                        arquivos_baixados.append(nome_arquivo)
                        caminhos_arquivos_zip.append(caminho_arquivo_zip)

                    else:
                        log(f"Falha ao baixar o arquivo ZIP: {nome_arquivo}")
    return caminhos_arquivos_zip


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

            # Conversão de colunas para string
            string_columns = ["cnpj_basico", "natureza_juridica"]
            df = convert_columns_to_string(df, string_columns)

            # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
            df = fill_left_zeros(df, "cnpj_basico", 8)
            # Convert 'capital_social' to numeric, handling ',' as decimal separator
            df["capital_social"] = (
                df["capital_social"].str.replace(",", ".").astype(float)
            )

            # Verifica se algum valor é igual a zero
            if (df["natureza_juridica"] == "0").any():
                # Acrescenta zeros à esquerda para ter 4 dígitos
                df["natureza_juridica"] = df["natureza_juridica"].str.zfill(4)

            # Export the processed DataFrame to a CSV file
            df.to_csv(f"{output_dir}empresas_{i}.csv", index=False)
            log(f"Arquivo csv de empresa_{i} baixado")

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

            # Realiza as transformações nos dados
            df["cpf_representante_legal"] = df["cpf_representante_legal"].replace(
                "***000000**", ""
            )

            # Tratamento das colunas de data
            for col in df.columns:
                if col.startswith("data_"):
                    df[col] = df[col].replace("0", "")
                    df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce")

            # Conversão de colunas para string
            string_columns = ["cnpj_basico", "id_pais"]
            df = convert_columns_to_string(df, string_columns)

            # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
            df = fill_left_zeros(df, "cnpj_basico", 8)

            # Remoção do '.0' do campo 'id_pais'
            df = remove_decimal_suffix(df, "id_pais")
            log(f"Tratamento finalizado para o csv socios_{i}")

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

        # Converte a coluna 'cnpj_basico' para string
        df["cnpj_basico"] = df["cnpj_basico"].astype(str)

        # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
        df = fill_left_zeros(df, "cnpj_basico", 8)

        # Export the processed DataFrame to a CSV file
        output_csv = f"{output_dir}simples.csv"
        df.to_csv(output_csv, index=False)
        log("Arquivo csv de simples baixado")
        # Remover o arquivo CSV extraído da pasta de input
        os.remove(caminho_arquivo_csv)
    return output_dir


@task
def clean_data_make_partitions_estabelecimentos(caminhos_arquivos_zip):
    # Data do upload dos dados
    data_coleta = data_url(url, headers).date()
    # Pasta de destino para salvar o arquivo CSV
    output_path = "/tmp/data/br_me_cnpj/output/estabelecimentos/"
    output_dir = f"/tmp/data/br_me_cnpj/output/estabelecimentos/data={data_coleta}/"
    for uf in ufs:
        output = f"/tmp/data/br_me_cnpj/output/estabelecimentos/data={data_coleta}/sigla_uf={uf}/"
        if not os.path.exists(output):
            os.makedirs(output)

    for i, caminho_arquivo_zip in enumerate(caminhos_arquivos_zip):
        pasta_destino = f"/tmp/data/br_me_cnpj/input/data={data_coleta}/"
        caminho_arquivo_csv = None
        with zipfile.ZipFile(caminho_arquivo_zip, "r") as z:
            for nome_arquivo in z.namelist():
                if "estabele" in nome_arquivo.lower():
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

            # Renomear as colunas
            df.columns = constants_cnpj.COLUNAS_ESTABELECIMENTO.value

            # Conversão de colunas para string
            string_columns = [
                "cnpj_basico",
                "cnpj_ordem",
                "cnpj_dv",
                "id_pais",
                "cep",
                "id_municipio_rf",
            ]
            df = convert_columns_to_string(df, string_columns)

            # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
            df = fill_left_zeros(df, "cnpj_basico", 8)

            # Preenchimento de zeros à esquerda no campo 'cnpj_ordem'
            df = fill_left_zeros(df, "cnpj_ordem", 4)

            # Preenchimento de zeros à esquerda no campo 'cnpj_dv'
            df = fill_left_zeros(df, "cnpj_dv", 2)

            # Generate the 'cnpj' column
            df["cnpj"] = df["cnpj_basico"] + df["cnpj_ordem"] + df["cnpj_dv"]
            df = df.sort_values("cnpj_basico")  # Sort by 'cnpj_basico'

            # Format date columns
            date_cols = [
                "data_situacao_cadastral",
                "data_inicio_atividade",
                "data_situacao_especial",
            ]
            df[date_cols] = df[date_cols].apply(
                pd.to_datetime, format="%Y%m%d", errors="coerce"
            )
            df[date_cols] = df[date_cols].apply(lambda x: x.dt.strftime("%Y-%m-%d"))

            # Convert 'email' to lowercase
            df["email"] = df["email"].str.lower()

            # Remoção do '.0' do campo 'id_pais'
            df = remove_decimal_suffix(df, "id_pais")

            # Remoção do '.0' do campo 'cep'
            df = remove_decimal_suffix(df, "cep")

            # Criando a coluna 'id_municipio'
            df["id_municipio"] = ""
            log(f"Tratamento finalizado para o csv estabelecimentos_{i}")

            for uf in ufs:
                df_particao = df[df["sigla_uf"] == uf].copy()
                df_particao.drop(["sigla_uf"], axis=1, inplace=True)
                particao = f"{output_dir}sigla_uf={uf}/estabelecimentos_{i}.csv"  # Modificado para incluir a UF na pasta
                df_particao.to_csv(particao, index=False)
            log(f"Arquivo csv de estabelecimentos_{i} baixado")

            # Remover o arquivo CSV extraído da pasta de input
            os.remove(caminho_arquivo_csv)

    return output_path
