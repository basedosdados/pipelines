# -*- coding: utf-8 -*-
"""
General purpose functions for the br_anp_precos_combustiveis project
"""
import basedosdados as bd
import pandas as pd
import unidecode
import os
import requests
from pipelines.utils.utils import log
from datetime import datetime
from pipelines.datasets.br_anp_precos_combustiveis.constants import (
    constants as anatel_constants,
)
import numpy as np


def download_files(urls, path):
    """Download files from URLs

    Args:
        urls (list): List of URLs of files to download
        path (str): Directory to save the downloaded files

    Returns:
        list: List of paths to the downloaded files
    """

    os.makedirs(path, exist_ok=True)
    downloaded_files = []

    for url in urls:
        response = requests.get(url)
        if response.status_code == 200:
            file_name = url.split("/")[-1]
            file_path = os.path.join(path, file_name)
            with open(file_path, "wb") as file:
                file.write(response.content)

            downloaded_files.append(file_path)
            log("----" * 150)
            log(f"Arquivo {file_path} com sucesso")
        else:
            raise Exception(
                f"Failed to download from {url}, status code: {response.status_code}"
            )

    return downloaded_files


def get_id_municipio(id_municipio: pd.DataFrame):
    # ! Carregando os dados direto do Diretório de municipio da BD
    # Para carregar o dado direto no pandas
    log("Carregando dados do diretório de municípios da BD")
    id_municipio = bd.read_table(
        dataset_id="br_bd_diretorios_brasil",
        table_id="municipio",
        billing_project_id="basedosdados-dev",
        from_file=True,
    )
    log("----" * 150)
    log("Dados carregados com sucesso")
    log("----" * 150)
    log("Iniciando tratamento dos dados id_municipio")
    # ! Tratamento do id_municipio para mergear com a base
    id_municipio["nome"] = id_municipio["nome"].str.upper()
    id_municipio["nome"] = id_municipio["nome"].apply(unidecode.unidecode)
    id_municipio["nome"] = id_municipio["nome"].replace(
        "ESPIGAO D'OESTE", "ESPIGAO DO OESTE"
    )
    id_municipio["nome"] = id_municipio["nome"].replace(
        "SANT'ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO"
    )
    id_municipio = id_municipio[["id_municipio", "nome", "sigla_uf"]]

    return id_municipio


def open_csvs(url_diesel_gnv, url_gasolina_etanol, url_glp):
    log("----" * 150)
    data_frames = []
    log("Abrindo os arquivos csvs")
    diesel = pd.read_csv(f"{url_diesel_gnv}", sep=";", encoding="utf-8")
    log("----" * 150)
    log("----" * 150)
    log(diesel["Data da Coleta"].unique())
    gasolina = pd.read_csv(f"{url_gasolina_etanol}", sep=";", encoding="utf-8")
    log("----" * 150)
    log("Abrindo os arquivos csvs gasolina")
    log("----" * 150)
    glp = pd.read_csv(f"{url_glp}", sep=";", encoding="utf-8")
    log("Abrindo os arquivos csvs glp")
    log("----" * 150)
    # log(glp["Data da Coleta"].unique())
    data_frames.extend([diesel, gasolina, glp])
    precos_combustiveis = pd.concat(data_frames, ignore_index=True)
    log("----" * 150)
    log("Dados concatenados com sucesso")
    log("----" * 150)

    return precos_combustiveis


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
        formatted_value = date_value.strftime("%Y-%m-%d")

        partition_path = os.path.join(
            output_directory, f"{column_name}={formatted_value}"
        )
        if not os.path.exists(partition_path):
            os.makedirs(partition_path)

        df_partition = df[df[column_name] == value].copy()

        df_partition.drop([column_name], axis=1, inplace=True)
        csv_path = os.path.join(partition_path, "data.csv")
        df_partition.to_csv(csv_path, index=False, encoding="utf-8", na_rep="")
        log(f"Arquivo {csv_path} salvo com sucesso!")


def merge_table_id_municipio(
    id_municipio: pd.DataFrame, pd_precos_combustiveis: pd.DataFrame
):
    log("Iniciando tratamento dos dados precos_combustiveis")
    precos_combustiveis = pd.merge(
        id_municipio,
        pd_precos_combustiveis,
        how="right",
        left_on=["nome", "sigla_uf"],
        right_on=["Municipio", "Estado - Sigla"],
    )
    log("----" * 150)
    log("Dados mergeados")

    return precos_combustiveis


def rename_and_to_create_endereco(precos_combustiveis: pd.DataFrame):
    precos_combustiveis["endereco_revenda"] = (
        precos_combustiveis["Nome da Rua"].fillna("")
        + ","
        + " "
        + precos_combustiveis["Numero Rua"].fillna("")
        + ","
        + " "
        + precos_combustiveis["Complemento"].fillna("")
    )
    precos_combustiveis.drop(columns=["sigla_uf"], inplace=True)
    precos_combustiveis.rename(columns={"Data da Coleta": "data_coleta"}, inplace=True)

    return precos_combustiveis


def orderning_data_coleta(precos_combustiveis: pd.DataFrame):
    precos_combustiveis["data_coleta"] = (
        precos_combustiveis["data_coleta"].str[6:10]
        + "-"
        + precos_combustiveis["data_coleta"].str[3:5]
        + "-"
        + precos_combustiveis["data_coleta"].str[0:2]
    )

    return precos_combustiveis


def lower_colunm_produto(precos_combustiveis: pd.DataFrame):
    precos_combustiveis["Produto"] = precos_combustiveis["Produto"].str.lower()
    return precos_combustiveis


def creating_column_ano(precos_combustiveis: pd.DataFrame):
    precos_combustiveis["ano"] = precos_combustiveis["data_coleta"].str[0:4]
    precos_combustiveis["ano"].replace("nan", "", inplace=True)

    return precos_combustiveis


def rename_and_reordening(precos_combustiveis: pd.DataFrame):
    precos_combustiveis.rename(columns=anatel_constants.RENAME.value, inplace=True)
    precos_combustiveis = precos_combustiveis[anatel_constants.ORDEM.value]

    return precos_combustiveis


def rename_columns(precos_combustiveis: pd.DataFrame):
    precos_combustiveis["ano"] = precos_combustiveis["ano"].apply(
        lambda x: str(x).replace(".0", "")
    )
    precos_combustiveis["cep_revenda"] = precos_combustiveis["cep_revenda"].apply(
        lambda x: str(x).replace("-", "")
    )
    precos_combustiveis["unidade_medida"] = precos_combustiveis["unidade_medida"].map(
        {"R$ / litro": "R$/litro", "R$ / m³": "R$/m3", "R$ / 13 kg": "R$/13kg"}
    )
    precos_combustiveis["nome_estabelecimento"] = precos_combustiveis[
        "nome_estabelecimento"
    ].apply(lambda x: str(x).replace(",", ""))
    precos_combustiveis["preco_compra"] = precos_combustiveis["preco_compra"].apply(
        lambda x: str(x).replace(",", ".")
    )
    precos_combustiveis["preco_venda"] = precos_combustiveis["preco_venda"].apply(
        lambda x: str(x).replace(",", ".")
    )
    precos_combustiveis["preco_venda"] = precos_combustiveis["preco_venda"].replace(
        "nan", ""
    )
    precos_combustiveis["preco_compra"] = precos_combustiveis["preco_compra"].replace(
        "nan", ""
    )
    precos_combustiveis.replace(np.nan, "", inplace=True)

    return precos_combustiveis
