# -*- coding: utf-8 -*-
"""
General purpose functions for the br_mg_belohorizonte_smfa_iptu project
"""

import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime
import geopandas as gpd
from shapely import wkt
from pipelines.utils.tasks import log
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.constants import constants


def scrapping_download_csv(input_path: str):
    # ----- Muitos sites bloqueiam solicitações que não têm um User-Agent adequado definido no cabeçalho da solicitação.
    # ----- Você pode definir um User-Agent apropriado para sua solicitação para parecer mais como um navegador comum.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }
    if not os.path.exists(constants.INPUT_PATH.value):
        os.mkdir(constants.INPUT_PATH.value)

    for url in constants.URLS.value:
        response = requests.get(url, headers=headers)

        soup = BeautifulSoup(response.content, "html.parser")

        links = soup.find_all("a", href=lambda href: href and href.endswith(".csv"))

        if links:
            link = links[-1]
            filename = link.get("href").split("/")[-1]
            file_response = link.get("href")
            response = requests.get(file_response, headers=headers)
            log(file_response)
            with open(f"{input_path}{filename}", "wb") as f:
                f.write(response.content)

            log(f"Arquivo {filename} baixado com sucesso!")


def concat_csv(input_path: str) -> pd.DataFrame:
    lista_dataframe = []
    arquivos = os.listdir(input_path)
    for arquivo in arquivos:
        if arquivo.endswith(".csv"):
            df = pd.read_csv(f"{input_path}{arquivo}", sep=";", encoding="utf-8")
            print(df.shape)
            lista_dataframe.append(df)
    df = pd.concat(lista_dataframe)
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns=constants.RENAME.value, inplace=True)
    return df


def fix_variables(df: pd.DataFrame) -> pd.DataFrame:
    bool_columns = [
        "indicador_rede_telefonica",
        "indicador_meio_fio",
        "indicador_pavimentacao",
        "indicador_arborizacao",
        "indicador_galeria_pluvial",
        "indicador_iluminacao_publica",
        "indicador_rede_esgoto",
        "indicador_agua",
    ]
    for valores_bool in bool_columns:
        df[valores_bool] = (
            df[valores_bool].replace("SIM", "true").replace("NÃO", "false")
        )

    inicap_columns = [
        "tipo_logradouro",
        "logradouro",
        "tipo_construtivo",
        "tipo_ocupacao",
        "tipologia",
        "frequencia_coleta",
    ]
    for column_initcap in inicap_columns:
        df[column_initcap] = df[column_initcap].str.capitalize()

    df["tipo_logradouro"] = (
        df["tipo_logradouro"]
        .replace("Ave", "Avenida")
        .replace("Est", "Estrada")
        .replace("Bec", "Beco")
        .replace("Rod", "Rodovia")
        .replace("Trv", "Trevo")
        .replace("Ala", "Alameda")
        .replace("Pca", "Praça")
    )
    return df


def new_column_endereco(df: pd.DataFrame) -> pd.DataFrame:
    df["endereco"] = (
        df["tipo_logradouro"].fillna("")
        + " "
        + df["logradouro"].fillna("")
        + ","
        + " "
        + df["numero_imovel"].fillna("")
    )
    return df


def new_columns_ano_mes(df: pd.DataFrame) -> pd.DataFrame:
    arquivos = os.listdir(constants.INPUT_PATH.value)
    valor = arquivos[0][0:6]
    df["ano"] = valor[0:4]
    df["mes"] = valor[4:6]
    return df


def reorder_and_fix_nan(df: pd.DataFrame) -> pd.DataFrame:
    df = df[constants.ORDEM.value]
    df = df.replace(np.nan, "")
    return df


def changing_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    df["poligono"] = df["poligono"].apply(wkt.loads)
    log("Definindo o GeoDataFrame")
    origem = "EPSG:31983"  # chama em metros
    destino = "EPSG:4326"  # chama a geográfica
    df = gpd.GeoDataFrame(df, geometry="poligono", crs=origem)
    log("Transformando coordenadas...")
    df["poligono"] = df["poligono"].to_crs(destino)
    return df
