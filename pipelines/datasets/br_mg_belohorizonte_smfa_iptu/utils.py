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
from pipelines.datasets.br_mg_belohorizonte_smfa_iptu.constants import constants


def scrapping_download_csv(input: str):
    # ----- Muitos sites bloqueiam solicitações que não têm um User-Agent adequado definido no cabeçalho da solicitação.
    # ----- Você pode definir um User-Agent apropriado para sua solicitação para parecer mais como um navegador comum.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }
    for url in constants.URLS.value:
        response = requests.get(url, headers=headers)

        soup = BeautifulSoup(response.content, "html.parser")

        links = soup.find_all("a", href=lambda href: href and href.endswith(".csv"))

        if links:
            link = links[-1]
            filename = link.get("href").split("/")[-1]
            file_response = link.get("href")
            response = requests.get(file_response, headers=headers)
            print(file_response)
            with open(f"{input}/{filename}", "wb") as f:
                f.write(response.content)

            print(f"Arquivo {filename} baixado com sucesso!")


def concat_csv(input: str) -> pd.DataFrame:
    dataframe = []
    arquivos = os.listdir(input)
    for arquivo in arquivos:
        arquivo.endswith(".csv")
        df = pd.read_csv(f"{input}/{arquivo}", encoding="utf-8", sep=";", dtype=str)
        dataframe.append(df)

    df = pd.concat(dataframe)

    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns=constants.RENAME.value, inplace=True)

    return df


def replace_variables(df: pd.DataFrame) -> pd.DataFrame:
    bool = [
        "indicador_rede_telefonica",
        "indicador_meio_fio",
        "indicador_pavimentacao",
        "indicador_arborizacao",
        "indicador_galeria_pluvial",
        "indicador_iluminacao_publica",
        "indicador_rede_esgoto",
        "indicador_agua",
    ]
    for valores_bool in bool:
        df[valores_bool] = (
            df[valores_bool].replace("SIM", "true").replace("NÃO", "false")
        )

    inicap = [
        "tipo_logradouro",
        "logradouro",
        "tipo_construtivo",
        "tipo_ocupacao",
        "tipologia",
        "frequencia_coleta",
    ]
    for variaveis_inicap in inicap:
        df[variaveis_inicap] = df[variaveis_inicap].str.capitalize()

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


def new_columns_endereco(df: pd.DataFrame) -> pd.DataFrame:
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
    df["ano"] = datetime.now().year
    df["mes"] = datetime.now().month

    return df


def reordering_and_np_nan(df: pd.DataFrame) -> pd.DataFrame:
    df = df[constants.ORDEM.value]
    df = df.replace(np.nan, "")

    return df
