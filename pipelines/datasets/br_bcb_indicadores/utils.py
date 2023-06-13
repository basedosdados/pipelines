# -*- coding: utf-8 -*-
"""
General purpose functions for the br_bcb_indicadores project
"""
import requests
import pandas as pd
from datetime import datetime
import pytz
from datetime import timedelta
import os
from io import StringIO
import numpy as np
import time as tm
from pipelines.datasets.br_bcb_indicadores.constants import constants as bcb_constants
from pipelines.utils.utils import log

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_bcb_indicadores.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.datasets.br_bcb_indicadores.utils import foo
# foo()
# ```
#
###############################################################################


def available_currencies():
    """
    Return available currencies from api
    """
    url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/Moedas"
    json_response = connect_to_endpoint(url)
    log(f"available currencies:{json_response['value']}")
    return json_response["value"]


def create_url(start_date: str, end_date: str, moeda="USD") -> str:
    """
    Creates parameterized url
    """
    search_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda='{}',dataInicial='{}',dataFinalCotacao='{}')".format(
        moeda, start_date, end_date
    )
    log(search_url)
    return search_url


def connect_to_endpoint(url: str) -> dict:
    """
    Connect to endpoint
    """
    response = requests.request("GET", url, timeout=30)
    log("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        log(Exception(response.status_code, response.text))
    return response.json()


def get_currency_data(currency):
    start_day = datetime.now(tz=pytz.UTC) - timedelta(days=14)
    start_day = start_day.strftime("%m-%d-%Y")
    log(f"start day: {start_day}")

    # Calculate the end date as 10 days before the current date
    now = datetime.now(tz=pytz.UTC) - timedelta(days=7)
    end_day = now.strftime("%m-%d-%Y")
    log(f"end day: {end_day}")

    # Currency code for the current currency
    currency_code = currency["simbolo"]

    # Create the URL for retrieving data using the start and end dates and currency code
    log("creating_url")
    url = create_url(start_day, end_day, currency_code)

    # Connect to the API endpoint and retrieve the JSON response
    attempts = 0
    while attempts < 3:
        attempts += 1
        log(f"tentativa {attempts}")
        try:
            log("connecting to endpoint")
            json_response = connect_to_endpoint(url)
        except requests.exceptions.Timeout:
            tm.sleep(3)

    data = json_response["value"]

    log("transform json into df")
    df = pd.DataFrame(data)

    log("Add a columns about the currency code")
    df["moeda"] = currency["simbolo"]
    df["tipo_moeda"] = currency["tipoMoeda"]

    return df


def treat_df(df, table_id):
    log("Convert the 'dataHoraCotacao' column in 'df' to datetime format")
    df["dataHoraCotacao"] = pd.to_datetime(
        df["dataHoraCotacao"], format="%Y-%m-%d %H:%M:%S.%f"
    )

    log(
        "Extract the date and time components from 'dataHoraCotacao' column and create new columns 'data_cotacao' and 'hora_cotacao'"
    )
    df["data_cotacao"] = df["dataHoraCotacao"].dt.date
    df["hora_cotacao"] = df["dataHoraCotacao"].dt.time

    log(
        "Read the architecture table from the given URL and store it in the 'architecture' variable"
    )
    architecture = read_architecture_table(
        url_architecture=bcb_constants.URL_BCB.value[table_id]
    )

    log("Rename the columns of the DataFrame 'df' based on the architecture table")
    df = rename_columns(df, architecture)

    log("Get the column order from the architecture table")
    order = get_order(architecture=architecture)

    log("Reorder the columns of 'df' based on the specified order")
    return df[order]


def read_architecture_table(url_architecture: str):
    """URL contendo a tabela de arquitetura no formato da base dos dados

    Args:
        url_architecture (str): url de tabela de arquitera no padrão da base dos dados

    Returns:
        df: um df com a tabela de arquitetura
    """
    # Converte a URL de edição para um link de exportação em formato csv
    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")

    # Coloca a arquitetura em um dataframe
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    log("Download of the architecture table")
    return df_architecture.replace(np.nan, "", regex=True)


def rename_columns(df, architecture):
    aux = architecture[["name", "original_name"]].drop_duplicates(
        subset=["original_name"], keep=False
    )
    dict_columns = dict(zip(aux.original_name, aux.name))
    log("Rename columns")
    return df.rename(columns=dict_columns)


def get_order(architecture):
    return list(architecture["name"])


def save_input(df, table_id):
    folder = f"tmp/{table_id}/input/"  # Folder path for storing the file
    os.system(f"mkdir -p {folder}")  # Create the folder if it doesn't exist
    full_filepath = f"{folder}/{table_id}.csv"  # Full file path for the CSV file
    df.to_csv(full_filepath, index=False)  # Save the DataFrame as a CSV file
    log("save_input")
    return full_filepath


def save_output(df, table_id):
    folder = f"tmp/{table_id}/output/"  # Folder path for storing the file
    os.system(f"mkdir -p {folder}")  # Create the folder if it doesn't exist
    full_filepath = f"{folder}/{table_id}.csv"  # Full file path for the CSV file
    df.to_csv(full_filepath, index=False)  # Save the DataFrame as a CSV file
    log("save_output")
    return full_filepath


def read_input_csv(table_id):
    path = f"tmp/{table_id}/input/{table_id}.csv"
    log("read input")
    return pd.read_csv(path)
