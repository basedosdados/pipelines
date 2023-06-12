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
from pipelines.datasets.br_bcb_indicadores.constants import URL_BCB
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
    return json_response["value"]


def create_url(start_date: str, end_date: str, moeda="USD") -> str:
    """
    Creates parameterized url
    """
    search_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoMoedaPeriodo(moeda='{}',dataInicial='{}',dataFinalCotacao='{}')".format(
        moeda, start_date, end_date
    )

    return search_url


def connect_to_endpoint(url: str) -> dict:
    """
    Connect to endpoint
    """
    response = requests.request("GET", url, timeout=30)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_currency_data(currency):
    start_day = datetime.now(tz=pytz.UTC) - timedelta(days=14)
    start_day = start_day.strftime("%m-%d-%Y")

    # Calculate the end date as 10 days before the current date
    now = datetime.now(tz=pytz.UTC) - timedelta(days=7)
    end_day = now.strftime("%m-%d-%Y")

    # Currency code for the current currency
    currency_code = currency["simbolo"]

    # Create the URL for retrieving data using the start and end dates and currency code
    url = create_url(start_day, end_day, currency_code)

    # Connect to the API endpoint and retrieve the JSON response
    attempts = 0
    while attempts < 3:
        try:
            json_response = connect_to_endpoint(url)
        except requests.exceptions.Timeout:
            attempts += 1
            log(
                f"timeout, tentativa {attempts} vamos esperar 15 segundos para rodar novamente"
            )
            tm.sleep(15)
            json_response = connect_to_endpoint(url)

    data = json_response["value"]

    df = pd.DataFrame(data)

    df["moeda"] = currency["simbolo"]  # Add a 'moeda' column with the currency code
    df["tipo_moeda"] = currency["tipoMoeda"]

    return df


def treat_df(df, table_id):
    # Convert the 'dataHoraCotacao' column in 'df' to datetime format
    df["dataHoraCotacao"] = pd.to_datetime(
        df["dataHoraCotacao"], format="%Y-%m-%d %H:%M:%S.%f"
    )

    # Extract the date and time components from 'dataHoraCotacao' column and create new columns 'data_cotacao' and 'hora_cotacao'
    df["data_cotacao"] = df["dataHoraCotacao"].dt.date
    df["hora_cotacao"] = df["dataHoraCotacao"].dt.time

    # Read the architecture table from the given URL and store it in the 'architecture' variable
    architecture = read_architecture_table(url_architecture=URL_BCB.value[table_id])

    # Rename the columns of the DataFrame 'df' based on the architecture table
    df = rename_columns(df, architecture)

    # Get the column order from the architecture table
    order = get_order(architecture=architecture)

    # Reorder the columns of 'df' based on the specified order
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

    return df_architecture.replace(np.nan, "", regex=True)


def rename_columns(df, architecture):
    aux = architecture[["name", "original_name"]].drop_duplicates(
        subset=["original_name"], keep=False
    )
    dict_columns = dict(zip(aux.original_name, aux.name))
    return df.rename(columns=dict_columns)


def get_order(architecture):
    return list(architecture["name"])


def save_input(df, table_id):
    folder = f"tmp/{table_id}/input/"  # Folder path for storing the file
    os.system(f"mkdir -p {folder}")  # Create the folder if it doesn't exist
    full_filepath = f"{folder}/{table_id}.csv"  # Full file path for the CSV file
    df.to_csv(full_filepath, index=False)  # Save the DataFrame as a CSV file

    return full_filepath


def save_output(df, table_id):
    folder = f"tmp/{table_id}/output/"  # Folder path for storing the file
    os.system(f"mkdir -p {folder}")  # Create the folder if it doesn't exist
    full_filepath = f"{folder}/{table_id}.csv"  # Full file path for the CSV file
    df.to_csv(full_filepath, index=False)  # Save the DataFrame as a CSV file

    return full_filepath


def read_input_csv(table_id):
    path = f"tmp/{table_id}/input/{table_id}.csv"
    return pd.read_csv(path)
