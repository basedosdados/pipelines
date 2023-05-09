# -*- coding: utf-8 -*-
"""
General purpose functions for the br_isp_estatisticas_seguranca project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_isp_estatisticas_seguranca.
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
# from pipelines.datasets.br_isp_estatisticas_seguranca.utils import foo
# foo()
# ```
#
###############################################################################

import pandas as pd
from io import StringIO
import os
import requests
from typing import List
from typing import Dict

# build a dict that maps a table name to a architectura and
# another dict that maps an original table name to a
# trated table name


def dict_original():

    dict_original = {
        "BaseDPEvolucaoMensalCisp.csv": "evolucao_mensal_cisp.csv",
        "DOMensalEstadoDesde1991.csv": "evolucao_mensal_uf.csv",
        "BaseEstadoTaxaMes.csv": "taxa_evolucao_mensal_uf.csv",
        "BaseMunicipioMensal.csv": "evolucao_mensal_municipio.csv",
        "BaseMunicipioTaxaMes.csv": "taxa_evolucao_mensal_municipio.csv",
        "ArmasApreendidasEvolucaoCisp.xlsx": "armas_apreendidas_mensal.csv",
        "PoliciaisMortos.csv": "evolucao_policial_morto_servico_mensal.csv",
        "BaseFeminicidioEvolucaoMensalCisp.csv": "feminicidio_mensal_cisp.csv",
    }
    return dict_original


def dict_arquitetura():

    dict_arquitetura = {
        "evolucao_mensal_cisp.csv": "https://docs.google.com/spreadsheets/d/1jibGPOYF6Tack3n9MmiQKdBagbeK-oLr/edit#gid=55379267",
        "evolucao_mensal_uf.csv": "https://docs.google.com/spreadsheets/d/1seN6LQ9WQnobVNpFw6KX5BMhYr0yyZa2/edit#gid=1349095453",
        "taxa_evolucao_mensal_uf.csv": "https://docs.google.com/spreadsheets/d/1fQ7MnfHm8vrlfdUhAYlU7F-CJouORKkc/edit#gid=1414191356",
        "taxa_evolucao_anual_uf.csv": "https://docs.google.com/spreadsheets/d/117EyqVw5a6_0PFPISjLZuZLIsRqyZiOo/edit#gid=588874333",
        "evolucao_mensal_municipio.csv": "https://docs.google.com/spreadsheets/d/1cHPcIBfmFwSxgMTsvGX-Ha-Efz7ZIDpn/edit#gid=347509921",
        "taxa_evolucao_mensal_municipio.csv": "https://docs.google.com/spreadsheets/d/1VKorutzmHUl71a2J--auChJm8tC-652i/edit#gid=199121203",
        "taxa_evolucao_anual_municipio.csv": "https://docs.google.com/spreadsheets/d/1LeH92JhPkr59NoUwepOgHKlsJIS6k-2W/edit#gid=786684819",
        "evolucao_mensal_upp.csv": "https://docs.google.com/spreadsheets/d/1TGG8T5xzmO_tzo9RScnIOt2NKjyIHu2O/edit#gid=1336604684",
        "armas_apreendidas_mensal.csv": "https://docs.google.com/spreadsheets/d/14wV3BkjG_9GDWKDUAbOVe2KOh0Bi6FAA/edit#gid=1673208544",
        "armas_fogo_apreendidas_mensal.csv": "https://docs.google.com/spreadsheets/d/19gynYMOxzfgjd7HsPjH4LbOkgV9844WSoVBiqAgp340/edit#gid=0",
        "evolucao_policial_morto_servico_mensal.csv": "https://docs.google.com/spreadsheets/d/1wuRr-I73jje0nkSeF_9LEJSpRmuwIftX/edit#gid=1573015202",
        "feminicidio_mensal_cisp.csv": "https://docs.google.com/spreadsheets/d/1DLb9GQAZR-TRbJp0YYc1w71OsEwXPYa8/edit#gid=1573015202",
        "taxa_letalidade.csv": "https://docs.google.com/spreadsheets/d/1wMbutt7Gs17ZlGEZ_-SBT4bwtF_QSWd5KLR-m8KaBMo/edit#gid=0",
    }

    return dict_arquitetura


# ---- build rename dicts
def change_columns_name(url_architecture: str) -> Dict[str, str]:

    """Essa função recebe como input uma string com link para uma tabela de arquitetura
    e retorna um dicionário com os nomes das colunas originais e os nomes das colunas
    padronizados

    Returns:
        dict: com chaves sendo os nomes originais e valores sendo os nomes padronizados
    """
    # Converte a URL de edição para um link de exportação em formato csv
    url = url_architecture.replace("edit#gid=", "export?format=csv&gid=")

    # Coloca a arquitetura em um dataframe
    df_architecture = pd.read_csv(
        StringIO(requests.get(url, timeout=10).content.decode("utf-8"))
    )

    # Cria um dicionário de nomes de colunas e tipos de dados a partir do dataframe df_architecture
    column_name_dict = dict(
        zip(df_architecture["original_name"], df_architecture["name"])
    )

    # Retorna o dicionário

    return column_name_dict


# ---- function to order columns


def create_columns_order(dict: Dict[str, str]) -> List[str]:
    """This function receives a dictionary with the
    original column names and the standardized column names
    and returns a list with the standardized order column names.
    from the architecture table


    Args:
        dict (Dict[str, str]): A dictionary with the original column names and the standardized column names

    Returns:
        List[str]: A list with standardized column names
    """
    ordered_list = dict.values()

    return ordered_list


# ---- function to
def check_tipo_fase(df: pd.DataFrame) -> pd.DataFrame:
    """Checks if column tipo_fase exists,
    if it does, it maps descriptions to the values

    Args:
        df (pd.DataFrame): A dataframe to check if column tipo_fase exists

    Returns: df (pd.DataFrame): A dataframe with mapped values for column tipo_fase
    """

    for col in df.columns:

        if col == "tipo_fase":

            df["tipo_fase"] = df["tipo_fase"].map(
                {
                    "2": "Consolidado sem errata",
                    "3": "Consolidado com errata",
                }
            )
        else:
            pass

    return df
