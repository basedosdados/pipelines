# -*- coding: utf-8 -*-
"""
General purpose functions for the br_inmet_bdmep project
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelo projeto br_inmet_bdmep.
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
# from pipelines.datasets.br_inmet_bdmep.utils import foo
# foo()
# ```
#
###############################################################################
# pylint: disable=too-few-public-methods,invalid-name

import pandas as pd

# import string
import tempfile
import urllib.request
import zipfile

# import rasterio
# import geopandas as gpd
import os
import numpy as np

# import glob
# import datetime
import re
from datetime import datetime, time
from unidecode import unidecode


def new_names(base: pd.DataFrame, oldname: str, newname: str):
    """
    Esta função renomeia a coluna oldname do DataFrame base para newname.

    Args:

    `base` : DataFrame do Pandas
    O DataFrame no qual a coluna deve ser renomeada.

    `oldname` : string
    O nome atual da coluna a ser renomeada.

    `newname` : string
    O novo nome que será atribuído à coluna.
    Retorno:

    `names(base)` : lista de strings
    Retorna uma lista contendo os nomes das colunas do DataFrame base após a renomeação. Se mais de uma coluna com o nome oldname for encontrada, a função retorna a lista de todos os nomes das colunas em base.

    """
    # x = [i for i, name in enumerate(base.columns) if name == oldname]
    x = re.search(oldname, base.columns)

    if len(x) > 1:
        return base.columns.tolist()

    else:
        base.rename(columns={oldname: newname}, inplace=True)
        return base.columns.tolist()


def lowercase_columns(df):
    df = df.rename(columns=lambda x: unidecode(x.lower()))
    return df


def change_names(base: pd.DataFrame):
    """
    Altera os nomes das colunas de um DataFrame baseado em um conjunto
    de regras pré-definidas.

    Args:
        base (pandas.DataFrame): DataFrame com as colunas que serão
            renomeadas.

    Returns:
        List[str]: Lista com os novos nomes das colunas.

    Regras:
        - Torna os nomes das colunas em letras minúsculas.
        - Remove caracteres acentuados e substitui por caracteres ASCII
          equivalentes.
        - Renomeia as colunas baseado em padrões específicos
    """
    base = lowercase_columns(base)
    # base.columns = base.columns.map(lambda x: x.translate(x, string.ascii_letters, 'ASCII'))
    base = rename_cols_with_regex(base, "data", "data")
    base = rename_cols_with_regex(base, "^hora", "hora")
    base = rename_cols_with_regex(base, "precipitacao.*total", "precipitacao_total")
    base = rename_cols_with_regex(base, "pressao.*nivel", "pressao_atm_hora")
    base = rename_cols_with_regex(base, "pressao.*max", "pressao_atm_max")
    base = rename_cols_with_regex(base, "pressao.*min", "pressao_atm_min")
    base = rename_cols_with_regex(base, "radiacao", "radiacao_global")
    base = rename_cols_with_regex(
        base, "temperatura.*bulbo.*horaria", "temperatura_bulbo_hora"
    )
    base = rename_cols_with_regex(base, "temperatura maxima", "temperatura_max")
    base = rename_cols_with_regex(base, "temperatura minima", "temperatura_min")
    base = rename_cols_with_regex(
        base, "temperatura do ponto de orvalho", "temperatura_orvalho_hora"
    )
    base = rename_cols_with_regex(
        base, "temperatura orvalho min", "temperatura_orvalho_min"
    )
    base = rename_cols_with_regex(
        base, "temperatura orvalho max", "temperatura_orvalho_max"
    )
    base = rename_cols_with_regex(base, "umidade relativa.*horaria", "umidade_rel_hora")
    base = rename_cols_with_regex(base, "umidade rel. max", "umidade_rel_max")
    base = rename_cols_with_regex(base, "umidade rel. min", "umidade_rel_min")
    base = rename_cols_with_regex(base, "vento.*direcao", "vento_direcao")
    base = rename_cols_with_regex(base, "vento.*rajada.* maxima", "vento_rajada_max")
    base = rename_cols_with_regex(base, "vento.*velocidade", "vento_velocidade")

    return base


def rename_cols_with_regex(df, regex, new_name):
    """
    Renomeia as colunas de um dataframe que correspondem a um regex.

    Parameters:
    df (pandas.DataFrame): O dataframe para renomear as colunas.
    regex (str): O regex para procurar nas colunas do dataframe.
    new_name (str): O novo nome para atribuir às colunas que correspondem ao regex.

    Returns:
    pandas.DataFrame: O dataframe com as colunas renomeadas.
    """
    pattern = re.compile(regex)
    col_names = df.columns.tolist()
    renamed_cols = [new_name if pattern.search(col) else col for col in col_names]
    df.columns = renamed_cols
    return df


def convert_to_time(hora: str):

    # hora_str = "0100 UTC"
    hora_parts = hora.split()[0]  # extrai "0100" da string original
    hora_obj = time(
        hour=int(hora_parts[:2]), minute=0, second=0
    )  # cria um objeto time com a hora

    return hora_obj.strftime("%H:%M:%S")


def get_clima_info(file: str) -> pd.DataFrame:
    """
    Extrai informações climáticas de um arquivo em formato .txt e retorna um dataframe com as informações.

    Args:
        file (str): O caminho e nome do arquivo a ser lido.

    Returns:
        pd.DataFrame: Um dataframe com as informações climáticas.
    """

    # lê o arquivo de clima
    clima = pd.read_csv(file, sep=";", skiprows=8, decimal=",", encoding="ISO-8859-1")

    # lê as informações de cabeçalho
    caract = pd.read_csv(
        file,
        sep=";",
        nrows=8,
        header=None,
        names=["caract", "value"],
        encoding="ISO-8859-1",
    )

    # remove a coluna V20 do dataframe clima
    clima.drop(columns=["Unnamed: 19"], inplace=True)

    # renomeia as colunas do dataframe clima
    clima = change_names(clima)

    # adiciona as informações da estação no dataframe clima
    clima["id_estacao"] = caract.loc[3, "value"]

    # substitui valores -9999 por NaN
    clima.replace(to_replace=-9999, value=np.nan, inplace=True)

    # converte a coluna data para datetime
    clima["data"] = clima["data"].apply(lambda x: datetime.strptime(str(x), "%Y/%m/%d"))

    # converte as colunas de 3 a 19 para float
    clima.iloc[:, 3:19] = clima.iloc[:, 3:19].astype(float)

    # converte a coluna hora para o formato "HH:00:00"
    clima["hora"] = clima["hora"].apply(lambda x: convert_to_time(x))

    return clima


def download_inmet(year: int) -> None:
    """
    Realiza o download dos dados históricos de uma determinado ano do INMET (Instituto Nacional de Meteorologia)
    e descompacta o arquivo em um diretório local.

    Args:
        year (int): O ano para o qual deseja-se baixar os dados históricos.

    Returns:
        None
    """

    ## to-do -> adicionar condição para testar se o dir já existe (pathlib)
    os.system("mkdir -p /tmp/data/input/")
    temp = tempfile.NamedTemporaryFile(delete=False)
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
    urllib.request.urlretrieve(url, temp.name)
    with zipfile.ZipFile(temp.name, "r") as zip_ref:
        zip_ref.extractall(f"/tmp/data/input/{year}")
    temp.close()
    # remove o arquivo temporário
    os.remove(temp.name)
