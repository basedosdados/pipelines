# -*- coding: utf-8 -*-
"""
Tasks for br_cvm_oferta_publica_distribuicao
"""
import os
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd
from pandas.api.types import is_string_dtype
from prefect import task
from unidecode import unidecode



@task
def crawl(root: str, url: str) -> None:
    # pylint: disable=invalid-name
    """Get table 'oferta_distribuicao' from CVM website"""
    os.makedirs(root, exist_ok=True)

    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=root)


@task
def clean_table_oferta_distribuicao(root: str) -> str:
    # pylint: disable=invalid-name,no-member,unsubscriptable-object, E1137
    """Standardizes column names and selected variables"""
    in_filepath = f"{root}/oferta_distribuicao.csv"
    ou_filepath = f"{root}/output/br_cvm_oferta_publica_distribuicao.csv"
    os.makedirs(f"{root}/output/", exist_ok=True)

    dataframe = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="latin-1",
        dtype=object,
    )

    dataframe.columns = [k.lower() for k in dataframe.columns]

    dataframe.loc[(dataframe["oferta_inicial"] == "N"), "oferta_inicial"] = "Nao"
    dataframe.loc[(dataframe["oferta_inicial"] == "S"), "oferta_inicial"] = "Sim"

    dataframe.loc[
        (dataframe["oferta_incentivo_fiscal"] == "N"), "oferta_incentivo_fiscal"
    ] = "Nao"
    dataframe.loc[
        (dataframe["oferta_incentivo_fiscal"] == "S"), "oferta_incentivo_fiscal"
    ] = "Sim"

    dataframe.loc[
        (dataframe["oferta_regime_fiduciario"] == "N"), "oferta_regime_fiduciario"
    ] = "Nao"
    dataframe.loc[
        (dataframe["oferta_regime_fiduciario"] == "S"), "oferta_regime_fiduciario"
    ] = "Sim"

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False, encoding="utf-8")

    return ou_filepath
