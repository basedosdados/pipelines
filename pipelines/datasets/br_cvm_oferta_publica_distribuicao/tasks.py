"""
Tasks for br_cvm_oferta_publica_distribuicao — Prefect 3.
"""

import os
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd
from pandas.api.types import is_string_dtype
from prefect import task
from unidecode import unidecode

ROOT = "/tmp/data_cvm_oferta"
URL = "http://dados.cvm.gov.br/dados/OFERTA/DISTRIB/DADOS/oferta_distribuicao.zip"


@task(retries=2, retry_delay_seconds=30)
def crawl(root: str = ROOT, url: str = URL) -> None:
    """Download and unzip oferta_distribuicao."""
    os.makedirs(root, exist_ok=True)
    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=root)


@task
def clean_table_oferta_distribuicao(root: str = ROOT) -> str:
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

    for col in (
        "oferta_inicial",
        "oferta_incentivo_fiscal",
        "oferta_regime_fiduciario",
    ):
        dataframe.loc[dataframe[col] == "N", col] = "Nao"
        dataframe.loc[dataframe[col] == "S", col] = "Sim"

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False, encoding="utf-8")
    return ou_filepath
