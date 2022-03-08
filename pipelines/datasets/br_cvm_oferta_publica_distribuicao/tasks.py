"""
Tasks for br_cvm_oferta_publica_distribuicao
"""
import os
from pathlib import Path
from typing import Union

import pandas as pd
import basedosdados as bd
from prefect import task

from pipelines.utils import log

@task
def crawl(root: str, url: str) -> None:
    # pylint: disable=invalid-name
    """Get table 'oferta_distribuicao' from CVM website"""
    filepath = f"{root}/oferta_distribuicao.csv"
    os.makedirs(root, exist_ok=True)

    df: pd.DataFrame = pd.read_csv(url, encoding="latin-1", sep=";")
    df.to_csv(filepath, index=False, sep=";")

@task
def clean_table_oferta_distribuicao(root: str) -> str:
    # pylint: disable=invalid-name,no-member,unsubscriptable-object
    """Standardizes column names and selected variables"""
    in_filepath = f"{root}/oferta_distribuicao.csv"
    ou_filepath = f"{root}/br_cvm_oferta_publica_distribuicao.csv"

    df = pd.DataFrame = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    df.columns = [k.lower() for k in df.columns]

    df.loc[(df["oferta_inicial"] == "N"), "oferta_inicial"] = "Não"
    df.loc[(df["oferta_inicial"] == "S"), "oferta_inicial"] = "Sim"

    df.loc[(df["oferta_incentivo_fiscal"] == "N"), "oferta_incentivo_fiscal"] = "Não"
    df.loc[(df["oferta_incentivo_fiscal"] == "S"), "oferta_incentivo_fiscal"] = "Sim"

    df.loc[(df["oferta_regime_fiduciario"] == "N"), "oferta_regime_fiduciario"] = "Não"
    df.loc[(df["oferta_regime_fiduciario"] == "S"), "oferta_regime_fiduciario"] = "Sim"

    df.to_csv(ou_filepath, index=False, encoding='utf-8')

    return ou_filepath
