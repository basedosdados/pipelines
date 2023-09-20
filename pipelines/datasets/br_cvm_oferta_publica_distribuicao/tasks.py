# -*- coding: utf-8 -*-
"""
Tasks for br_cvm_oferta_publica_distribuicao
"""
import os


import pandas as pd
from pandas.api.types import is_string_dtype
from prefect import task
from unidecode import unidecode
import basedosdados as bd
from datetime import datetime
from pipelines.utils.utils import log


@task
def crawl(root: str, url: str) -> None:
    # pylint: disable=invalid-name
    """Get table 'oferta_distribuicao' from CVM website"""
    filepath = f"{root}/oferta_distribuicao.csv"
    os.makedirs(root, exist_ok=True)

    dataframe: pd.DataFrame = pd.read_csv(url, encoding="latin-1", sep=";")
    dataframe.to_csv(filepath, index=False, sep=";")


@task
def clean_table_oferta_distribuicao(root: str) -> str:
    # pylint: disable=invalid-name,no-member,unsubscriptable-object, E1137
    """Standardizes column names and selected variables"""
    in_filepath = f"{root}/oferta_distribuicao.csv"
    ou_filepath = f"{root}/output/br_cvm_oferta_publica_distribuicao.csv"
    os.makedirs(f"{root}/output/", exist_ok=True)

    dataframe = pd.DataFrame = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="utf-8",
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


@task
def extract_last_date(
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
    var_name: str,
) -> str:
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        billing_project_id (str): The billing project ID.

    Returns:
        str: The last update date in the format 'yyyy-mm-dd'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """
    log("dasdasdsadas")

    query_bd = f"""
    SELECT MAX({var_name}) as max_date
    FROM
    `{billing_project_id}.{dataset_id}.{table_id}`
    """
    log(f"{query_bd}")
    log("dasdasdsadas")

    t = bd.read_sql(
        query=query_bd,
        billing_project_id=billing_project_id,
        from_file=True,
    )
    log(f"{t}")
    data = t["max_date"][0]

    log(f"A data mais recente da tabela é: {data}")

    return str(data)
