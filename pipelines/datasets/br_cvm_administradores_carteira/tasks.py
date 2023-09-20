# -*- coding: utf-8 -*-
"""
Tasks for br_cvm_administradores_carteira
"""
# pylint: disable=line-too-long, W0702, E1101, E1136, E1137

import os
import shutil

import pandas as pd
import requests
from pandas.api.types import is_string_dtype
from prefect import task
from unidecode import unidecode
import basedosdados as bd
from datetime import datetime
from pipelines.utils.utils import log


@task
def crawl(root: str, url: str, chunk_size=128) -> None:
    """Download and unzip dataset br_cvm_administradores_carteira"""
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)
    os.makedirs(f"{root}/cleaned", exist_ok=True)

    response = requests.get(url, timeout=300)

    with open(filepath, "wb") as file:
        for chunk in response.iter_content(chunk_size=chunk_size):
            file.write(chunk)

    shutil.unpack_archive(filepath, extract_dir=root)


@task
def clean_table_responsavel(root: str) -> str:
    # pylint: disable=invalid-name
    """Clean table responsavel"""
    in_filepath = f"{root}/cad_adm_cart_resp.csv"
    ou_filepath = f"{root}/cleaned/bd_responsavel.csv"

    dataframe: pd.DataFrame = pd.read_csv(
        in_filepath,
        sep=";",
        na_values="",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    dataframe.columns = ["cnpj", "nome", "tipo"]

    dataframe["cnpj"] = dataframe["cnpj"].str.replace(".", "")
    dataframe["cnpj"] = dataframe["cnpj"].str.replace("/", "")
    dataframe["cnpj"] = dataframe["cnpj"].str.replace("-", "")

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False, encoding="utf-8")

    return ou_filepath


@task
def clean_table_pessoa_fisica(root: str) -> str:
    # pylint: disable=invalid-name
    """Clean table pessoa_fisica"""
    in_filepath = f"{root}/cad_adm_cart_pf.csv"
    ou_filepath = f"{root}/cleaned/bd_pessoa_fisica.csv"

    dataframe: pd.DataFrame = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    dataframe.columns = [
        "nome",
        "data_registro",
        "data_cancelamento",
        "motivo_cancelamento",
        "situacao",
        "data_inicio_situacao",
        "categoria_registro",
    ]

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False)

    return ou_filepath


@task
def clean_table_pessoa_juridica(root: str) -> str:
    # pylint: disable=invalid-name
    """Clean table pessoa_fisica"""
    in_filepath = f"{root}/cad_adm_cart_pj.csv"
    ou_filepath = f"{root}/cleaned/bd_pessoa_juridica.csv"

    dataframe: pd.DataFrame = pd.read_csv(
        in_filepath, sep=";", keep_default_na=False, encoding="latin1", dtype=object
    )

    dataframe.columns = [
        "cnpj",
        "denominacao_social",
        "denominacao_comercial",
        "data_registro",
        "data_cancelamento",
        "motivo_cancelamento",
        "situacao",
        "data_inicio_situacao",
        "categoria_registro",
        "subcategoria_registro",
        "controle_acionario",
        "tipo_endereco",
        "logradouro",
        "complemento",
        "bairro",
        "municipio",
        "sigla_uf",
        "cep",
        "ddd",
        "telefone",
        "valor_patrimonial_liquido",
        "data_patrimonio_liquido",
        "email",
        "website",
    ]

    dataframe["cnpj"] = dataframe["cnpj"].str.replace(".", "")
    dataframe["cnpj"] = dataframe["cnpj"].str.replace("/", "")
    dataframe["cnpj"] = dataframe["cnpj"].str.replace("-", "")

    for col in dataframe.columns:
        if is_string_dtype(dataframe[col]):
            dataframe[col] = dataframe[col].apply(
                lambda x: unidecode(x) if isinstance(x, str) else x
            )

    dataframe.to_csv(ou_filepath, index=False)

    return ou_filepath


@task
def extract_last_date(
    dataset_id: str,
    table_id: str,
    billing_project_id: str,
    var_name: str,
) -> datetime:
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

    query_bd = f"""
    SELECT MAX({var_name}) as max_date
    FROM
    `{billing_project_id}.{dataset_id}.{table_id}`
    """

    t = bd.read_sql(
        query=query_bd,
        billing_project_id=billing_project_id,
        from_file=True,
    )

    data = t["max_date"][0]

    log(f"A data mais recente da tabela é: {data}")

    return str(data)
