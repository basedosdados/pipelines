"""
Tasks for br_cvm_administradores_carteira
"""

import os
import shutil
from pathlib import Path
from typing import Union

import requests
import pandas as pd
from prefect import task
from pandas.api.types import is_string_dtype
from unidecode import unidecode

from pipelines.utils.utils import log


@task
def crawl(root: str, url: str, chunk_size=128) -> None:
    """Download and unzip dataset br_cvm_administradores_carteira"""
    filepath = f"{root}/data.zip"
    os.makedirs(root, exist_ok=True)

    response = requests.get(url)

    with open(filepath, "wb") as file:
        for chunk in response.iter_content(chunk_size=chunk_size):
            file.write(chunk)

    shutil.unpack_archive(filepath, extract_dir=root)


@task
def clean_table_responsavel(root: str) -> str:
    # pylint: disable=invalid-name
    """Clean table pessoa_fisica"""
    in_filepath = f"{root}/cad_adm_cart_resp.csv"
    ou_filepath = f"{root}/bd_responsavel.csv"

    df: pd.DataFrame = pd.read_csv(
        in_filepath,
        sep=";",
        na_values="",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    df.columns = ["cnpj", "nome", "tipo"]

    df["cnpj"] = df["cnpj"].str.replace(".", "")
    df["cnpj"] = df["cnpj"].str.replace("/", "")
    df["cnpj"] = df["cnpj"].str.replace("-", "")

    for col in df.columns:
        if is_string_dtype(df[col]):
            df[col] = df[col].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    df.to_csv(ou_filepath, index=False)

    return ou_filepath


@task
def clean_table_pessoa_fisica(root: str) -> str:
    # pylint: disable=invalid-name
    """Clean table pessoa_fisica"""
    in_filepath = f"{root}/cad_adm_cart_pf.csv"
    ou_filepath = f"{root}/bd_pessoa_fisica.csv"

    df: pd.DataFrame = pd.read_csv(
        in_filepath,
        sep=";",
        keep_default_na=False,
        encoding="latin1",
        dtype=object,
    )

    df.columns = [
        "nome",
        "data_registro",
        "data_cancelamento",
        "motivo_cancelamento",
        "situacao",
        "data_inicio_situacao",
        "categoria_registro",
    ]

    for col in df.columns:
        if is_string_dtype(df[col]):
            df[col] = df[col].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    df.to_csv(ou_filepath, index=False)

    return ou_filepath


@task
def clean_table_pessoa_juridica(root: str) -> str:
    # pylint: disable=invalid-name
    """Clean table pessoa_fisica"""
    in_filepath = f"{root}/cad_adm_cart_pj.csv"
    ou_filepath = f"{root}/bd_pessoa_juridica.csv"

    df: pd.DataFrame = pd.read_csv(
        in_filepath, sep=";", keep_default_na=False, encoding="latin1", dtype=object
    )

    df.columns = [
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

    df["cnpj"] = df["cnpj"].str.replace(".", "")
    df["cnpj"] = df["cnpj"].str.replace("/", "")
    df["cnpj"] = df["cnpj"].str.replace("-", "")

    for col in df.columns:
        if is_string_dtype(df[col]):
            df[col] = df[col].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

    df.to_csv(ou_filepath, index=False)

    return ou_filepath
