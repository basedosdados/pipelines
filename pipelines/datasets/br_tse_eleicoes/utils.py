# -*- coding: utf-8 -*-
"""
General purpose functions for the br_tse_eleicoes project
"""
# pylint: disable=invalid-name,line-too-long
import re

import basedosdados as bd
import pandas as pd
from datetime import datetime
import unicodedata
import zipfile
import os
import requests
from bs4 import BeautifulSoup
from pipelines.datasets.br_tse_eleicoes.constants import constants as tse_constants
from pipelines.utils.utils import log


def conv_data(date: str, birth: bool = False) -> str:
  try:
    data_datetime = datetime.strptime(date, "%d/%m/%Y")
    idade = datetime.now().year - data_datetime.year
    if not 18 <= idade < 120 and birth:
      raise Exception("Idade Inválida")
    return data_datetime.strftime('%Y-%m-%d')
  except:
    return ""

def slugify(s: str) -> str:

    s = s.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = s.encode("ascii", "ignore")
    s = s.decode("utf-8")
    s = s.lower().strip()
    return s

def add_ensino(instrucao: str) -> str:

  if instrucao.count("superior"):
    instrucao = f"ensino {instrucao}"

  return instrucao

def request_extract_by_select(url: str, select: str, text: bool = False,
                              atributo: str = "href") ->  str | list[str] | None:

  response = requests.get(url)

  suop = BeautifulSoup(response.text)

  if text:
    return suop.select_one(select).text

  return suop.select_one(select).get(atributo)

def download_and_extract_zip(url: str, save_path: str = "/tmp/data/input/",
                             path_input: str = "/tmp/data/input/", chunk_size=128) -> None:
    """
    Gets all csv files from a url and saves them to a directory.
    """
    os.makedirs(path_input, exist_ok=True)

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }

    r = requests.get(url, headers=request_headers, stream=True, timeout=10)

    save_path = os.path.join(save_path, url.split("/")[-1])

    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    with zipfile.ZipFile(save_path) as z:
        z.extractall(path_input)

def form_df_base() -> pd.DataFrame:

    canditados24 = pd.read_csv("/tmp/data/input/consulta_cand_2024_BRASIL.csv",
                                   sep=";", encoding="ISO-8859-1", dtype=str)
    complementar24 = pd.read_csv("/tmp/data/input/consulta_cand_complementar_2024_BRASIL.csv",
                                 sep=";", encoding="ISO-8859-1", dtype=str)

    municipios = bd.read_sql(
        tse_constants.QUERY_MUNIPIPIOS.value,
        from_file=True,
        billing_project_id="basedosdados-dev"
    )

    temp_merge_left = pd.merge(canditados24, complementar24, left_on="SQ_CANDIDATO", right_on='SQ_CANDIDATO', how='left')

    temp_merge_left["SG_UE"] = temp_merge_left["SG_UE"].str.lstrip("0") # Precisamos limpas alguns zero a esquerda

    temp_merge_left = pd.merge(temp_merge_left, municipios, left_on="SG_UE", right_on="id_municipio_tse", how="left")

    temp_merge_left["id_candidato_bd"] = ""

    base = temp_merge_left.loc[:, tse_constants.ORDER.value.values()]

    base.fillna("", inplace=True)

    base.columns = tse_constants.ORDER.value.keys()

    base = format_df_base(base)

    return base


def format_df_base(base: pd.DataFrame) -> pd.DataFrame:

    # Remover valores indesejados da colunas

    removes = ["#NULO", "#NE", "NÃO DIVULGÁVEL", "Não Divulgável",
           "-1", "-4", "-3"]

    removes_upper = {remove.upper(): "" for remove in removes}

    base.replace(removes_upper, regex=False, inplace=True)

    # Formatar datas

    base["data_eleicao"] = base["data_eleicao"].apply(conv_data)
    base["data_nascimento"] = base["data_nascimento"].apply(lambda date: conv_data(date, birth=True))

    # Formatar Colunas com slug

    slug_columns_format = ["tipo_eleicao", "cargo", "situacao",
                       "ocupacao", "genero", "instrucao",
                       "estado_civil", "nacionalidade", "raca"]

    base[slug_columns_format] = base[slug_columns_format].applymap(slugify)

    base["instrucao"] = base["instrucao"].apply(add_ensino)

    # Colocar nomes como title como dados em produção

    for column_to_format in ["municipio_nascimento", "nome", "nome_urna"]:
        base[column_to_format] = base[column_to_format].str.title()

    # trocar `brasileira nata` para `brasileira`

    base["nacionalidade"] = base["nacionalidade"].str.replace("brasileira nata", "brasileira")

    base.drop_duplicates(inplace=True)

    return base


def form_df_bens_candidato() -> pd.DataFrame:

  bens24 = pd.read_csv("/tmp/data/input/bem_candidato_2024_BRASIL.csv" ,
                  sep=";", encoding="ISO-8859-1", decimal=",", dtype={
                  "CD_ELEICAO": str,
                  "SQ_CANDIDATO": str,
                  })

  bens24["id_candidato_bd"] = ""

  base = bens24.loc[:, tse_constants.ORDER_BENS.value.values()]
  base.columns = tse_constants.ORDER_BENS.value.keys()

  base["data_eleicao"] = base["data_eleicao"].apply(conv_data)
  base["tipo_eleicao"]= base["tipo_eleicao"].apply(slugify)

  base.drop_duplicates(inplace=True)

  return base