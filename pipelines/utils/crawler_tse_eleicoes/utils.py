# -*- coding: utf-8 -*-
"""
General purpose functions for the br_tse_eleicoes project
"""
# pylint: disable=invalid-name,line-too-long

import basedosdados as bd
import pandas as pd
from datetime import datetime
import unicodedata
import zipfile
import requests
from pipelines.utils.crawler_tse_eleicoes.constants import constants as tse_constants
import tempfile
from pathlib import Path



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


def flows_catalog() -> dict:

   catalog = {
    "candidatos": {
      "flow": Candidatos,
      "urls": tse_constants.CANDIDATOS_URLS.value,
      "source": "consulta_cand_2024_BRASIL.csv"
                   },
    "bens_candidato": {
      "flow": BensCandidato,
      "urls": [tse_constants.BENS_CANDIDATOS24.value],
      "source": "bem_candidato_2024_BRASIL.csv"
                   },
    "despesas_candidato": {
      "flow": None,
      "urls": [None],
      "source": "despesas_contratadas_candidatos_2024_BRASIL.csv"
                   },
    "receitas_candidato": {
      "flow": None,
      "urls": [None],
      "source": "receitas_candidatos_2024_BRASIL.csv"
                   }
  }

   return catalog


# Class Principal

class BrTseEleicoes:

  def __init__(self, urls: list, table_id: str, source: str, year: int = 2024, mode: str = "dev"):

    self.urls = urls
    self.year = year
    self.table_id = table_id
    self.source = source
    self.billing_project_id = tse_constants.MODE_TO_PROJECT_DICT.value[mode]
    self.query = tse_constants.QUERY_COUNT_MODIFIED.value.format(table_id=table_id,
                                                                 mode=self.billing_project_id, year=year)
    self.base_path = Path(tempfile.gettempdir(), "data")
    self.path_input = self.base_path / "input"
    self.path_output = self.base_path / "output"
    self.df_main = self.df_complement = self.path_main = self.path_complement = None

  def download_urls(self) -> None:
      """
      Gets all csv files from a url and saves them to a directory.
      """
      for url in self.urls:
          self.download_extract_zip(url)


  def download_extract_zip(self, url: str, chunk_size=128) -> None:
    """
    Gets all csv files from a url and saves them to a directory.
    """
    self.path_input.mkdir(parents=True, exist_ok=True)

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }

    r = requests.get(url, headers=request_headers, stream=True, timeout=60)

    save_path = self.path_input / url.split("/")[-1]

    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    with zipfile.ZipFile(save_path) as z:
        z.extractall(self.path_input)


    if not self.path_main:

      self.path_main = self.path_input / self.source
      self.df_main = pd.read_csv(self.path_main, sep=";", encoding="ISO-8859-1", dtype=str)

    else:

      self.path_complement = self.path_input / f"{save_path.stem}_BRASIL.csv"
      self.df_complement = pd.read_csv(self.path_complement, sep=";", encoding="ISO-8859-1", dtype=str)

  def get_data_source_max_date(self) -> bool:

    df_prod = bd.read_sql(
        self.query,
        from_file=True,
        billing_project_id=self.billing_project_id
    )
    prod_len, last_modified = df_prod.iloc[0].values

    if self.df_main.shape[0] > prod_len:
      return datetime.today()

    return last_modified

  def formatar(self) -> None:

    base = self.form_df_base()
    # Etapa de salvar a base

    path_output = self.path_output / f"ano={self.year}"

    file_path = path_output / f"{self.table_id}.csv"

    path_output.mkdir(parents=True, exist_ok=True)

    base.to_csv(file_path, index=False)


  def form_df_base(self) -> None:
    pass

# Classes Dos Flows

class Candidatos(BrTseEleicoes):

  def form_df_base(self) -> pd.DataFrame:

      municipios = bd.read_sql(
          tse_constants.QUERY_MUNIPIPIOS.value,
          from_file=True,
          billing_project_id=self.billing_project_id
      )

      temp_merge_left = pd.merge(self.df_main, self.df_complement,
                                 left_on="SQ_CANDIDATO", right_on='SQ_CANDIDATO', how='left')

      temp_merge_left["SG_UE"] = temp_merge_left["SG_UE"].str.lstrip("0") # Precisamos limpas alguns zero a esquerda

      temp_merge_left = pd.merge(temp_merge_left, municipios,
                                 left_on="SG_UE", right_on="id_municipio_tse", how="left")

      temp_merge_left["id_candidato_bd"] = ""

      base = temp_merge_left.loc[:, tse_constants.ORDER.value.values()]

      base.fillna("", inplace=True)

      base.columns = tse_constants.ORDER.value.keys()

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


class BensCandidato(BrTseEleicoes):

  def form_df_base(self) -> pd.DataFrame:

    self.df_main = pd.read_csv(self.path_main ,
                    sep=";", encoding="ISO-8859-1", decimal=",", dtype={
                    "CD_ELEICAO": str,
                    "SQ_CANDIDATO": str,
                    })

    self.df_main["id_candidato_bd"] = ""

    base = self.df_main.loc[:, tse_constants.ORDER_BENS.value.values()]
    base.columns = tse_constants.ORDER_BENS.value.keys()

    base["data_eleicao"] = base["data_eleicao"].apply(conv_data)
    base["tipo_eleicao"]= base["tipo_eleicao"].apply(slugify)

    base.drop_duplicates(inplace=True)

    return base