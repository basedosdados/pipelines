# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""


from datetime import datetime

import pandas as pd
import requests

from pipelines.utils.utils import log, to_partitions


import asyncio
import aiohttp
from aiohttp.client import ClientSession
from typing import NamedTuple, List
from more_itertools import batched
from time import sleep, time
from datetime import datetime
import glob
import requests
import pandas as pd
from pathlib import Path
import tempfile


# Acesse Site

async def download_link(row: NamedTuple, session: ClientSession) -> list:

  # try:
  async with session.get(row.link) as response:

          result = await response.json()

          return [row, result]


async def download_all(rows: list):
    my_conn = aiohttp.TCPConnector(limit=20, ssl=False)
    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(connector=my_conn, timeout=timeout) as session:
        slots = []
        for row in rows:
            slot = asyncio.ensure_future(download_link(row=row, session=session))
            slots.append(slot)
        await asyncio.gather(*slots, return_exceptions=True)# the await must be nest inside of the session
    return slots


# Geral

def form_link(base_url: str, *id_elements) -> str:
  return base_url.format(*id_elements)


def form_path_coleta(*id_elements) -> str:
  return "{0}_{1}_{2}_{3}.csv".format(*id_elements)


def make_list_chunks(values: list, chunk_size:int = 2000) -> list:
  resquet_in_chunks = [chunk for chunk in batched(values, chunk_size)]
  return resquet_in_chunks


def column_to_date(date_string: str):
  try:
    date_object = datetime.strptime(date_string, "[%Y, %m, %d]")
    if datetime(1984, 1, 1) <= date_object <= datetime.today():
      return date_object.strftime("%Y-%m-%d")
  except:
    return pd.NA


def get_year(date_string: str):
  try:
    year = date_string.split("-")[0]
    return year
  except:
    return pd.NA


# Classes

class PartidariaDataFrames:

  def __init__(self):

    self.partidos = self.create_partidos()
    self.municipios = self.create_municipios()
    self.zonas = self.create_zonas()
    self.coletas = self.create_coletas()


  def create_partidos(self) -> pd.DataFrame:

    url_base_partido = "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1/partidos"

    html = requests.get(url_base_partido)

    df_partidos = pd.DataFrame(html.json(), dtype=str)

    return df_partidos


  def create_municipios(self) -> pd.DataFrame:

    url_base_municipios = "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1/localidade/{0}/municipios"
    url_base_zona_eleitoral = "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1/zona/municipio/{0}/zonasEleitorais"

    links_municipios = pd.DataFrame([{"link": url_base_municipios.format(id_uf)} for id_uf in range(1,29)])

    tasks = asyncio.run(download_all(links_municipios.itertuples()))

    datas = [slot.result() for slot in tasks if not slot.exception()]

    municipios = [municipio for _, estado in datas for municipio in estado]
    ufs = [municipio.pop("uf") for _, estado in datas for municipio in estado]

    df_municipios = pd.DataFrame(municipios, dtype=str)
    df_ufs = pd.DataFrame(ufs, dtype=str)

    localization = pd.merge(df_municipios, df_ufs, left_index=True, right_index=True)
    localization["link"] = [form_link(url_base_zona_eleitoral, row.codObjeto_x) for row in localization.itertuples()]

    return localization


  def create_zonas(self) -> pd.DataFrame:

    tasks = asyncio.run(download_all(self.municipios.itertuples()))
    datas = [slot.result() for slot in tasks if not slot.exception()]

    zonas = [zona.update({"link": row.link}) for row, zonas in datas for zona in zonas]
    zonas = [zona for email, zonas in datas for zona in zonas]

    df_zonas = pd.DataFrame(zonas, dtype=str)
    zonas_municipios = pd.merge(df_zonas, self.municipios, left_on="link", right_on="link")

    return zonas_municipios

  def create_coletas(self) -> pd.DataFrame:

    url_coleta = "https://filia2-consulta.tse.jus.br/filia-consulta/rest/v1/relacao-filiados?sgUe={0}&cdMunicipio={1}&cdZona={2}&sqPartido={3}&currentPage=0&pageSize=10000"

    rows = [{"sigla_uf": row_infos.sglUf,
            "id_municipio": row_infos.codObjeto_x,
            "id_zona": row_infos.codObjeto,
            "id_partido": partido.id}
        for partido in self.partidos.itertuples() for row_infos in self.zonas.itertuples()]

    df_coleta = pd.DataFrame(rows, dtype=str)
    paths = [form_path_coleta(*row) for row in df_coleta.itertuples(index=False)]
    links = [form_link(url_coleta, *row) for row in df_coleta.itertuples(index=False)]
    df_coleta["path"] = paths
    df_coleta["link"] = links

    return df_coleta


class PartidariaColetaTramento(PartidariaDataFrames):

  def __init__(self):

    super().__init__()

    self.base_path = Path(tempfile.gettempdir(), "data")
    self.path_base_segments = self.base_path / "input_segments"
    self.path_base_input = self.base_path /  "input"
    self.path_base_output = self.base_path / "output"


  def coleta(self) -> None:

    chunk_rows = make_list_chunks(self.coletas[self.coletas.sigla_uf.isin(["AC", "RR", "AP", "DF", "RO", "AM"])].itertuples(), chunk_size=1000)

    for n, chuck in enumerate(chunk_rows):

      log(f"Chuck {n + 1}/{len(chunk_rows)}")

      start_time = time()

      tasks = asyncio.run(download_all(chuck))
      datas = [slot.result() for slot in tasks if not slot.exception()]

      for row, json_row in datas:
        self.save_json_df(row, json_row["entitys"])

      end_time = time() - start_time

      log(f"Duração da execução: {end_time:.2f}")
      sleep(end_time)
      log(f"Temos de {len(datas)} dados extraidos\n{'-' * 30}")

  def save_json_df(self, row: NamedTuple, rows: list) -> None:

    path = self.path_base_segments / row.sigla_uf
    path.mkdir(exist_ok=True, parents=True)
    path_file = path / row.path

    df_json = pd.DataFrame(rows, dtype=str)
    df_json.to_csv(path_file, index=False)


  def form_df_by_uf(self, sigla: str) -> None:

    dfs = []

    self.path_base_input.mkdir(exist_ok=True, parents=True)

    math_file = f"{self.path_base_segments}/{sigla}/*.csv"

    files = glob.glob(math_file)

    for file_csv in files:
      try:
        dfs.append(pd.read_csv(file_csv, dtype=str))
      except:
        pass

    if dfs:

      df_sigla_uf = pd.concat(dfs)

      df_sigla_uf.to_csv(self.path_base_input / f"{sigla}.csv", index=False)


  def merge_segments(self):
    for uf in self.coletas.sigla_uf.unique():
      self.form_df_by_uf(uf)


  def format_and_partition_df(self, path: str) -> None:

    temp_df = pd.read_csv(path, dtype=str)

    data_columns_format = ["dtFiliacao", "dtDesfiliacao", "dtExclusao",
                        "dtCancelamento"]

    temp_df[data_columns_format] = temp_df[data_columns_format].applymap(column_to_date)

    temp_df[temp_df.dtFiliacao.notna()]["dtFiliacao"]

    temp_df["ano"] = temp_df["dtFiliacao"].apply(get_year)

    temp_df = temp_df.dropna(subset="ano")

    temp_df["sigla_uf"] = temp_df["sgUe"].values

    to_partitions(temp_df, ["ano", "sigla_uf"],
                  savepath=str(self.path_base_output), file_type="csv")

  def processing(self):

    math_path = str(self.path_base_input / "[A-Z][A-Z].csv")
    path_dataframes = glob.glob(math_path)

    for path in path_dataframes:
      self.format_and_partition_df(path)
