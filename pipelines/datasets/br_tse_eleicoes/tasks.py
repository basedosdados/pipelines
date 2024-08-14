# -*- coding: utf-8 -*-
"""
Tasks for br_tse_eleicoes
"""
import os
import re
import zipfile

# pylint: disable=invalid-name,line-too-long
from datetime import timedelta
from glob import glob
from itertools import product

import numpy as np
import pandas as pd
import requests
from prefect import task
from tqdm import tqdm
from unidecode import unidecode
from datetime import datetime
from dateparser import parse
from bs4 import BeautifulSoup
import basedosdados as bd


from pipelines.constants import constants
from pipelines.datasets.br_tse_eleicoes.constants import constants as tse_constants

from pipelines.datasets.br_tse_eleicoes.utils import (
    download_and_extract_zip,
    request_extract_by_select,
    form_df_base,
    form_df_bens_candidato
)
from pipelines.utils.utils import log


@task
def download_urls(urls: list) -> None:
    """
    Gets all csv files from a url and saves them to a directory.
    """
    for url in urls:
        download_and_extract_zip(url)


@task
def get_data_source_max_date() -> datetime | None:

  base_url = "https://dadosabertos.tse.jus.br"

  url = base_url + "/dataset/activity/candidatos-2024"

  endpoint = request_extract_by_select(url, "span[class='date'] a:nth-child(2)")

  changes_url = base_url + endpoint

  data = request_extract_by_select(changes_url, "select[name='new_id'] option:nth-child(1)",
                                   text=True)

  last_update = parse(data, languages=["pt"])

  return last_update


@task
def preparing_data() -> None:

    base = form_df_base()
    # Etapa de salvar a base

    path_output = os.path.join( "/tmp/data/", "output", "ano=2024")

    file_path = os.path.join(path_output, "candidatos.csv")

    os.makedirs(path_output, exist_ok=True)

    base.to_csv(file_path, index=False)



@task
def preparing_data_bens_candidato() -> None:

    base = form_df_bens_candidato()
    # Etapa de salvar a base

    path_output = os.path.join( "/tmp/data/", "output", "ano=2024")

    file_path = os.path.join(path_output, "bens_candidato.csv ")

    os.makedirs(path_output, exist_ok=True)

    base.to_csv(file_path, index=False)