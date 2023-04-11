# -*- coding: utf-8 -*-
"""
Tasks for br_inmet_bdmep
"""
from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_inmet_bdmep.utils import (
    get_clima_info,
    download_inmet,
    year_list,
)
from pipelines.constants import constants

import pandas as pd
import os
import numpy as np
import glob
from datetime import datetime, time
from prefect import task
from pipelines.datasets.br_inmet_bdmep.constants import constants as inmet_constants

# pylint: disable=C0103


@task
def get_base_inmet(year: int) -> str:
    """
    Faz o download dos dados meteorológicos do INMET, processa-os e salva os dataframes resultantes em arquivos CSV.

    Retorna:
    - str: o caminho para o diretório que contém os arquivos CSV de saída.
    """

    download_inmet(year)

    files = glob.glob(os.path.join(f"/tmp/data/input/{year}/", "*.CSV"))

    base = pd.concat([get_clima_info(file) for file in files], ignore_index=True)

    # ordena as colunas
    ordem = inmet_constants.COLUMNS_ORDER.value
    base = base[ordem]

    # Salva o dataframe resultante em um arquivo CSV
    os.makedirs(os.path.join(f"/tmp/data/output/microdados/ano={year}"), exist_ok=True)
    name = os.path.join(
        f"/tmp/data/output/microdados/ano={year}/", f"microdados_{year}.csv"
    )
    base.to_csv(name, index=False)

    return "/tmp/data/output/microdados/"
