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
    Baixa e processa dados climáticos do INMET (Instituto Nacional de Meteorologia) para um determinado ano e salva em arquivo CSV.

    Parâmetros
    ----------
    year: int
        Ano dos dados climáticos a serem baixados e processados.

    Retorno
    -------
    None
    """
    download_inmet(year)

    files = glob.glob(os.path.join(f"/tmp/data/input/{year}/", "*.CSV"))

    base = pd.concat([get_clima_info(file) for file in files], ignore_index=True)

    # ordena as colunas
    ordem = inmet_constants.COLUMNS_ORDER.value
    base = base[ordem]

    os.makedirs(os.path.join(f"/tmp/data/output/microdados/ano={year}"), exist_ok=True)
    name = os.path.join(
        f"/tmp/data/output/microdados/ano={year}/", f"microdados_{year}.csv"
    )
    base.to_csv(name, index=False)

    return "/tmp/data/output/microdados/"
