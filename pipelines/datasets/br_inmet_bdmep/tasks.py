# -*- coding: utf-8 -*-
"""
Tasks for br_inmet_bdmep
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_inmet_bdmep.utils import (
    get_clima_info,
    download_inmet,
)
from pipelines.constants import constants

import pandas as pd

# import rasterio
# import geopandas as gpd
import os
import numpy as np
import glob

# import datetime
# import re
from datetime import datetime, time

# from unidecode import unidecode
from prefect import task

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

    os.makedirs(os.path.join(f"/tmp/data/output/microdados/ano={year}"), exist_ok=True)
    name = os.path.join(
        f"/tmp/data/output/microdados/ano={year}/", f"microdados_{year}.csv"
    )
    base.to_csv(name, index=False)

    return "/tmp/data/output/microdados/"
