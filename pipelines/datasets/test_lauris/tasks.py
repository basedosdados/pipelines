# -*- coding: utf-8 -*-
"""
Tasks for test_lauris
"""

import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from prefect import task
import requests
import urllib.request
import pandas as pd
import json

from pipelines.utils.utils import (
    log,
)
from pipelines.datasets.br_bd_metadados.utils import (
    get_temporal_coverage_list,
)
from pipelines.constants import constants


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def crawler_available_options():

    """
    Pulls metadata about available options from Base dos Dados' APIs and returns it structured in a csv file.
    Args:
    Returns:
    Path to csv file with structured metadata about available options.
    """

    url = "https://basedosdados.org/api/3/action/bd_available_options"

    response = urllib.request.urlopen(url)

    data = json.loads(response.read())

    lista_geral = []
    for element in data["result"].keys():
        for chave in data["result"][element].keys():
            valor = data["result"][element][chave]
            lista_geral.append([element, chave, valor])

    df = pd.DataFrame(lista_geral, columns=["elemento", "chave", "valor"])

    os.system("mkdir -p /tmp/data/available_options")
    df.to_csv("/tmp/data/available_options/available_options.csv", index=False)

    return "/tmp/data/available_options/available_options.csv"
