# -*- coding: utf-8 -*-
"""
Tasks for br_me_novo_caged
"""

import re
from glob import glob
from datetime import timedelta

from prefect import task
import pandas as pd
from unidecode import unidecode
from tqdm import tqdm

from pipelines.constants import constants


# pylint: disable=C0103
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def build_partitions(group: str) -> str:
    """
    build partitions from gtup files

    group: cagedmov | cagedfor | cagedexc
    """
    input_files = glob(f"/tmp/novo_caged/{group}/input/*txt")
    for filename in tqdm(input_files):
        dataframe = pd.read_csv(filename, sep=";", dtype={"uf": str})
        date = re.search(r"\d+", filename).group()
        ano = date[:4]
        mes = int(date[-2:])

        dataframe.columns = [unidecode(col) for col in dataframe.columns]

        dict_uf = {
            "11": "RO",
            "12": "AC",
            "13": "AM",
            "14": "RR",
            "15": "PA",
            "16": "AP",
            "17": "TO",
            "21": "MA",
            "22": "PI",
            "23": "CE",
            "24": "RN",
            "25": "PB",
            "26": "PE",
            "27": "AL",
            "28": "SE",
            "29": "BA",
            "31": "MG",
            "32": "ES",
            "33": "RJ",
            "35": "SP",
            "41": "PR",
            "42": "SC",
            "43": "RS",
            "50": "MS",
            "51": "MT",
            "52": "GO",
            "53": "DF",
        }

        dataframe["uf"] = dataframe["uf"].map(dict_uf)

        for state in dict_uf.values():
            data = dataframe[dataframe["uf"] == state]
            data.drop(["competenciamov", "uf"], axis=1, inplace=True)
            data.to_csv(
                f"/tmp/novo_caged/{group}/ano={ano}/mes={mes}/sigla_uf={state}/data.csv",
                index=False,
            )
            del data
        del dataframe

    return "/tmp/novo_caged/{group}/"
