# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""


from prefect import task
from datetime import timedelta
from pipelines.utils.utils import log
from pipelines.constants import constants
import re
import os
import pandas as pd

from pipelines.datasets.br_rf_cafir.constants import constants as br_rf_cafir_constants
from pipelines.datasets.br_rf_cafir.utils import (
    parse_date_parse_files,
    download_csv_files,
)


@task
def download_files_parse_date(url):
    log("########  download_files_parse_data  ########")

    date_files = parse_date_parse_files(url)

    return date_files


@task
def parse_data(url, other_task_output):
    # repeat it 20 times
    log(f"Other task output {other_task_output}")
    date = other_task_output[1]
    log(f"###### Extraindo dados para data: {date}")
    files_list = other_task_output[2]

    counter = 0

    for file in files_list:
        counter += 1

        log(f"Dowloading file: {file} from {url}")
        # donwload file
        download_csv_files(
            base_url=url,
            download_directory=br_rf_cafir_constants.PATH.value[0],
            files_names=file,
        )

        # read file
        log(f"Reading file: {file}")
        file_path = br_rf_cafir_constants.PATH.value[0] + file

        df = pd.read_fwf(
            file_path,
            widths=[30, 55, 2, 56, 40, 2, 40, 8, 8, 3, 1],
            names=[
                "id",
                "nome",
                "situacao",
                "endereco",
                "zona_redefinir",
                "sigla_uf",
                "municipio",
                "cep",
                "data_inscricao",
                "status_rever",
                "cd",
            ],
            encoding="latin1",
        )
        # adiciona coluna com a data
        df["data"] = date[0]

        log(f"Saving file: {file}")
        # instead of file i need a counter. Each interation of the loop +1

        save_path = (
            br_rf_cafir_constants.PATH.value[1]
            + "imoveis_rurais_"
            + str(counter)
            + ".csv"
        )

        # save new file as csv
        df.to_csv(save_path, index=False, sep=",", na_rep="", encoding="utf-8")

        log(f"no dir input tem: {os.listdir(br_rf_cafir_constants.PATH.value[0])}")
        # remove o arquivo de input
        os.system("rm -rf " + br_rf_cafir_constants.PATH.value[0] + "*")

        log(f"no dir input tem: {os.listdir(br_rf_cafir_constants.PATH.value[0])}")

    # ath to saved files
    return br_rf_cafir_constants.PATH.value[1]
