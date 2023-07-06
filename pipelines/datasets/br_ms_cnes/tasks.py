# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""


from prefect import task
from datetime import timedelta
from pipelines.utils.utils import log
from pipelines.constants import constants

import pandas as pd
from ftplib import FTP
import wget
import os
from rpy2.robjects.packages import importr
import rpy2.robjects.packages as rpackages
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri

import datetime as dt

from pipelines.datasets.br_ms_cnes.utils import (
    list_all_cnes_dbc_files,
    year_month_sigla_uf_parser,
    pre_cleaning_to_utf8,
)


# task to parse files and select files
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def parse_latest_cnes_dbc_files():
    """
    This task access DATASUS FTP to retrive CNES database ST group latest .DBC file paths
    """

    available_dbs = list_all_cnes_dbc_files(database="CNES", CNES_group="ST")

    today = dt.datetime.today()
    today = today.strftime("%Y%m")
    today = today[2:]
    today = str(int(today) - 2)
    log(f"the YYYY MM {today}")
    # todo: generalize parse current month -2

    list_files = []

    for file in available_dbs:
        if file[-8:-4] == today:
            list_files.append(file)

    log(f"the following files were selected: {list_files}")

    return list_files


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def access_ftp_donwload_files(file_list: list, path: str, table: str) -> list[str]:
    """This function recives a list of files and
    download them from DATASUS FTP server

    Args:
        file_list (list): a file list extracted with the function choose_latest_cnes_dbc_files

    Returns:
        pd.DataFrame: a list with the downloaded files paths
    """
    dbc_files_path_list = list()

    for file in file_list:
        # build partition dirs

        year_month_sigla_uf = year_month_sigla_uf_parser(file=file)

        log(f"the file {file} was parsed to {year_month_sigla_uf}")
        input_path = path + table + "/" + year_month_sigla_uf

        os.system(f"mkdir -p {input_path}")

        log(f"created input dir {path + table + '/' + year_month_sigla_uf}")

        url = f"ftp://ftp.datasus.gov.br/{file}"
        # access ftp
        wget.download(url, out=input_path)

        # list downloaded files
        dbc_file = os.listdir(input_path)

        # append to list
        complete_path = input_path + "/" + dbc_file[0]
        log(f"The complete path is -- {complete_path}")

        dbc_files_path_list.append(complete_path)

        log(f"The file {dbc_file} is at the {input_path}")

    return dbc_files_path_list


# task to convert dbc to csv and save to a partitioned dir
@task
def read_dbc_save_csv(file_list: list, path: str, table: str) -> str:
    """
    Convert dbc to csv
    """

    # import R's utility package
    utils = rpackages.importr("utils")

    # select a mirror for R packages
    utils.chooseCRANmirror(ind=1)

    log("installing read.dbc package")
    utils.install_packages("read.dbc")
    readdbc = importr("read.dbc")

    # list files
    for file in file_list:
        log(f"the file {file} is being converted to csv")
        # read dbc
        dbc_file = readdbc.read_dbc(file)
        # convert from r to pandas
        # https://rpy2.github.io/doc/latest/html/generated_rst/pandas.html#from-r-to-pandas

        # parse year month sigla_uf
        year_month_sigla_uf = year_month_sigla_uf_parser(file=file)
        log(f"year_month_sigla_uf of {file} parsed")
        output_path = path + table + "/" + year_month_sigla_uf

        os.system(f"mkdir -p {output_path}")

        log(f"created output partition dir {path + table + '/'+ year_month_sigla_uf}")

        output_file = output_path + "/" + table + ".csv"

        log(f"{file} 1 saved")
        # salvar df
        dbc_file.to_csvfile(
            output_file,
            sep=",",
            na="",
            row_names=False,
        )

        # ler df

        log("file 2 being read")
        df = pd.read_csv(file, dtype=str, encoding="latin1")

        # tratar
        df = pre_cleaning_to_utf8(df)

        # salvar de novo
        df.to_csv(output_file, sep=",", na_rep="", index=False, encoding="utf-8")

        log(
            f"The file {file} was converted to csv and saved at {output_path + file + '.csv'}"
        )

    return path + table
