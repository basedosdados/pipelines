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

from pipelines.datasets.br_ms_cnes.constants import constants as cnes_constants
from pipelines.datasets.br_ms_cnes.utils import (
    list_all_cnes_dbc_files,
    year_month_sigla_uf_parser,
    pre_cleaning_to_utf8,
    check_and_create_column,
    if_column_exist_delete,
)


# task to parse files and select files
@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def parse_latest_cnes_dbc_files(database: str, cnes_group: str) -> list[str]:
    """
    This task access DATASUS FTP to retrive CNES database ST or PF group latest .DBC file paths
    """
    cnes_database = database
    cnes_group_file = cnes_group

    available_dbs = list_all_cnes_dbc_files(
        database=cnes_database, CNES_group=cnes_group_file
    )

    # generate current date
    today = dt.date.today()

    current_month = today.month
    # generate two last digits of current year to match datasus FTP year representation format
    # eg. 2023 -> 23
    current_year = str(today.year)
    current_year = current_year[2:]

    # GENERATE_MONTH_TO_PARSE is a dict that given a int representing an month (Eg. 1 = january)
    # gives a string representing the current minus 2. (Eg. 1 = january : '11' november)
    month_to_parse = cnes_constants.GENERATE_MONTH_TO_PARSE.value[current_month]
    year_month_to_parse = str(current_year) + str(month_to_parse)

    log(f"the YEARMONTH being used to parse files is: {year_month_to_parse}")

    list_files = []

    for file in available_dbs:
        if file[-8:-4] == year_month_to_parse:
            list_files.append(file)

    # check if list is null
    if len(list_files) == 0:
        raise ValueError(
            f"cnes files parsed with {year_month_to_parse} were not found. It probably indicates that those files have not been released yet."
        )

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
        df = pd.read_csv(output_file, dtype=str, encoding="latin1")

        # tratar
        list_columns_to_delete = [
            "AP01CV07",
            "AP02CV07",
            "AP03CV07",
            "AP04CV07",
            "AP05CV07",
            "AP06CV07",
            "AP07CV07",
        ]
        list_columns_to_keep = [
            "COMPETEN",
            "CNES",
            "UFMUNRES",
            "NOMEPROF",
            "CNS_PROF",
            "CBO",
            "REGISTRO",
            "CONSELHO",
            "TERCEIRO",
            "VINCULAC",
            "VINCUL_C",
            "VINCUL_A",
            "VINCUL_N",
            "PROF_SUS",
            "PROFNSUS",
            "HORAOUTR",
            "HORAHOSP",
            "HORA_AMB",
        ]
        if table == "estabelecimento":
            df = pre_cleaning_to_utf8(df)
            df = if_column_exist_delete(df=df, col_list=list_columns_to_delete)
            df = check_and_create_column(df=df, col_name="NAT_JUR")
        else:
            df = df[list_columns_to_keep]

        # salvar de novo
        df.to_csv(output_file, sep=",", na_rep="", index=False, encoding="utf-8")

        log(
            f"The file {file} was converted to csv and saved at {output_path + file + '.csv'}"
        )

    return path + table
