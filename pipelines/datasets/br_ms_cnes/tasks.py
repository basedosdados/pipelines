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
    extract_last_date,
)


@task
def check_files_to_parse(
    dataset_id: str,
    table_id: str,
    billing_project_id,
    cnes_database: str,
    cnes_group_file: str,
) -> list[str]:
    log(f"extracting last date from bq for {dataset_id}.{table_id}")
    # 1. extrair data mais atual do bq
    last_date = extract_last_date(
        dataset_id=dataset_id,
        table_id=table_id,
        billing_project_id=billing_project_id,
    )
    log("building next year/month to parse")
    # 2. adicionar mais um no mes ou transformar pra 1 se for 12
    # eg. last_date = 20234
    month = last_date[4:]
    month = int(month)
    if month <= 11:
        month = month + 1
    else:
        month = 1
    # 3. buildar no formato do ftp YYMM
    year = last_date[2:]

    if month <= 9:
        month = "0" + str(month)

    year_month_to_parse = year + month
    log(f"year_month_to_parse (YYMM) is {year_month_to_parse}")
    # 4. ver se o arquivo existe
    available_dbs = list_all_cnes_dbc_files(
        database=cnes_database, CNES_group=cnes_group_file
    )

    list_files = []

    # filtra pra ver se acha e retorna a lista
    for file in available_dbs:
        if file[-8:-4] == year_month_to_parse:
            list_files.append(file)
    # final
    log(f"the following files were selected fom DATASUS FTP: {list_files}")

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

    log(f"wrangling {table} data")

    for file in file_list:
        # build partition dirs

        year_month_sigla_uf = year_month_sigla_uf_parser(file=file)

        log(f"the file {file} was parsed to {year_month_sigla_uf}")
        input_path = path + table + "/" + year_month_sigla_uf

        os.system(f"mkdir -p {input_path}")
        os.system(f"find {input_path} -type f -delete")

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

        log(f"The file {dbc_file} is in {input_path}")

    return dbc_files_path_list


# task to convert dbc to csv and save to a partitioned dir
@task
def read_dbc_save_csv(file_list: list, path: str, table: str) -> str:
    """
    Convert dbc to csv
    """
    log(f"wrangling {table} data")
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
        # delete dbc file
        os.system(f"rm {file}")

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

        if table == "estabelecimento":
            df = pre_cleaning_to_utf8(df)
            df = if_column_exist_delete(df=df, col_list=list_columns_to_delete)
            df = check_and_create_column(df=df, col_name="NAT_JUR")

        elif table == "profissional":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["PF"]]

        elif table == "leito":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["LT"]]

        elif table == "equipamento":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["EP"]]

        elif table == "equipe":
            # equipe
            # the EQ table has different names for same variables across the years
            # this is a workaround to standardize the names
            standardize_colums = {
                "IDEQUIPE": "ID_EQUIPE",
                "AREA_EQP": "ID_AREA",
            }

            df.rename(columns=standardize_colums, inplace=True)

            df = df[cnes_constants.COLUMNS_TO_KEEP.value["EQ"]]

        elif table == "estabelecimento_ensino":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["EE"]]

        elif table == "dados_complementares":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["DC"]]

        elif table == "estabelecimento_filantropico":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["EF"]]

        elif table == "gestao_metas":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["GM"]]

        elif table == "habilitacao":
            df = check_and_create_column(df=df, col_name="NAT_JUR")
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["HB"]]

        elif table == "incentivos":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["IN"]]

        elif table == "regra_contratual":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["RC"]]

        elif table == "servico_especializado":
            df = df[cnes_constants.COLUMNS_TO_KEEP.value["SR"]]

        # salvar de novo
        df.to_csv(output_file, sep=",", na_rep="", index=False, encoding="utf-8")

        log(
            f"The file {file} was converted to csv and saved at {output_path + file + '.csv'}"
        )

    return path + table


@task
def is_empty(lista):
    if len(lista) == 0:
        return True
    else:
        return False
