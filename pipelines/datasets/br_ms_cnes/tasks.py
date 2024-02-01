# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""


import os
import subprocess
from datetime import timedelta
from glob import glob

import pandas as pd

# import rpy2.robjects.packages as rpackages
import wget
from datasus import decompress

# from pyreaddbc import read_dbc as dbcreader
from prefect import task
from simpledbf import Dbf5
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.datasets.br_ms_cnes.constants import constants as cnes_constants
from pipelines.datasets.br_ms_cnes.utils import (
    check_and_create_column,
    if_column_exist_delete,
    list_all_cnes_dbc_files,
    pre_cleaning_to_utf8,
    year_month_sigla_uf_parser,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date
from pipelines.utils.utils import log

# from rpy2.robjects.packages import importr


@task
def check_files_to_parse(
    dataset_id: str,
    table_id: str,
    cnes_database: str,
    cnes_group_file: str,
) -> list[str]:
    log(f"extracting last date from api for {dataset_id}.{table_id}")
    # 1. extrair data mais atual da api
    last_date = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="%Y-%m",
    )
    log("building next year/month to parse")
    # 2. adicionar mais um no mes ou transformar pra 1 se for 12
    # para ver se o mês seguinte já está disponível no FTP
    # eg. last_date = 2023-04-01

    year = str(last_date.year)
    month = last_date.month

    # download files to compare
    year = "2023"
    month = 7

    if month <= 11:
        month = month + 1
    else:
        month = 1
    # 3. buildar no formato do ftp YYMM
    year = year[2:4]

    if month <= 9:
        month = "0" + str(month)
    else:
        month = str(month)

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

    for file in tqdm(file_list):
        # build partition dirs

        try:

            year_month_sigla_uf = year_month_sigla_uf_parser(file=file)

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

            dbc_files_path_list.append(complete_path)

        except Exception as e:
            log(f"An error occurred while downloading {file} from DATASUS FTP: {e}")
            raise e

    return dbc_files_path_list


@task
def decompress_dbc(file_list: list) -> None:

    log(f"==== curent env {os. getcwd()}")

    # ? Tive que dar um grant pra exucutar bash na pipeline
    # ? chmod +x adapted_convert2dbf.sh
    subprocess.run(
        ["/home/gabri/pipelines/pipelines/datasets/br_ms_cnes/adapted_convert2dbf.sh"]
        + file_list
    )


# task to convert dbc to csv and save to a partitioned dir
@task
def decompress_dbf(file_list: list, path: str, table: str) -> str:
    """
    Convert dbc to csv
    """
    log(f"wrangling {table} data")

    # list files
    for file in tqdm(file_list):
        log(f"---------- {file}")
        dbf_file = ".".join([file.split(".")[0], "dbf"])

        log(f"-------- reading .dbf {dbf_file}")
        dbf = Dbf5(dbf_file, codec="iso-8859-1")

        year_month_sigla_uf = year_month_sigla_uf_parser(file=file)

        log(f"year_month_sigla_uf of {file} parsed")
        output_path = path + table + "/" + year_month_sigla_uf

        os.system(f"mkdir -p {output_path}")
        log(f"created output partition dir {path + table + '/'+ year_month_sigla_uf}")

        output_file = output_path + "/" + table + ".csv"
        log(f"{file} 1 saved")

        # filesd='.'.join([dbf_file.split('.')[0], "csv"])
        log(f"-------- saving to .csv {output_file}")
        dbf.to_csv(output_file)

        # delete dbc file
        folder_path = file.split(".")[0]
        files_to_remove = glob(os.path.join(folder_path, "*.dbc")) + glob(
            os.path.join(folder_path, "*.dbf")
        )
        # Remove each file
        for file in files_to_remove:
            os.system(f"rm {file}")

        # ler df
        log("file 2 being read")
        df = pd.read_csv(output_file, dtype=str, encoding="iso-8859-1")

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
