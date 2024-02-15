# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

import asyncio
from datetime import datetime
from ftplib import FTP

import aioftp
import pandas as pd

from pipelines.utils.crawler_datasus.constants import constants as datasus_constants


def list_all_cnes_dbc_files(
    datasus_database: str,
    datasus_database_table: str = None,
) -> list:
    # todo: insert fcodeco link reference
    """
    Enters FTP server and lists all DBCs files found for a
    a given datasus_database.
    """
    available_dbs = list()
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    # se datasus_database tiver diferentes URL em virtude de
    # variados anos  de inicio mudar abaixo
    cnes_path = ["dissemin/publicos/CNES/200508_/Dados"]

    for path in cnes_path:
        try:
            # Adicionar aqui eventuais caminhos de databse
            if datasus_database == "CNES":
                if not datasus_database_table:
                    raise ValueError("No group assigned to CNES_group")
                available_dbs.extend(ftp.nlst(f"{path}/{datasus_database_table}/*.DBC"))
        except Exception as e:
            raise e
    ftp.close()
    return available_dbs


async def download_files(files: list, output_dir: str):
    tasks = [
        download_file(file, f"{output_dir}/{year_month_sigla_uf_parser(file=file)}")
        for file in files
    ]
    await asyncio.gather(*tasks)


async def download_file(file: str, output_path: str):
    try:
        async with aioftp.Client.context(
            host="ftp.datasus.gov.br",
            parse_list_line_custom=line_file_parser,
            # max wait time for data to be received from the server, if it fails trigger a
            socket_timeout=30,
        ) as client:
            await client.download(file, output_path)
    except aioftp.StatusCodeError as e:
        print(e.expected_codes, e.received_codes, e.info)


def line_file_parser(file_line):
    line = file_line.decode("utf-8")
    info = {}
    if "<DIR>" in line:
        date, time, _, *name = line.strip().split()
        info["size"] = 0
        info["type"] = "dir"
        name = " ".join(name)
    else:
        date, time, size, name = line.strip().split()
        info["size"] = size
        info["type"] = "file"

    modify = datetime.strptime(" ".join([date, time]), "%m-%d-%y %I:%M%p")
    info["modify"] = modify.strftime("%m/%d/%Y %I:%M%p")

    return name, info


def year_month_sigla_uf_parser(file: str) -> str:
    """receives a DATASUS CNES ST file and parse year, month and sigla_uf
    to create proper partitions

    Returns:
        (str): partition string
    """
    try:
        # parse file name last 8 digits before .DBC
        file = file[-10:-4]

        # parse and build year
        year = "20" + file[2:4]

        # parse and build month
        month = int(file[4:6])

        # parse and build state
        sigla_uf = file[:2]

    except IndexError:
        raise ValueError("Unable to parse month and year from file")

    return f"ano={year}/mes={month}/sigla_uf={sigla_uf}"


def pre_cleaning_to_utf8(df: pd.DataFrame):
    """This function is used to pre-clean the data to convert its encoding
    from latin1 to utf-8

    Args:
        df (pd.Dataframe): a df
    """

    colums_to_clean = [
        "REGSAUDE",
        "MICR_REG",
        "DISTRSAN",
        "DISTRADM",
        "PF_PJ",
        "NIV_DEP",
        "COD_IR",
        "ESFERA_A",
        "ATIVIDAD",
        "TP_UNID",
        "TURNO_AT",
        "NIV_HIER",
        "TP_PREST",
        "ORGEXPED",
        "AV_ACRED",
        "AV_PNASS",
    ]

    for column in colums_to_clean:
        df[column] = df[column].str.replace(",", "")
        df[column] = df[column].str.replace("¿", "")
        df[column] = df[column].str.rstrip("ª")
        df[column] = df[column].str.rstrip("º")
        df[column] = df[column].str.lstrip("0")

    return df


def check_and_create_column(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Check if a column exists in a Pandas DataFrame. If it doesn't, create a new column with the given name
    and fill it with NaN values. If it does exist, do nothing.

    Parameters:
    df (Pandas DataFrame): The DataFrame to check.
    col_name (str): The name of the column to check for or create.

    Returns:
    Pandas DataFrame: The modified DataFrame.
    """
    if col_name not in df.columns:
        df[col_name] = ""
    return df


def if_column_exist_delete(df: pd.DataFrame, col_list: str):
    """Some columns present in cnes st files from 2019 onwards only contain blank values and are not
    registred in any metadata file
    https://cnes.datasus.gov.br/pages/downloads/documentacao.jsp
    It checkes if one of those columns are present in the dataframe and deletes it

    Args:
        df (pd.DataFrame): a cnes st group dataframe
        col_list (str): list of cnes st group dataframe columns to delete

    Returns:
        df (pd.DataFrame): withut the columns in col_list
    """

    for col in col_list:
        if col in df.columns:
            del df[col]

    return df


def post_process_estabelecimento(df: pd.DataFrame) -> pd.DataFrame:
    list_columns_to_delete = [
        "AP01CV07",
        "AP02CV07",
        "AP03CV07",
        "AP04CV07",
        "AP05CV07",
        "AP06CV07",
        "AP07CV07",
    ]

    df = pre_cleaning_to_utf8(df)
    df = if_column_exist_delete(df=df, col_list=list_columns_to_delete)
    df = check_and_create_column(df=df, col_name="NAT_JUR")

    return df


def post_process_profissional(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["PF"]]
    return df


def post_process_leito(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["LT"]]
    return df


def post_process_equipamento(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EP"]]
    return df


def post_process_equipe(df: pd.DataFrame) -> pd.DataFrame:
    standardize_colums = {
        "IDEQUIPE": "ID_EQUIPE",
        "AREA_EQP": "ID_AREA",
    }

    df.rename(columns=standardize_colums, inplace=True)

    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EQ"]]

    return df


def post_process_estabelecimento_ensino(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EE"]]
    return df


def post_process_dados_complementares(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["DC"]]

    return df


def post_process_estabelecimento_filantropico(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EF"]]
    return df


def post_process_gestao_metas(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["GM"]]
    return df


def post_process_habilitacao(df: pd.DataFrame) -> pd.DataFrame:
    df = check_and_create_column(df=df, col_name="NAT_JUR")
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["HB"]]
    return df


def post_process_incentivos(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["IN"]]
    return df


def post_process_regra_contratual(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["RC"]]
    return df


def post_process_servico_especializado(df: pd.DataFrame) -> pd.DataFrame:
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["SR"]]
    return df
