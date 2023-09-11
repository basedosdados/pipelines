# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

from ftplib import FTP
import pandas as pd
import basedosdados as bd
from datetime import datetime
from pipelines.utils.utils import log


def list_all_cnes_dbc_files(
    database: str,
    CNES_group: str = None,
) -> list:
    # todo: insert fcodeco link reference
    """
    Enters FTP server and lists all DBCs files found for a
    CNES-ST database group.
    """
    available_dbs = list()
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    cnes_path = ["dissemin/publicos/CNES/200508_/Dados"]

    for path in cnes_path:
        try:
            # CNES
            if database == "CNES":
                if not CNES_group:
                    raise ValueError("No group assigned to CNES_group")
                available_dbs.extend(ftp.nlst(f"{path}/{CNES_group}/*.DBC"))
        except Exception as e:
            raise e
    ftp.close()
    return available_dbs


def year_month_sigla_uf_parser(file: str) -> str:
    """receives a DATASUS CNES ST file and parse year, month and sigla_uf
    to create proper partitions

    Returns:
        (str):
    """
    # todo: implement a check to make sure the result is right
    # parse file name last 8 digits before .DBC
    file = file[-10:-4]

    # parse and build year
    year = "20" + file[2:4]

    # parse and build month
    month = int(file[4:6])

    # parse and build state
    sigla_uf = file[:2]

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


def extract_last_date(dataset_id, table_id, billing_project_id: str):
    """
    Extracts the last update date of a given dataset table.

    Args:
        dataset_id (str): The ID of the dataset.
        table_id (str): The ID of the table.
        billing_project_id (str): The billing project ID.

    Returns:
        str: The last update date in the format 'yyyy-mm' or 'yyyy-mm-dd'.

    Raises:
        Exception: If an error occurs while extracting the last update date.
    """

    query_bd = f"""
    SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) as max_date
    FROM
    `{billing_project_id}.{dataset_id}.{table_id}`
    """

    t = bd.read_sql(
        query=query_bd,
        billing_project_id=billing_project_id,
        from_file=True,
    )
    year_month = t["max_date"][0]

    log(f"O Ano/Mês mais recente da tabela é: {year_month}")

    return year_month
