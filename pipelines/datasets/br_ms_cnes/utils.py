# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

from ftplib import FTP
import pandas as pd


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
