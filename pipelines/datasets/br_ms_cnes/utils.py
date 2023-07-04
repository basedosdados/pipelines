# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

from ftplib import FTP


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
