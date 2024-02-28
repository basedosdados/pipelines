# -*- coding: utf-8 -*-
"""
General purpose functions for the br_ms_cnes project
"""

import asyncio
from datetime import datetime
from ftplib import FTP
import pyarrow as pa
from pathlib import Path
import pyarrow.parquet as pq
from dbfread import DBF
import struct
import aioftp
import pandas as pd
import os
from typing import List, Dict, Tuple
from pipelines.utils.crawler_datasus.constants import constants as datasus_constants
from pipelines.utils.utils import log


#-----------------------------
#Utils to handle dbf and dbf
#https://github.com/AlertaDengue/PySUS/blob/main/pysus/data/__init__.py
def stream_dbf(dbf, chunk_size=400000):
    """Fetches records in parquet chunks to preserve memory"""
    data = []
    i = 0
    for records in dbf:
        data.append(records)
        i += 1
        if i == chunk_size:
            yield data
            data = []
            i = 0
    else:
        yield data

def decode_column(value):
    """
    Decodes binary data to str
    """
    if isinstance(value, bytes):
        return value.decode(encoding="iso-8859-1").replace("\x00", "")

    if isinstance(value, str):
        return str(value).replace("\x00", "")
    return value

def dbf_to_parquet(dbf: str) -> str:
    """
    Parses DBF file into parquet to preserve memory
    """


    path = Path(dbf)
    if path.suffix.lower() != ".dbf":
        raise ValueError(f"Not a DBF file: {path}")

    #TODO: adaptar no flow do cnes para trocar sufixos de arquivos
    parquet = path.with_suffix(".parquet")
    print(parquet)
    approx_final_size = (
        os.path.getsize(path)/1000
    )  # TODO: possibly use it to decide upon number of chunks
    print(approx_final_size)


    #cira diretório de output
    dbf = dbf.replace("input", "output")
    path_parts = dbf.split("/")[:-1]
    output_path = os.path.join("/", *path_parts)
    os.makedirs(output_path, exist_ok=True)

    try:
        #chunk_size = 30_000
        #chunk_size = 100_000
        for chunk in stream_dbf(
            #dbf5 no lugar de DBF
            DBF(path, encoding="iso-8859-1", raw=True)
        ):
            chunk_df = pd.DataFrame(chunk)
            #TODO: INSERT SCHEMA FOR CNES TABLES
            #NOTE: applymap is deprecated on pandas=2.1 onwards; basedosdados uses 2.0.1
            table = pa.Table.from_pandas(chunk_df.applymap(decode_column))
            #TypeError: __init__() got multiple values for argument 'schema' when set schema
            pq.write_to_dataset(table, root_path=str(output_path))
    except struct.error as err:

        Path(path).unlink()
        raise err


    path.unlink()

    return str(parquet)

def list_datasus_dbc_files(
    datasus_database: str,
    datasus_database_table: str = None,
) -> list:

    """
    Enters FTP server and lists all DBCs files found for a
    a given datasus_database.

    NOTE: It currently supports CNES and SIA databases groups;
    CIHA, SIH, SIM, SINAN, SINASC and PNI are not implemented yet.
    """
    available_dbs = list()
    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()

    # se datasus_database tiver diferentes URL em virtude de
    # variados anos  de inicio mudar abaixo
    cnes_path = [
        "dissemin/publicos/CNES/200508_/Dados",
        "dissemin/publicos/SIASUS/200801_/Dados"
        ]

    for path in cnes_path:
        try:
            # Adicionar aqui eventuais caminhos de databse
            if datasus_database == "CNES":
                if not datasus_database_table:
                    raise ValueError("No group assigned to CNES_group")
                print(f"{path}/{datasus_database_table}/*.DBC")
                available_dbs.extend(ftp.nlst(f"{path}/{datasus_database_table}/*.DBC"))

            if datasus_database == "SIA":
                print("SIA database")
                if not datasus_database_table:
                    raise ValueError("No group assigned to SIA_group")
                available_dbs.extend(ftp.nlst(f"{path}/{datasus_database_table}*.DBC"))
        except Exception as e:
            raise e
    ftp.close()
    return available_dbs

async def download_chunks(files: List[str], output_dir: str, chunk_size: int, max_parallel: int):
    """This function is used to sequentially control the number of concurrent downloads to avoid
    #OSError: [Errno 24] Too many open files

    Args:
        files (List[str]): list of files to download
        output_dir (str): savepath
        chunk_size (int): size of the chunk
        max_parallel (int): number of concurrent downloads
    """

    for i in range(0, len(files), chunk_size):
        chunk = files[i:i + chunk_size]
        await download_files(files=chunk, output_dir=output_dir, max_parallel=max_parallel)


async def download_files(files: list, output_dir: str, max_parallel: int)-> None:
    """Download files asynchronously and save them in the given output directory

    Args:
        files (list): List of files to download
        output_dir (str): output directory
        max_parallel (int): number of max parallel downloads
    """

    semaphore = asyncio.Semaphore(max_parallel)
    async with semaphore:
        tasks = [
            download_file(file, f"{output_dir}/{year_month_sigla_uf_parser(file=file)}")
            for file in files
        ]
        await asyncio.gather(*tasks)


async def download_file(file: str, output_path: str)-> None:
    """Create connection with DATASUS FTP server using AIOFTP which supports
    Asynchronous download within FTP servers

    Args:
        file (str): A file path to download
        output_path (str): output path to save
    """


    try:
        log(f"------- Downloading {file} ")
        async with aioftp.Client.context(
            host="ftp.datasus.gov.br",
            parse_list_line_custom=line_file_parser,
            # max wait time for data to be received from the server
            socket_timeout=30,
        ) as client:
            await client.download(file, output_path)
    except aioftp.StatusCodeError as e:
        log(e.expected_codes, e.received_codes, e.info)


def line_file_parser(file_line)-> Tuple[List, Dict]:
    """This function is used to parse the file line from the FTP server

    Returns:
        Tupple: A tuple containing a list and a dictionary
    """

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
        #file = file[-10:-4]
        file = file.split('/')[-1]


        # parse and build year
        year = "20" + file[4:6]

        # parse and build month
        month = int(file[6:8])

        # parse and build state
        sigla_uf = file[2:4]

    except IndexError:
        raise ValueError("Unable to parse month and year from file")

    return f"ano={year}/mes={month}/sigla_uf={sigla_uf}"


def pre_cleaning_to_utf8(df: pd.DataFrame):
    """This function is used to remove latin1 characters

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
    """Apply custom post-process to the profissional table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """

    df = df[datasus_constants.COLUMNS_TO_KEEP.value["PF"]]
    return df


def post_process_leito(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the leito table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["LT"]]
    return df


def post_process_equipamento(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the equipamento table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EP"]]
    return df


def post_process_equipe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the equipe table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    standardize_colums = {
        "IDEQUIPE": "ID_EQUIPE",
        "AREA_EQP": "ID_AREA",
    }

    df.rename(columns=standardize_colums, inplace=True)

    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EQ"]]

    return df


def post_process_estabelecimento_ensino(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the estabelecimento_ensino table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EE"]]
    return df


def post_process_dados_complementares(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the dados_complementares table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["DC"]]

    return df


def post_process_estabelecimento_filantropico(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the estabelecimento_filantropico table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["EF"]]
    return df


def post_process_gestao_metas(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the gestao_metas table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["GM"]]
    return df


def post_process_habilitacao(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the habilitacao table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = check_and_create_column(df=df, col_name="NAT_JUR")
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["HB"]]
    return df


def post_process_incentivos(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the incentivos table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["IN"]]
    return df


def post_process_regra_contratual(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the regra_contratual table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["RC"]]
    return df


def post_process_servico_especializado(df: pd.DataFrame) -> pd.DataFrame:
    """Apply custom post-process to the servico_especializado table

    Args:
        df (pd.DataFrame): a pd.DataFrame

    Returns:
        pd.DataFrame: a pd.DataFrame
    """
    df = df[datasus_constants.COLUMNS_TO_KEEP.value["SR"]]
    return df
