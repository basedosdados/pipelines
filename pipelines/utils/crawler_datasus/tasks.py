# -*- coding: utf-8 -*-
"""
Tasks for br_ms_cnes
"""


import asyncio
import os
from datetime import timedelta

import aioftp
import pandas as pd
from prefect import task
from prefect.tasks.shell import ShellTask
from simpledbf import Dbf5
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.utils.crawler_datasus.constants import constants as datasus_constants
from pipelines.utils.crawler_datasus.utils import (
    download_chunks,
    list_datasus_dbc_files,
    year_month_sigla_uf_parser,
    dbf_to_parquet,
    post_process_dados_complementares,
    post_process_equipamento,
    post_process_equipe,
    post_process_estabelecimento,
    post_process_estabelecimento_ensino,
    post_process_estabelecimento_filantropico,
    post_process_gestao_metas,
    post_process_habilitacao,
    post_process_incentivos,
    post_process_leito,
    post_process_profissional,
    post_process_regra_contratual,
    post_process_servico_especializado,

)
import basedosdados as bd
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.utils import log



@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def check_files_to_parse(
    dataset_id: str,
    table_id: str,
    year_month_to_extract: str,
) -> list[str]:
    log(f"------- Extracting last date from api for {dataset_id}.{table_id}")
    # 1. extrair data mais atual da api
    backend = bd.Backend(graphql_url=get_url("prod"))
    last_date = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="%Y-%m",
        backend=backend
    )

    log("------- Building next year/month to parse")

    year = str(last_date.year)
    month = last_date.month


    if month <= 11:
        month = month + 1
    else:
        month = 1
        year = int(year) + 1
        year = str(year)

    year = year[2:4]

    if month <= 9:
        month = "0" + str(month)
    else:
        month = str(month)

    year_month_to_parse = year + month
    log(f"------- The year_month_to_parse (YYMM) is {year_month_to_parse}")

    #Convert datasus database names to Basedosdados database names
    datasus_database = datasus_constants.DATASUS_DATABASE.value[dataset_id]
    datasus_database_table = datasus_constants.DATASUS_DATABASE_TABLE.value[table_id]

    available_dbs = list_datasus_dbc_files(
        datasus_database=datasus_database, datasus_database_table=datasus_database_table
    )

    #
    if len(year_month_to_extract) == 0:
        list_files = [file for file in available_dbs if file.split('/')[-1][4:8] == year_month_to_parse]
    else:
        list_files = [file for file in available_dbs if file.split('/')[-1][4:8] in year_month_to_extract ]




    log(f"------- The following files were selected fom DATASUS FTP: {list_files}")

    return list_files


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def access_ftp_download_files_async(
    file_list: list, dataset_id: str, table_id: str, max_parallel: int = 20, chunk_size: int = 15
) -> list[str]:
    """This task access Datasus FTP server and download a list of files asynchronously

    Args:
        file_list (list): datasus FTP file list
        dataset_id (str): Basedosdados dataset id
        table_id (str): Basedosdados table id
        max_parallel (int, optional): Max concurrent downloads. Defaults to 10.
        chunk_size (int, optional): Sequentially define max concurrent downloads. Defaults to 20.

    Returns:
        list[str]: Path to downloaded files
    """

    input_path = os.path.join("/tmp", dataset_id, "input", table_id)
    log(f"------ created input dir {input_path}")
    os.makedirs(input_path, exist_ok=True)

    # https://github.com/AlertaDengue/PySUS/blob/main/pysus/ftp/__init__.py#L156
    log(f"------ dowloading {table_id} files from DATASUS FTP")



    asyncio.run(
        download_chunks(
            files=file_list,
            output_dir=input_path,
            max_parallel=max_parallel,
            chunk_size=chunk_size,
        )
    )

    dbc_files_path_list = [
        os.path.join(
            input_path, year_month_sigla_uf_parser(file=file), file.split("/")[-1]
        )
        for file in tqdm(file_list)
    ]

    return dbc_files_path_list


@task
def decompress_dbc(file_list: list, dataset_id: str) -> None:
    """
    Convert dbc to dbf format
    """

    # ShellTask to create the blast-dbf directory
    create_dir_task = ShellTask(
        name="Create blast-dbf Directory",
        command=f"mkdir -p /tmp/{dataset_id}/blast-dbf",
        return_all=True,
        log_stderr=True,
    )

    # ShellTask to clone the blast-dbf repository
    clone_repo_task = ShellTask(
        name="Clone blast-dbf Repository",
        command=f"git clone https://github.com/eaglebh/blast-dbf /tmp/{dataset_id}/blast-dbf",
        return_all=True,
        log_stderr=True,
    )

    # ShellTask to build the blast-dbf repository
    compile_blast_dbf = ShellTask(
        name="Build blast-dbf",
        command=f"cd /tmp/{dataset_id}/blast-dbf && make",
        return_all=True,
        log_stderr=True,
    )

    create_dir_task.run()
    log("------ Cloning blast-dbf repository")
    clone_repo_task.run()
    log("------ Compiling blast-dbf repository")
    compile_blast_dbf.run()

    for file in tqdm(file_list):
        # Check if the file has a .dbc extension
        if file.endswith(".dbc"):
            # Execute blast-dbf on the file
            decompress_task = ShellTask(
                name=f"Decompress {file}",
                command=f"/tmp/{dataset_id}/blast-dbf/blast-dbf {file} {file.replace('.dbc', '.dbf')}",
                return_all=True,
                log_stderr=True,
            )
            log(f"------ Decompressing {file.split('/')[-1]} file")
            decompress_task.run()
            log(f"------ Removing {file.split('/')[-1]} file")
            os.system(f"rm -f {file}")

        else:
            log(f"Skipping non-DBC file: {file}")

    log("------ Removing dbc files")

    log(f"------ Removing /tmp/{dataset_id}/blast-dbf")
    os.system(f'rm -rf /tmp/{dataset_id}/blast-dbf')





@task
def decompress_dbf(file_list: list, table_id: str) -> str:
    """
    Convert dbc to csv
    """
    log(f"--------- Decompressing {table_id} .DBF files")

    csv_file_list = list()

    dbf_file_list = [file.replace(".dbc", ".dbf") for file in file_list]

    for file in tqdm(dbf_file_list):

        # replace .csv with .dbf
        log(f"-------- Reading {file}")
        dbf = Dbf5(file, codec="iso-8859-1")

        # remove cnes default file name
        file = file.replace("input", "output")
        path_parts = file.split("/")[:-1]
        output_path = os.path.join("/", *path_parts)
        os.makedirs(output_path, exist_ok=True)

        output_file = os.path.join(output_path, f"{table_id}.csv")
        log(f"-------- Saving {file.split('/')[-1]} to {table_id}.csv ")

        dbf.to_csv(output_file)
        csv_file_list.append(output_file)

        if file.endswith(".dbf"):
            file = file.replace("output", "input")
            log(f"------ Removing {file.split('/')[-1]}")
            os.system(f"rm -f {file}")

    return csv_file_list






@task
def pre_process_files(file_list: list, dataset_id: str, table_id: str) -> str:

    log("Post-processing CSV files")

    # todo: colocar em constants
    post_process_functions = {
        "estabelecimento": post_process_estabelecimento,
        "profissional": post_process_profissional,
        "equipamento": post_process_equipamento,
        "dados_complementares": post_process_dados_complementares,
        "equipe": post_process_equipe,
        "estabelecimento_ensino": post_process_estabelecimento_ensino,
        "leito": post_process_leito,
        "estabelecimento_filantropico": post_process_estabelecimento_filantropico,
        "gestao_metas": post_process_gestao_metas,
        "habilitacao": post_process_habilitacao,
        "incentivos": post_process_incentivos,
        "regra_contratual": post_process_regra_contratual,
        "servico_especializado": post_process_servico_especializado,
    }

    try:
        post_process_function = post_process_functions.get(table_id)

    except: #Exception as e:
        log(
            f"The error {e} occurred. It probably indicates that no post-processing function was found for table_id: {table_id}"
        )

    for file in tqdm(file_list):

        log(f"-------- wrangling {file.split('/')[-1]} and saving to parquet")
        concatenated_df = pd.DataFrame()

        for chunk_df in pd.read_csv(
            file, dtype=str, encoding="iso-8859-1", chunksize=50000
        ):
            concatenated_df = pd.concat([concatenated_df, chunk_df])

        processed_df = post_process_function(concatenated_df)

        processed_df = processed_df.astype(str)
        output_path = file.replace(".csv", ".parquet")
        processed_df.to_parquet(output_path, index=None, compression='gzip')

        os.system(f"rm -f {file}")

    log("Post-processing complete")

    path_parts = file.split("/")[:5]
    output_path = os.path.join("/", *path_parts)

    return output_path




@task
def is_empty(lista):
    if len(lista) == 0:
        return True
    else:
        return False




@task
def read_dbf_save_parquet_chunks(file_list: list, table_id: str, dataset_id:str= "br_ms_sia") -> str:
    """
    Convert dbc to parquet
    """
    log(f"--------- Decompressing {table_id} .DBF files")


    dbf_file_list = [file.replace(".dbc", ".dbf") for file in file_list]
    _counter = 0
    log(f'----coutner {_counter}')
    for file in tqdm(dbf_file_list):


        log(f"-------- Reading {file}")
        result_path = dbf_to_parquet(dbf=file, table_id=table_id, counter=_counter)
        _counter += 1


    return f'/tmp/{dataset_id}/output/{table_id}'
