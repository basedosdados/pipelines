"""
Tasks for br_cvm_fi
"""

import csv
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import rpy2.robjects.packages as rpackages
from bs4 import BeautifulSoup
from prefect import task
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.datasets.br_cvm_fi.constants import constants as cvm_constants
from pipelines.datasets.br_cvm_fi.utils import (
    check_and_create_column,
    create_year_month_columns,
    format_cnpj_columns,
    limpar_string,
    obter_anos_meses,
)
from pipelines.utils.utils import log, to_partitions


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def extract_links_and_dates(url: str) -> tuple[pd.DataFrame, str]:
    """
    Extracts all file names and their respective last update dates in a pandas dataframe.

    Parameters:

    url: str
        The base URL from which files will be downloaded.

    Returns:
        tuple[pd.DataFrame, str]: The files and dates data frame, along with the max date (last update)
    """

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Encontra todos os links dentro do HTML
    filename_pattern = r"^[\w_]+\.[\w]{3,}$"
    date_pattern = r"([\d\w]{2,}-[\d\w]{2,}-[\d\w]{2,})\s?\d{2}:\d{2}"
    links = soup.find_all(
        "a", string=lambda text: re.findall(filename_pattern, text)
    )
    links_filtered = []
    dates_update = []

    if url in cvm_constants.CSV_LIST.value:
        for link in links:
            if link.has_attr("href") and link["href"].endswith(".csv"):
                log(link)
                links_filtered.append(link["href"])
                data = str(link.next_element.next_element.text)
                log(data.strip())
                if data and re.match(date_pattern, data.strip()):
                    dates_update.append(
                        re.match(date_pattern, data.strip()).group(0)
                    )
    else:
        for link in links:
            if link.has_attr("href") and link["href"].endswith(".zip"):
                log(link)
                links_filtered.append(link["href"])
                data = str(link.next_element.next_element.text)
                log(data.strip())
                if data and re.match(date_pattern, data.strip()):
                    dates_update.append(
                        re.match(date_pattern, data.strip()).group(0)
                    )

    dados = {
        "arquivo": links_filtered,
        "ultima_atualizacao": dates_update,
        "data_hoje": datetime.now().strftime("%Y-%m-%d"),
    }
    df = pd.DataFrame(dados)
    df.ultima_atualizacao = df.ultima_atualizacao.apply(
        lambda x: datetime.strptime(x, "%d-%b-%Y %H:%M").strftime("%Y-%m-%d")
    )

    data_maxima = df["ultima_atualizacao"].max()
    return df, data_maxima


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def generate_links_to_download(
    df: pd.DataFrame, max_date: datetime
) -> list[str]:
    """
    Checks for outdated tables.
    """
    lists = df.query(f"ultima_atualizacao == '{max_date}'").arquivo.to_list()
    log(f"The following files will be downloaded: {lists}")
    return lists


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_unzip_csv(
    url: str,
    table_id: str,
    files: list | str,
    chunk_size: int = 128,
) -> str:
    """
    Downloads and unzips a .csv file from a given list of files and saves it to a local directory.

    Parameters:

    url: str
        The base URL from which files will be downloaded.
    table_id: str
        The BigQuery Table ID.
    files: list or str
        The .zip file names or a single .zip file name to extract the csv file from.
    chunk_size: int, optional
        The size of each chunk to download in bytes. Default is 128 bytes.

    Returns:
    str
        The path to the downloaded file(s) directory.
    """

    input_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }

    if isinstance(files, str):
        files = [files]
    elif not isinstance(files, list):
        raise ValueError("O argumento 'files' possui um tipo inadequado.")

    for file in files:
        log(f"Baixando o arquivo {file}")
        download_url = f"{url}{file}"
        save_path = input_dir / str(file)

        r = requests.get(
            download_url, headers=request_headers, stream=True, timeout=50
        )
        with open(save_path, "wb") as fd:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                fd.write(chunk)

        try:
            with zipfile.ZipFile(save_path) as z:
                z.extractall(input_dir)
            log("Dados extraídos com sucesso!")

        except zipfile.BadZipFile:
            log(f"O arquivo {file} não é um arquivo ZIP válido.")

    return input_dir


@task
def download_csv_cvm(
    url: str, table_id: str, files: list | str, chunk_size: int = 128
) -> str:
    input_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "input"
    input_dir.mkdir(parents=True, exist_ok=True)

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    if isinstance(files, str):
        files = [files]
    elif not isinstance(files, list):
        raise ValueError("O argumento 'files' possui um tipo inadequado.")
    for file in files:
        log(f"Baixando o arquivo {file}")
        download_url = f"{url}{file}"
        save_path = input_dir / str(file)

        r = requests.get(
            download_url, headers=request_headers, stream=True, timeout=10
        )
        with open(save_path, "wb") as fd:
            for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
                fd.write(chunk)

    return input_dir


@task
def is_empty(lista):
    return len(lista) == 0


@task
def clean_data_and_make_partitions(
    input_dir: str | Path, table_id: str
) -> str:
    """
    Cleans CVM Informe Diário data based on the specified schema at constants.py file and makes partitions.
    """
    files = Path(input_dir).glob("*.csv")

    for file in files:
        df = pd.read_csv(file, sep=";")
        log(f"File {file} read.")
        # Rename columns to fit standard
        df_final = df.rename(
            columns=cvm_constants.RENAME_MAPPING_INFORME.value
        )
        # Create year and month partiton columns
        df_final = create_year_month_columns(df_final)
        # Ensure final columns
        df_final = check_and_create_column(
            df_final, colunas_totais=cvm_constants.FINAL_COLS_INFORME.value
        )
        df_final = format_cnpj_columns(df_final, ["cnpj"])
        df_final = df_final[cvm_constants.FINAL_COLS_INFORME.value]
        log(f"File {file.name} cleaned.")

        output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        to_partitions(
            df_final,
            partition_columns=["ano", "mes"],
            savepath=output_dir,
        )
        log("Partition created.")

    return output_dir


@task
def clean_data_make_partitions_cda(input_dir: str | Path, table_id: str):
    anos_meses = obter_anos_meses(input_dir)
    for i in anos_meses:
        df_concat = pd.DataFrame()
        arquivos = Path(input_dir).glob("*.csv")
        padrao = f"cda_fi_BLC_[1-8]_{i}.csv"
        arquivos_filtrados = [
            arq for arq in arquivos if re.match(padrao, arq.name)
        ]

        for file in tqdm(arquivos_filtrados):
            log(f"Baixando o arquivo ------> {file}")

            df = pd.read_csv(
                file,
                sep=";",
                encoding="ISO-8859-1",
                dtype="string",
                quoting=csv.QUOTE_NONE,
            )
            # Rename columns to fit standard
            df_final = df.rename(
                columns=cvm_constants.RENAME_MAPPING_CDA.value
            )
            # Create year and month partiton columns
            create_year_month_columns(df_final)
            # Files are partitioned by block. They must be concatenated
            match = re.search(r"(BLC_[1-8])", file.name)
            df_final["bloco"] = match.group(1)
            df_concat = pd.concat([df_concat, df_final], ignore_index=True)

        # Ensure final columns
        df_concat = check_and_create_column(
            df_concat, colunas_totais=cvm_constants.FINAL_COLS_CDA.value
        )
        df_concat[cvm_constants.TO_MAP_COLS_CDA.value] = df_concat[
            cvm_constants.TO_MAP_COLS_CDA.value
        ].map(lambda x: cvm_constants.MAPEAMENTO.value.get(x, x))

        cnpj_cols = [
            "cnpj",
            "cnpj_instituicao_financeira_coobrigacao",
            "indicador_codigo_identificacao_emissor_pessoa_fisica_juridica",
            "cnpj_emissor",
            "cnpj_fundo_investido",
        ]
        df_concat = format_cnpj_columns(df_concat, cnpj_cols)

        df_concat = df_concat.replace(",", ".", regex=True)
        df_concat[cvm_constants.ASCII_COLS_CDA.value] = df_concat[
            cvm_constants.ASCII_COLS_CDA.value
        ].fillna("")
        df_concat[cvm_constants.ASCII_COLS_CDA.value] = df_concat[
            cvm_constants.ASCII_COLS_CDA.value
        ].map(limpar_string)
        df_concat = df_concat[cvm_constants.FINAL_COLS_CDA.value]

        log(f"Fazendo partições para o ano ------> {i}")

        output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        to_partitions(
            df_concat,
            partition_columns=["ano", "mes"],
            savepath=output_dir,
        )  # constant

    return output_dir


@task
def clean_data_make_partitions_ext(
    input_dir: str | Path,
    table_id: str,
    filename: str = cvm_constants.FILE_EXT.value,
):
    df_final = pd.DataFrame()
    files = [
        file for file in Path(input_dir).glob("*.csv") if file.name == filename
    ]
    filepath = files[0]
    df = pd.read_csv(
        filepath,
        sep=";",
        encoding="ISO-8859-1",
        dtype="string",
        quoting=csv.QUOTE_NONE,
    )
    # Rename columns to fit standard
    df_final = df.rename(columns=cvm_constants.RENAME_MAPPING_EXTRATO.value)
    # Create year and month partiton columns
    df_final = create_year_month_columns(df_final)
    # Ensure final columns
    df_final = check_and_create_column(
        df_final, colunas_totais=cvm_constants.FINAL_COLS_EXTRATO.value
    )
    df_final[cvm_constants.TO_MAP_COLS_EXTRATO.value] = df_final[
        cvm_constants.TO_MAP_COLS_EXTRATO.value
    ].map(lambda x: cvm_constants.MAPEAMENTO.value.get(x, x))
    df_final = format_cnpj_columns(df_final, ["cnpj"])
    df_final = df_final.replace(",", ".", regex=True)
    df_final[cvm_constants.ASCII_COLS_EXTRATO.value] = df_final[
        cvm_constants.ASCII_COLS_EXTRATO.value
    ].fillna("")
    df_final[cvm_constants.ASCII_COLS_EXTRATO.value] = df_final[
        cvm_constants.ASCII_COLS_EXTRATO.value
    ].map(limpar_string)
    df_final = df_final[cvm_constants.FINAL_COLS_EXTRATO.value]

    output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    to_partitions(
        df_final,
        partition_columns=["ano", "mes"],
        savepath=output_dir,
    )

    return output_dir


@task
def clean_data_make_partitions_perfil(input_dir: str | Path, table_id: str):
    df_final = pd.DataFrame()
    arquivos = Path(input_dir).glob("*.csv")

    df_final = pd.DataFrame()
    arquivos = Path(input_dir).glob("*.csv")

    # Import R's utility package
    utils = rpackages.importr("utils")
    # Select a mirror for R packages
    utils.chooseCRANmirror(ind=1)
    # R package names
    packnames = "readr"
    utils.install_packages(packnames)

    # Import readr
    readr = rpackages.importr("readr")

    for file in tqdm(arquivos):
        log(f"Baixando o arquivo ------> {file}")
        ## Reading with R
        df_r = readr.read_delim(
            file, delim=";", locale=readr.locale(encoding="ISO-8859-1")
        )
        readr.write_delim(df_r, file, na="", delim=";")

        ## Return to python
        df = pd.read_csv(
            file, sep=";", encoding="ISO-8859-1", quoting=csv.QUOTE_NONE
        )
        # Rename columns to fit standard
        df_final = df.rename(columns=cvm_constants.RENAME_MAPPING_PERFIL.value)
        # Create year and month partiton columns
        df_final = create_year_month_columns(df_final)
        # Ensure final columns
        df_final = check_and_create_column(
            df_final, colunas_totais=cvm_constants.FINAL_COLS_PERFIL.value
        )
        df_final[cvm_constants.TO_MAP_COLS_PERFIL.value] = df_final[
            cvm_constants.TO_MAP_COLS_PERFIL.value
        ].map(lambda x: cvm_constants.MAPEAMENTO.value.get(x, x))
        cnpj_cols = [
            "cnpj",
            "cpf_cnpj_comitente_1",
            "cpf_cnpj_comitente_2",
            "cpf_cnpj_comitente_3",
            "cpf_cnpj_emissor_1",
            "cpf_cnpj_emissor_2",
            "cpf_cnpj_emissor_3",
        ]
        df_final = format_cnpj_columns(df_final, cnpj_cols)
        df_final = df_final.replace(",", ".", regex=True)
        df_final[cvm_constants.ASCII_COLS_PERFIL_MENSAL.value] = df_final[
            cvm_constants.ASCII_COLS_PERFIL_MENSAL.value
        ].fillna("")
        df_final[cvm_constants.ASCII_COLS_PERFIL_MENSAL.value] = df_final[
            cvm_constants.ASCII_COLS_PERFIL_MENSAL.value
        ].map(limpar_string)
        df_final = df_final[cvm_constants.FINAL_COLS_PERFIL.value]

        output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        to_partitions(
            df_final,
            partition_columns=["ano", "mes"],
            savepath=output_dir,
        )  # constant
        log(f"Partições feitas para o ano ------> {file.name}")
    return output_dir


@task
def clean_data_make_partitions_cad(
    input_dir: Path | str,
    table_id: str,
    filename: str = cvm_constants.FILE_CAD.value,
):
    df_final = pd.DataFrame()
    files = [
        file for file in Path(input_dir).glob("*.csv") if file.name == filename
    ]
    filepath = files[0]

    df = pd.read_csv(
        filepath,
        sep=";",
        encoding="ISO-8859-1",
        dtype="string",
        quoting=csv.QUOTE_NONE,
    )
    # Rename columns to fit standard
    df_final = df.rename(columns=cvm_constants.RENAME_MAPPING_CAD.value)
    # Ensure final columns
    df_final = check_and_create_column(
        df_final, colunas_totais=cvm_constants.FINAL_COLS_CAD.value
    )

    cnpj_cols = cnpj_cols = [
        "cnpj",
        "cnpj_administrador",
        "cpf_cnpj_gestor",
        "cnpj_auditor",
        "cnpj_custodiante",
        "cnpj_controlador",
    ]
    df_final = format_cnpj_columns(df_final, cnpj_cols)
    df_final = df_final.replace(",", ".", regex=True)
    df_final[cvm_constants.ASCII_COLS_CAD.value] = df_final[
        cvm_constants.ASCII_COLS_CAD.value
    ].fillna("")
    df_final[cvm_constants.ASCII_COLS_CAD.value] = df_final[
        cvm_constants.ASCII_COLS_CAD.value
    ].map(limpar_string)
    df_final = df_final[cvm_constants.FINAL_COLS_CAD.value]

    output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    df_final.to_csv(
        output_dir / "data.csv",
        encoding="utf-8",
        index=False,
    )

    return output_dir / "data.csv"


@task
def clean_data_make_partitions_balancete(input_dir: str | Path, table_id: str):
    arquivos = Path(input_dir).glob("*.csv")
    for file in tqdm(arquivos):
        log(f"Baixando o arquivo ------> {file}")
        df = pd.read_csv(
            file,
            sep=";",
            encoding="ISO-8859-1",
            dtype="string",
            quoting=csv.QUOTE_NONE,
        )
        # Rename columns to fit standard
        df_final = df.rename(
            columns=cvm_constants.RENAME_MAPPING_BALANCETE.value
        )
        # Create year and month partiton columns
        df_final = create_year_month_columns(df_final)
        # Ensure final columns
        df_final = check_and_create_column(
            df, colunas_totais=cvm_constants.FINAL_COLS_BALANCETE.value
        )
        df_final = df_final[cvm_constants.FINAL_COLS_BALANCETE.value]
        df_final = format_cnpj_columns(df_final, ["cnpj"])
        df_final = df_final.replace(",", ".", regex=True)

        output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        to_partitions(
            df_final,
            partition_columns=["ano", "mes"],
            savepath=output_dir,
        )
        log(f"Partições feitas para o ano ------> {file}")

    return output_dir
