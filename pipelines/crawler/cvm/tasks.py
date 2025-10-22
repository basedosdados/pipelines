"""
Tasks for br_cvm_fi
"""

import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task
from tqdm import tqdm

from pipelines.constants import constants
from pipelines.crawler.cvm.constants import constants as cvm_constants
from pipelines.crawler.cvm.utils import (
    TABLE_CONFIGS,
    apply_common_transformations,
    obter_anos_meses,
    process_file,
    save_output,
)
from pipelines.utils.utils import log


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def extract_links_and_dates(
    table_id: str, url: str | None = None
) -> tuple[pd.DataFrame, str]:
    """
    Extracts all file names and their respective last update dates in a pandas dataframe.

    Parameters:

    url: str
        The base URL from which files will be downloaded.

    Returns:
        tuple[pd.DataFrame, str]: The files and dates data frame, along with the max date (last update)
    """
    if url is None:
        url = TABLE_CONFIGS[table_id][url]

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
def download_unzip(
    table_id: str,
    files: list | str,
    url: str | None = None,
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
    if url is None:
        url = TABLE_CONFIGS[table_id][url]
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


# Generic cleaning task
@task
def clean_cvm_data(
    input_dir: str | Path, table_id: str, config_key: str
) -> str:
    """
    Generic CVM data cleaning task that handles all table types.
    """
    config = TABLE_CONFIGS[config_key]

    if len(config["files"]) == 0:
        files = list(Path(input_dir).glob("*.csv"))
    else:
        files = [Path(input_dir) / file for file in config["files"]]

    if config_key == "documentos_carteiras_fundos_investimento":
        return _clean_cda_data(config, input_dir, table_id)
    elif config_key == "documentos_perfil_mensal":
        return _clean_perfil_data(config, input_dir, table_id)
    else:
        return _clean_standard_data(files, table_id, config)


def _clean_standard_data(
    files: list[Path], table_id: str, config: dict
) -> str:
    """Clean standard CVM data (single file or multiple independent files)."""
    all_data = []

    for file in files:
        df = process_file(config=config, file_path=file)
        df = apply_common_transformations(config, df)
        all_data.append(df)

    if len(all_data) == 1:
        final_df = all_data[0]
    else:
        final_df = pd.concat(all_data, ignore_index=True)

    return save_output(
        config,
        final_df,
        table_id,
        config.get("create_partition_columns", True),
    )


def _clean_cda_data(config: dict, input_dir: str | Path, table_id: str) -> str:
    """Special handling for CDA data that needs grouping by year-month."""
    anos_meses = obter_anos_meses(input_dir)

    for ano_mes in anos_meses:
        df_concat = pd.DataFrame()
        arquivos = Path(input_dir).glob("*.csv")
        padrao = f"cda_fi_BLC_[1-8]_{ano_mes}.csv"
        arquivos_filtrados = [
            arq for arq in arquivos if re.match(padrao, arq.name)
        ]

        for file in arquivos_filtrados:
            df = process_file(config=config, file_path=file)

            # CDA-specific: add block information
            match = re.search(r"(BLC_[1-8])", file.name)
            df["bloco"] = match.group(1)

            df_concat = pd.concat([df_concat, df], ignore_index=True)

        df_concat = apply_common_transformations(config, df_concat)
        save_output(config, df_concat, table_id, use_partitions=True)

    output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
    return str(output_dir)


def _clean_perfil_data(
    config: dict, input_dir: str | Path, table_id: str
) -> str:
    """Special handling for Perfil data that requires R processing."""
    files = Path(input_dir).glob("*.csv")

    for file in files:
        # Standard processing
        df = process_file(config=config, file_path=file)
        df = apply_common_transformations(config, df)
        save_output(config, df, table_id, use_partitions=True)

    output_dir = cvm_constants.DATASET_DIR.value / str(table_id) / "output"
    return str(output_dir)
