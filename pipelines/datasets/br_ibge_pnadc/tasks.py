"""
Tasks for br_ibge_pnadc
"""

import os
import zipfile
from datetime import datetime
from glob import glob
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task
from tqdm import tqdm

from pipelines.datasets.br_ibge_pnadc.constants import (
    constants as pnad_constants,
)
from pipelines.datasets.br_ibge_pnadc.utils import get_extraction_year
from pipelines.utils.utils import log


@task
def build_table_paths(
    table_id: str, parent_dir: str | Path = pnad_constants.DATASET_DIR.value
) -> tuple[Path, Path]:
    parent_dir = Path(parent_dir)
    parent_dir.mkdir(parents=True, exist_ok=True)

    table_dir = parent_dir / table_id
    table_dir.mkdir(exist_ok=True)
    table_input_dir = table_dir / "input"
    table_output_dir = table_dir / "output"

    table_input_dir.mkdir(exist_ok=True)
    table_output_dir.mkdir(exist_ok=True)

    return table_input_dir, table_output_dir


@task
def get_data_source_date_and_url() -> tuple[datetime, str]:
    """Return the url for the PNAD microdata file for a given year and month.
    Args:
        year (int): Year of the microdata file.
        quarter (int): Quarter of the microdata file.
    Returns:
        str: url
    """

    year = get_extraction_year()

    download_page = pnad_constants.URL_PREFIX.value.format(year=year)

    response = requests.get(download_page, timeout=60)

    if response.status_code >= 400 and response.status_code <= 599:
        raise Exception(
            f"Erro de requisição: status code {response.status_code}"
        )

    soup = BeautifulSoup(response.text)
    hrefs = [row.get("href") for row in soup.select("tr td a")]
    dates = [
        row.text.strip().split(" ")[0]
        for row in soup.select("table td:nth-child(3)")
    ]
    dados = dict(zip(dates, hrefs, strict=False))
    last_update = max(dados.keys())
    filename = dados[last_update]

    last_modified = datetime.strptime(last_update, "%Y-%m-%d")
    url = download_page + f"{filename}"
    log(f"LAST MODIFIED: {last_modified}\nURL: {url}")

    return last_modified, url


@task
def download_txt(url, chunk_size=128, mkdir=False) -> str:
    """
    Gets all csv files from a url and saves them to a directory.
    """
    if mkdir:
        os.system("mkdir -p /tmp/data/input/")

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    r = requests.get(url, headers=request_headers, stream=True, timeout=10)
    save_path = "/tmp/data/"
    save_path = save_path + url.split("/")[-1]
    with open(save_path, "wb") as fd:
        for chunk in tqdm(r.iter_content(chunk_size=chunk_size)):
            fd.write(chunk)

    with zipfile.ZipFile(save_path) as z:
        z.extractall("/tmp/data/input")
    os.system('cd /tmp/data/input; find . -type f ! -iname "*.txt" -delete')
    filepath = glob("/tmp/data/input/*.txt")[0]

    log(f"Using file {filepath}")

    return filepath


@task
def build_partitions(input_path: str | Path, output_dir: str | Path) -> str:
    """
    Build parquets from txt original file.
    """

    filepaths = glob(f"{input_path}*/.txt")
    filepath = filepaths[0] if len(filepaths) > 0 else filepaths
    chunks = pd.read_fwf(
        filepath,
        widths=pnad_constants.COLUMNS_WIDTHS.value,
        names=pnad_constants.COLUMNS_NAMES.value,
        header=None,
        encoding="utf-8",
        dtype=str,
        chunksize=25000,
    )

    for _, chunk in enumerate(chunks):
        # partition by year, quarter and region
        chunk = chunk.rename(
            columns={
                "UF": "id_uf",
                "Estrato": "id_estrato",
                "UPA": "id_upa",
                "Capital": "capital",
                "RM_RIDE": "rm_ride",
                "Trimestre": "trimestre",
                "Ano": "ano",
            }
        )
        chunk["sigla_uf"] = chunk["id_uf"].map(
            pnad_constants.map_codigo_sigla_uf.value
        )
        chunk["id_domicilio"] = (
            chunk["id_estrato"] + chunk["V1008"] + chunk["V1014"]
        )

        chunk["habitual"] = [np.nan] * len(chunk)
        chunk["efetivo"] = [np.nan] * len(chunk)
        ordered_columns = pnad_constants.COLUMNS_ORDER.value
        chunk = chunk[ordered_columns]

        trimestre = chunk["trimestre"].unique()[0]
        ano = chunk["ano"].unique()[0]
        chunk = chunk.drop(columns=["trimestre", "ano"])
        ufs = chunk["sigla_uf"].unique()

        for uf in ufs:
            df_uf = chunk[chunk["sigla_uf"] == uf]
            df_uf = df_uf.drop(columns=["sigla_uf"])

            os.makedirs(
                os.path.join(
                    output_dir,
                    f"ano={ano}/trimestre={trimestre}/sigla_uf={uf}",
                ),
                exist_ok=True,
            )

            # Save to CSV incrementally
            df_uf.to_csv(
                f"{output_dir}/ano={ano}/trimestre={trimestre}/sigla_uf={uf}/data.csv",
                index=False,
                mode="a",
                header=not os.path.exists(
                    f"{output_dir}/ano={ano}/trimestre={trimestre}/sigla_uf={uf}/data.csv"
                ),
            )

        # Release memory
        del df_uf
        del chunk
    log("Partitions created")

    return "/tmp/data/output/"
