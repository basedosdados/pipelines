# -*- coding: utf-8 -*-
"""
Tasks for br_ibge_pnadc
"""
import os

# pylint: disable=invalid-name,unnecessary-dunder-call
import zipfile
from glob import glob

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from prefect import task
from tqdm import tqdm

from pipelines.datasets.br_ibge_pnadc.constants import constants as pnad_constants
from pipelines.utils.utils import log, to_partitions


@task
def get_url_from_template(year: int, quarter: int) -> str:
    """Return the url for the PNAD microdata file for a given year and month.
    Args:
        year (int): Year of the microdata file.
        quarter (int): Quarter of the microdata file.
    Returns:
        str: url
    """
    download_page = f"https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/{year}/"
    response = requests.get(download_page, timeout=5)
    if response.status_code >= 400 and response.status_code <= 599:
        raise Exception(f"Erro de requisição: status code {response.status_code}")

    else:
        hrefs = [k for k in response.text.split('href="')[1:] if "zip" in k]
        hrefs = [k.split('"')[0] for k in hrefs]
        filename = None
        for href in hrefs:
            if f"0{quarter}{year}" in href:
                filename = href
        if not filename:
            raise Exception("Erro: o atributo href não existe.")

        url = pnad_constants.URL_PREFIX.value + "/{year}/{filename}"
    return url.format(year=year, filename=filename)


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
def build_parquet_files(save_path: str) -> str:
    """
    Build parquets from txt original file.
    """
    filepath = glob(f"{save_path}*.txt")[0]
    os.system("mkdir -p /tmp/data/staging/")
    os.system("mkdir -p /tmp/data/output/")
    output_dir = "/tmp/data/output/"
    # read file
    chunks = pd.read_fwf(
        filepath,
        widths=pnad_constants.COLUMNS_WIDTHS.value,
        names=pnad_constants.COLUMNS_NAMES.value,
        header=None,
        encoding="utf-8",
        dtype=str,
        chunksize=25000,
    )

    for i, chunk in enumerate(chunks):
        # partition by year, quarter and region
        chunk.rename(
            columns={
                "UF": "id_uf",
                "Estrato": "id_estrato",
                "UPA": "id_upa",
                "Capital": "capital",
                "RM_RIDE": "rm_ride",
                "Trimestre": "trimestre",
                "Ano": "ano",
            },
            inplace=True,
        )
        chunk["sigla_uf"] = chunk["id_uf"].map(pnad_constants.map_codigo_sigla_uf.value)
        chunk["id_domicilio"] = chunk["id_estrato"] + chunk["V1008"] + chunk["V1014"]

        chunk["habitual"] = [np.nan] * len(chunk)
        chunk["efetivo"] = [np.nan] * len(chunk)
        ordered_columns = pnad_constants.COLUMNS_ORDER.value
        chunk = chunk[ordered_columns]

        trimestre = chunk["trimestre"].unique()[0]
        ano = chunk["ano"].unique()[0]
        chunk.drop(columns=["trimestre", "ano"], inplace=True)
        ufs = chunk["sigla_uf"].unique()

        for uf in ufs:
            df_uf = chunk[chunk["sigla_uf"] == uf]
            df_uf.drop(columns=["sigla_uf"], inplace=True)

            os.makedirs(
                os.path.join(
                    output_dir, f"ano={ano}/trimestre={trimestre}/sigla_uf={uf}"
                ),
                exist_ok=True,
            )

            # Save to CSV incrementally
            df_uf.to_csv(
                f"{output_dir}/ano={ano}/trimestre={trimestre}/sigla_uf={uf}/microdados.csv",
                index=False,
                mode="a",
                header=not os.path.exists(
                    f"{output_dir}/ano={ano}/trimestre={trimestre}/sigla_uf={uf}/microdados.csv"
                ),
            )

        # Release memory
        del df_uf
        del chunk
    log("Partitions created")

    return "/tmp/data/output/"


### Unused task
# @task
# def save_partitions(filepath: str) -> str:
#     """
#     Save partitions to disk.

#     Args:
#         filepath (str): Path to the file used to build the partitions.

#     Returns:
#         str: Path to the saved file.
#     """
#     os.system("mkdir -p /tmp/data/output/")
#     output_dir = "/tmp/data/output/"
#     os.listdir(output_dir)
#     # Read Parquet file incrementally
#     chunksize = 10000
#     for i, fp in enumerate(Path("/tmp/data/staging/").glob("*.parquet")):
#         # Process each chunk
#         parquet_file = pq.ParquetFile(f"{fp}")
#         for batch in parquet_file.iter_batches(batch_size=chunksize):
#             table = pa.Table.from_batches([batch])
#             chunk = table.to_pandas()
#             trimestre = chunk["trimestre"].unique()[0]
#             ano = chunk["ano"].unique()[0]
#             chunk.drop(columns=["trimestre", "ano"], inplace=True)
#             ufs = chunk["sigla_uf"].unique()

#             for uf in ufs:
#                 df_uf = chunk[chunk["sigla_uf"] == uf]
#                 df_uf.drop(columns=["sigla_uf"], inplace=True)

#                 os.makedirs(
#                     os.path.join(
#                         output_dir, f"ano={ano}/trimestre={trimestre}/sigla_uf={uf}"
#                     ),
#                     exist_ok=True,
#                 )

#                 # Save to CSV incrementally
#                 df_uf.to_csv(
#                     f"{output_dir}/ano={ano}/trimestre={trimestre}/sigla_uf={uf}/microdados.csv",
#                     index=False,
#                     mode="a",
#                     header=not os.path.exists(
#                         f"{output_dir}/ano={ano}/trimestre={trimestre}/sigla_uf={uf}/microdados.csv"
#                     ),
#                 )

#             # Release memory
#             del df_uf

#     return output_dir
