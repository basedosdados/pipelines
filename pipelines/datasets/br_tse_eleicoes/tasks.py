# -*- coding: utf-8 -*-
"""
Tasks for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long
from datetime import timedelta
import zipfile
import os
from glob import glob

import requests
from unidecode import unidecode
from tqdm import tqdm
import numpy as np
import pandas as pd
from prefect import task
from pipelines.constants import constants

from pipelines.datasets.br_tse_eleicoes.utils import get_id_candidato_bd, get_blobs_from_raw


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_before22(table_id: str) -> None:
    '''
    Download external data from previous elections
    '''
    os.system("mkdir -p /tmp/data/input")
    blobs = get_blobs_from_raw(dataset_id="br_tse_eleicoes", table_id=table_id)
    for blob in tqdm(blobs):
        df = pd.read_csv(blob.public_url, encoding="utf-8")
        os.system(f"mkdir -p /tmp/data/{'/'.join(blob.name.split('/')[:-1])}")
        df.to_csv(f"/tmp/data/{blob.name}", sep=";", index=False)
        del df


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_csv_files(url, save_path, chunk_size=128) -> None:
    """
    Gets all csv files from a url and saves them to a directory.
    """
    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    r = requests.get(url, headers=request_headers, stream=True)
    save_path = save_path + url.split("/")[-1]
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    with zipfile.ZipFile(save_path) as z:
        z.extractall("/tmp/data/input")
    os.system('cd /tmp/data/input; find . -type f ! -iname "*.csv" -delete')
    os.system("tree /tmp/data/input")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_candidatos22(folder: str):
    """
    Cleans the candidatos csv file.
    """

    dfs = []

    files = glob(f"{folder}/*.csv")

    for file in files:
        df = pd.read_csv(file, sep=";", encoding="latin-1")
        dfs.append(df)

    df = pd.concat(dfs)

    n = df.shape[0]

    table = pd.DataFrame(
        {
            "tipo_eleicao": [
                unidecode(unidecode(k.lower())) if isinstance(k, str) else k
                for k in df["NM_TIPO_ELEICAO"].to_list()
            ],
            "sigla_uf": df["SG_UF"].to_list(),
            "id_municipio": n * [np.nan],
            "id_municipio_tse": n * [np.nan],
            "id_candidato_bd": n * [np.nan],
            "cpf": [
                str(k).zfill(11) if isinstance(k, int) else k
                for k in df["NR_CPF_CANDIDATO"].to_list()
            ],
            "titulo_eleitoral": [
                str(k).zfill(12) if isinstance(k, int) else k
                for k in df["NR_TITULO_ELEITORAL_CANDIDATO"].to_list()
            ],
            "sequencial": df["SQ_CANDIDATO"].to_list(),
            "numero": df["NR_CANDIDATO"].to_list(),
            "nome": [
                unidecode(k.title()) if isinstance(k, str) else k
                for k in df["NM_CANDIDATO"].to_list()
            ],
            "nome_urna": [
                unidecode(k.title()) if isinstance(k, str) else k
                for k in df["NM_URNA_CANDIDATO"].to_list()
            ],
            "numero_partido": df["NR_PARTIDO"].to_list(),
            "sigla_partido": df["SG_PARTIDO"].to_list(),
            "cargo": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_CARGO"].to_list()
            ],
            "situacao": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_SITUACAO_CANDIDATURA"].to_list()
            ],
            "ocupacao": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_OCUPACAO"].to_list()
            ],
            "data_nascimento": [
                k.replace("/", "-") if isinstance(k, str) else k
                for k in df["DT_NASCIMENTO"].to_list()
            ],
            "idade": df["NR_IDADE_DATA_POSSE"].to_list(),
            "genero": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_GENERO"].to_list()
            ],
            "instrucao": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_GRAU_INSTRUCAO"].to_list()
            ],
            "estado_civil": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_ESTADO_CIVIL"].to_list()
            ],
            "nacionalidade": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_NACIONALIDADE"].to_list()
            ],
            "sigla_uf_nascimento": df["SG_UF_NASCIMENTO"].to_list(),
            "municipio_nascimento": [
                unidecode(k.title()) if isinstance(k, str) else k
                for k in df["NM_MUNICIPIO_NASCIMENTO"].to_list()
            ],
            "email": [
                k.lower() if isinstance(k, str) else k for k in df["NM_EMAIL"].to_list()
            ],
            "raca": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_COR_RACA"].to_list()
            ],
            "situacao_totalizacao": df["DS_SITUACAO_CANDIDATO_TOT"].to_list(),
            "numero_federacao": df["NR_FEDERACAO"].to_list(),
            "nome_federacao": [
                unidecode(k.title()) if isinstance(k, str) else k
                for k in df["NM_FEDERACAO"].to_list()
            ],
            "sigla_federacao": df["SG_FEDERACAO"].to_list(),
            "composicao_federacao": [
                unidecode(k.lower()) if isinstance(k, str) else k
                for k in df["DS_COMPOSICAO_FEDERACAO"].to_list()
            ],
            "prestou_contas": df["ST_PREST_CONTAS"].to_list(),
        }
    )

    del df
    df = table.copy()
    del table

    df.replace("#NULO#", np.nan, inplace=True)
    df.replace("#Nulo#", np.nan, inplace=True)
    df.replace("#nulo#", np.nan, inplace=True)

    os.system("mkdir -p /tmp/data/staging/br_tse_eleicoes/candidatos/ano=2022/")

    df.to_csv(
        "/tmp/data/staging/br_tse_eleicoes/candidatos/ano=2022/candidatos.csv",
        sep=";",
        index=False,
    )

    os.system("tree /tmp/data/")


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def build_candidatos(folder: str):
    """
    Builds the candidatos csv file.
    """

    dfs = []

    files = [f"{folder}/ano={ano}/candidatos.csv" for ano in range(2022, 1992, -2)]

    for file in files:
        df = pd.read_csv(
            file,
            sep=";",
            encoding="latin-1",
            dtype={
                "id_candidato_bd": str,
                "cpf": str,
                "titulo_eleitoral": str,
                "sequencial": str,
                "numero": str,
            },
        )
        df["ano"] = int(file.split("/")[-2].split("=")[-1])
        dfs.append(df)

    df = pd.concat(dfs)

    df = get_id_candidato_bd(df)

    for ano in range(2022, 1992, -2):
        os.system(f"mkdir -p /tmp/data/output/ano={ano}/")
        table = df[df["ano"] == ano]
        table.drop_duplicates(inplace=True)
        table.drop("ano", axis=1, inplace=True)
        table.to_csv(f"/tmp/data/output/ano={ano}/candidatos.csv", index=False)

    os.system("tree /tmp/data/")

    return "/tmp/data/output/"
