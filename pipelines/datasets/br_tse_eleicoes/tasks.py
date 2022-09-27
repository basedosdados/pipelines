# -*- coding: utf-8 -*-
"""
Tasks for br_tse_eleicoes
"""
# pylint: disable=invalid-name,line-too-long
from datetime import timedelta
import zipfile
import os
from glob import glob
from itertools import product
import re

import requests
from unidecode import unidecode
from tqdm import tqdm
import numpy as np
import pandas as pd
from prefect import task
from pipelines.constants import constants
from pipelines.utils.utils import log
from pipelines.datasets.br_tse_eleicoes.utils import (
    get_id_candidato_bd,
    get_blobs_from_raw,
    normalize_dahis,
    get_data_from_prod,
    clean_digit_id,
)


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_before22(table_id: str, start: int) -> None:
    """
    Download external data from previous elections
    """
    os.system("mkdir -p /tmp/data/input")
    blobs = get_blobs_from_raw(dataset_id="br_tse_eleicoes", table_id=table_id)
    for blob in tqdm(blobs):
        if int("".join([k for k in blob.name if k.isdigit()])) > start:
            df = pd.read_csv(blob.public_url, encoding="utf-8")
            os.system(f"mkdir -p /tmp/data/{'/'.join(blob.name.split('/')[:-1])}")
            df.to_csv(f"/tmp/data/{blob.name}", sep=";", index=False, encoding="utf-8")
            del df

    log(os.system("tree /tmp/data/"))


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def get_csv_files(url, save_path, chunk_size=128, mkdir=False) -> None:
    """
    Gets all csv files from a url and saves them to a directory.
    """
    if mkdir:
        os.system("mkdir -p /tmp/data/input/")

    request_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    r = requests.get(url, headers=request_headers, stream=True, timeout=10)
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
def clean_candidatos22(folder: str) -> str:
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
                clean_digit_id(number=k, n_digits=11) if isinstance(k, int) else k
                for k in df["NR_CPF_CANDIDATO"].to_list()
            ],
            "titulo_eleitoral": [
                clean_digit_id(number=k, n_digits=12) if isinstance(k, int) else k
                for k in df["NR_TITULO_ELEITORAL_CANDIDATO"].to_list()
            ],
            "sequencial": [
                clean_digit_id(number=k, n_digits=12) if isinstance(k, int) else k
                for k in df["SQ_CANDIDATO"].to_list()
            ],
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
            "data_nascimento": pd.to_datetime(
                df["DT_NASCIMENTO"], format="%d/%m/%Y"
            ).to_list(),
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
    df.replace(-1, np.nan, inplace=True)

    os.system("mkdir -p /tmp/data/raw/br_tse_eleicoes/candidatos/ano=2022/")

    df.to_csv(
        "/tmp/data/raw/br_tse_eleicoes/candidatos/ano=2022/candidatos.csv",
        sep=";",
        index=False,
    )

    os.system("tree /tmp/data/")

    return "/tmp/data/raw/br_tse_eleicoes/candidatos/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def build_candidatos(folder: str, start: int, end: int, id_candidato_bd: bool = False):
    """
    Builds the candidatos csv file.
    """

    if id_candidato_bd:
        dfs = []

        files = [f"{folder}/ano={ano}/candidatos.csv" for ano in range(end, start, -2)]

        for file in files:
            df = pd.read_csv(
                file,
                sep=";",
                encoding="utf-8",
                dtype={
                    "id_candidato_bd": str,
                    "cpf": str,
                    "titulo_eleitoral": str,
                    "sequencial": str,
                },
            )
            df["ano"] = int(file.split("/")[-2].split("=")[-1])
            dfs.append(df)

        df = pd.concat(dfs)

        df = get_id_candidato_bd(df)

        df = normalize_dahis(df)

        for ano in range(end, start, -2):
            os.system(f"mkdir -p /tmp/data/output/ano={ano}/")
            table = df[df["ano"] == ano]
            table["cpf"] = [
                clean_digit_id(number=k, n_digits=11) if isinstance(k, int) else k
                for k in table["cpf"].to_list()
            ]
            table["titulo_eleitoral"] = [
                clean_digit_id(number=k, n_digits=12) if isinstance(k, int) else k
                for k in table["titulo_eleitoral"].to_list()
            ]
            table["sequencial"] = [
                clean_digit_id(number=k, n_digits=12) if isinstance(k, int) else k
                for k in table["sequencial"].to_list()
            ]
            table.drop_duplicates(inplace=True)
            table.drop("ano", axis=1, inplace=True)
            table.to_csv(f"/tmp/data/output/ano={ano}/candidatos.csv", index=False)
    else:
        for ano in range(end, start, -2):
            df = pd.read_csv(
                f"/tmp/data/raw/br_tse_eleicoes/candidatos/ano={ano}/candidatos.csv",
                sep=";",
                encoding="utf-8",
            )
            df["cpf"] = [
                clean_digit_id(number=k, n_digits=11) if pd.notna(k) else k
                for k in df["cpf"].to_list()
            ]
            df["titulo_eleitoral"] = [
                clean_digit_id(number=k, n_digits=12) if pd.notna(k) else k
                for k in df["titulo_eleitoral"].to_list()
            ]
            df["sequencial"] = [
                clean_digit_id(number=k, n_digits=12) if pd.notna(k) else k
                for k in df["sequencial"].to_list()
            ]
            df.replace("#NULO#", np.nan, inplace=True)
            df.replace("#Nulo#", np.nan, inplace=True)
            df.replace("#NI#", np.nan, inplace=True)
            df.replace(-1, np.nan, inplace=True)
            df.drop_duplicates(inplace=True)
            os.system(f"mkdir -p /tmp/data/output/ano={ano}/")
            df.to_csv(f"/tmp/data/output/ano={ano}/candidatos.csv", index=False)

    return "/tmp/data/output/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def build_bens_candidato(
    folder: str, start: int, end: int, id_candidato_bd: bool = False
) -> str:
    """
    Builds the bens_candidato csv file.
    """
    ufs = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "BR",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
        "BR",
        "VT",
        "ZZ",
    ]
    dfs = []
    if id_candidato_bd:
        files = [
            f"{folder}/ano={ano}/sigla_uf={uf}/bens_candidato.csv"
            for ano, uf in product(range(end, start, -2), ufs)
        ]

        for file in files:
            try:
                df = pd.read_csv(file, sep=";", encoding="utf-8")
                df["ano"] = int(file.split("/")[-3].split("=")[-1])
                df["sigla_uf"] = file.split("/")[-2].split("=")[-1]
                dfs.append(df)
            except FileNotFoundError:
                log(f"File {file} not found")
                continue

        df = pd.concat(dfs)

        candidatos = get_data_from_prod(
            "br_tse_eleicoes",
            "candidatos",
            ["ano", "tipo_eleicao", "sigla_uf", "sequencial", "id_candidato_bd"],
        )

        candidatos.drop_duplicates(inplace=True)
        candidatos["ano"] = [
            int(k) if str(k).isdigit() else k for k in candidatos["ano"]
        ]

        df.rename(columns={"sequencial_candidato": "sequencial"}, inplace=True)

        df.drop(columns=["id_candidato_bd"], inplace=True)

        df = df.merge(
            candidatos, on=["ano", "tipo_eleicao", "sigla_uf", "sequencial"], how="left"
        )

        df.rename(columns={"sequencial": "sequencial_candidato"}, inplace=True)

        df = df.reindex(
            columns=[
                "ano",
                "sigla_uf",
                "tipo_eleicao",
                "sequencial_candidato",
                "id_candidato_bd",
                "id_tipo_item",
                "tipo_item",
                "descricao_item",
                "valor_item",
            ]
        )

        for ano, uf in product(range(end, start, -2), ufs):
            table = df[df["ano"] == ano]
            table = table[table["sigla_uf"] == uf]
            if table.shape[0] == 0:
                continue
            os.system(f"mkdir -p /tmp/data/output/ano={ano}/sigla_uf={uf}/")
            table.drop_duplicates(inplace=True)
            table.drop("ano", axis=1, inplace=True)
            table.drop("sigla_uf", axis=1, inplace=True)
            table.to_csv(
                f"/tmp/data/output/ano={ano}/sigla_uf={uf}/bens_candidato.csv",
                index=False,
            )

    else:
        for uf in ufs:
            try:
                file = f"{folder}/ano=2022/sigla_uf={uf}/bens_candidato.csv"
                df = pd.read_csv(file, sep=";", encoding="utf-8")
                os.system(f"mkdir -p /tmp/data/output/ano=2022/sigla_uf={uf}/")
                df.drop("sigla_uf", axis=1, inplace=True)
                df = df.reindex(
                    columns=[
                        "tipo_eleicao",
                        "sequencial_candidato",
                        "id_candidato_bd",
                        "id_tipo_item",
                        "tipo_item",
                        "descricao_item",
                        "valor_item",
                    ]
                )
                df.drop_duplicates(inplace=True)
                df.to_csv(
                    f"/tmp/data/output/ano=2022/sigla_uf={uf}/bens_candidato.csv",
                    index=False,
                )
            except FileNotFoundError:
                log(f"File {file} not found")
                continue

    return "/tmp/data/output/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_bens22(folder) -> None:
    """
    Clean bens_canidadto.csv files for 2022
    """
    dfs = []

    files = glob(f"{folder}/bem_candidato*.csv")

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
            "sequencial_candidato": df["SQ_CANDIDATO"].to_list(),
            "id_tipo_item": df["CD_TIPO_BEM_CANDIDATO"].to_list(),
            "id_candidato_bd": n * np.nan,
            "tipo_item": [
                unidecode(unidecode(k.title())) if isinstance(k, str) else k
                for k in df["DS_TIPO_BEM_CANDIDATO"].to_list()
            ],
            "descricao_item": [
                unidecode(unidecode(k.title())) if isinstance(k, str) else k
                for k in df["DS_BEM_CANDIDATO"].to_list()
            ],
            "valor_item": [
                float(k.replace(",", ".")) if k.replace(",", "").isdigit() else k
                for k in df["VR_BEM_CANDIDATO"].to_list()
            ],
        }
    )

    del df
    df = table.copy()
    del table

    df.replace("#NULO#", np.nan, inplace=True)
    df.replace("#Nulo#", np.nan, inplace=True)
    df.replace("#nulo#", np.nan, inplace=True)

    ufs = df["sigla_uf"].unique()

    for uf in ufs:
        df_uf = df[df["sigla_uf"] == uf].copy()
        os.system(
            f"mkdir -p /tmp/data/raw/br_tse_eleicoes/bens_candidato/ano=2022/sigla_uf={uf}/"
        )
        df_uf.to_csv(
            f"/tmp/data/raw/br_tse_eleicoes/bens_candidato/ano=2022/sigla_uf={uf}/bens_candidato.csv",
            index=False,
            sep=";",
        )
        del df_uf


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_despesa22(folder):
    """
    Clean despesa_candidato.csv files for 2022
    """
    files = glob(f"{folder}/despesas_contratadas*.csv")

    for file in files:
        df = pd.read_csv(file, sep=";", encoding="latin-1")
        n = df.shape[0]
        uf = "".join([k for k in file if k.isupper()])

        table = pd.DataFrame(
            {
                "ano": int("".join([k for k in file if k.isdigit()])),
                "turno": df["ST_TURNO"].to_list(),
                "tipo_eleicao": [
                    unidecode(k.lower()) if isinstance(k, str) else k
                    for k in df["NM_TIPO_ELEICAO"]
                ],
                "sigla_uf": uf,
                "id_municipio": n * [np.nan],
                "id_municipio_tse": n * [np.nan],
                "numero_candidato": df["NR_CANDIDATO"].to_list(),
                "cpf_candidato": [
                    clean_digit_id(k, n_digits=11) if pd.notna(k) else k
                    for k in df["NR_CPF_CANDIDATO"].to_list()
                ],
                "sequencial_candidato": [
                    clean_digit_id(k, n_digits=12) if pd.notna(k) else k
                    for k in df["SQ_CANDIDATO"].to_list()
                ],
                "id_candidato_bd": n * [np.nan],
                "nome_candidato": [
                    unidecode(k.title()) if isinstance(k, str) else k
                    for k in df["NM_CANDIDATO"].to_list()
                ],
                "cpf_vice_suplente": [
                    clean_digit_id(k, n_digits=11) if pd.notna(k) else k
                    for k in df["NR_CPF_VICE_CANDIDATO"].to_list()
                ],
                "numero_partido": df["NR_PARTIDO"].to_list(),
                "sigla_partido": df["SG_PARTIDO"].to_list(),
                "nome_partido": df["NM_PARTIDO"].to_list(),
                "cargo": df["DS_CARGO"].to_list(),
                "sequencial_despesa": df["SQ_DESPESA"].to_list(),
                "data_despesa": pd.to_datetime(
                    df["DT_DESPESA"], format="%d/%m/%Y"
                ).to_list(),
                "tipo_despesa": n * [np.nan],
                "descricao_despesa": df["DS_DESPESA"].to_list(),
                "origem_despesa": df["DS_ORIGEM_DESPESA"].to_list(),
                "valor_despesa": [
                    str(k).replace(",", ".") if k.replace(",", "").isdigit() else k
                    for k in df["VR_DESPESA_CONTRATADA"].to_list()
                ],
                "tipo_prestacao_contas": df["TP_PRESTACAO_CONTAS"].to_list(),
                "data_prestacao_contas": pd.to_datetime(
                    df["DT_PRESTACAO_CONTAS"], format="%d/%m/%Y"
                ).to_list(),
                "sequencial_prestador_contas": df["SQ_PRESTADOR_CONTAS"].to_list(),
                "cnpj_prestador_contas": [
                    clean_digit_id(k, n_digits=14) if pd.notna(k) else k
                    for k in df["NR_CNPJ_PRESTADOR_CONTA"].to_list()
                ],
                "cnpj_candidato": n * [np.nan],
                "tipo_documento": df["DS_TIPO_DOCUMENTO"].to_list(),
                "numero_documento": df["NR_DOCUMENTO"].to_list(),
                "especie_recurso": n * [np.nan],
                "fonte_recurso": n * [np.nan],
                "cpf_cnpj_fornecedor": df["NR_CPF_CNPJ_FORNECEDOR"].to_list(),
                "nome_fornecedor": df["NM_FORNECEDOR"].to_list(),
                "cnae_2_fornecedor": df["CD_CNAE_FORNECEDOR"].to_list(),
                "descricao_cnae_2_fornecedor": df["DS_CNAE_FORNECEDOR"].to_list(),
                "tipo_fornecedor": df["DS_TIPO_FORNECEDOR"].to_list(),
                "esfera_partidaria_fornecedor": df[
                    "DS_ESFERA_PART_FORNECEDOR"
                ].to_list(),
                "sigla_uf_fornecedor": df["SG_UF_FORNECEDOR"].to_list(),
                "id_municipio_tse_fornecedor": df["CD_MUNICIPIO_FORNECEDOR"].to_list(),
                "sequencial_candidato_fornecedor": df[
                    "SQ_CANDIDATO_FORNECEDOR"
                ].to_list(),
                "numero_candidato_fornecedor": df["NR_CANDIDATO_FORNECEDOR"].to_list(),
                "numero_partido_fornecedor": df["NR_PARTIDO_FORNECEDOR"].to_list(),
                "sigla_partido_fornecedor": df["SG_PARTIDO_FORNECEDOR"].to_list(),
                "nome_partido_fornecedor": df["NM_PARTIDO_FORNECEDOR"].to_list(),
                "cargo_fornecedor": df["DS_CARGO_FORNECEDOR"].to_list(),
            }
        )

        table["tipo_eleicao"] = table["tipo_eleicao"].replace(
            {"ordinaria": "eleicao ordinaria"}
        )
        table.replace("#NULO#", np.nan, inplace=True)
        table.replace("#Nulo#", np.nan, inplace=True)
        table.replace("#nulo#", np.nan, inplace=True)
        table.replace(-1, np.nan, inplace=True)

        if table.shape[0] == 0:
            continue
        os.system(f"mkdir -p /tmp/data/output/ano=2022/sigla_uf={uf}/")
        table.drop_duplicates(inplace=True)
        table.drop("ano", axis=1, inplace=True)
        table.drop("sigla_uf", axis=1, inplace=True)
        table.to_csv(
            f"/tmp/data/output/ano=2022/sigla_uf={uf}/despesas_candidato.csv",
            index=False,
        )

    os.system("rm -rf /tmp/data/output/ano=2022/sigla_uf=BR*")

    return "/tmp/data/output/"


@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def clean_receita22(folder):
    """
    Clean receita candidatos data
    """

    files = glob(f"{folder}/*.csv")
    files = list(
        filter(
            lambda x: bool(
                re.match(r"receitas_candidatos_\d{4}_[A-Z]{2}.csv", x.split("/")[-1])
            ),
            files,
        )
    )

    for file in files:
        df = pd.read_csv(file, sep=";", encoding="latin-1")
        # replace cells like '##############' to np.nan
        df.replace({r'^#*$':np.nan}, regex=True, inplace=True)
        n = df.shape[0]
        uf = "".join([k for k in file if k.isupper()])

        table = pd.DataFrame(
            {
                "ano": int("".join([k for k in file if k.isdigit()])),
                "turno": df["ST_TURNO"].to_list(),
                "tipo_eleicao": [
                    unidecode(k.lower()) for k in df["NM_TIPO_ELEICAO"].to_list()
                ],
                "sigla_uf": uf,
                "id_municipio": n * [np.nan],
                "id_municipio_tse": n * [np.nan],
                "numero_candidato": df["NR_CANDIDATO"].to_list(),
                "cpf_candidato": [
                    clean_digit_id(k, n_digits=11) if pd.notna(k) else k
                    for k in df["NR_CPF_CANDIDATO"].to_list()
                ],
                "cnpj_candidato": n * [np.nan],
                "titulo_eleitor_candidato": n * [np.nan],
                "sequencial_candidato": [
                    clean_digit_id(k, n_digits=12) if pd.notna(k) else k
                    for k in df["SQ_CANDIDATO"].to_list()
                ],
                "id_candidato_bd": n * [np.nan],
                "nome_candidato": [
                    unidecode(k.title()) if pd.notna(k) else k
                    for k in df["NM_CANDIDATO"].to_list()
                ],
                "cpf_vice_suplente": [
                    str(k).replace(".0", "") if str(k)[0].isdigit() else k
                    for k in df["NR_CPF_VICE_CANDIDATO"].to_list()
                ],
                "numero_partido": df["NR_PARTIDO"].to_list(),
                "nome_partido": df["NM_PARTIDO"].to_list(),
                "sigla_partido": df["SG_PARTIDO"].to_list(),
                "cargo": df["DS_CARGO"].to_list(),
                "sequencial_receita": df["SQ_RECEITA"].to_list(),
                "data_receita": pd.to_datetime(
                    df["DT_RECEITA"], format="%d/%m/%Y"
                ).to_list(),
                "fonte_receita": [
                    unidecode(k.lower()) if isinstance(k, str) else k
                    for k in df["DS_FONTE_RECEITA"].to_list()
                ],
                "origem_receita": [
                    unidecode(k.lower()) if isinstance(k, str) else k
                    for k in df["DS_ORIGEM_RECEITA"].to_list()
                ],
                "natureza_receita": [
                    unidecode(k.lower()) if isinstance(k, str) else k
                    for k in df["DS_NATUREZA_RECEITA"].to_list()
                ],
                "especie_receita": df["DS_ESPECIE_RECEITA"].to_list(),
                "situacao_receita": n * [np.nan],
                "descricao_receita": df["DS_RECEITA"].to_list(),
                "valor_receita": [
                    float(k.replace(",", ".")) if isinstance(k, str) else k
                    for k in df["VR_RECEITA"].to_list()
                ],
                "sequencial_candidato_doador": [
                    str(k).replace(".0", "") if str(k)[0].isdigit() else k
                    for k in df["SQ_CANDIDATO_DOADOR"].to_list()
                ],
                "cpf_cnpj_doador": [
                    clean_digit_id(k, n_digits=14) if pd.notna(k) else k
                    for k in df["NR_CPF_CNPJ_DOADOR"].to_list()
                ],
                "sigla_uf_doador": df["SG_UF_DOADOR"].to_list(),
                "id_municipio_tse_doador": df["CD_MUNICIPIO_DOADOR"].to_list(),
                "nome_doador": [
                    unidecode(k.title()) if isinstance(k, str) else k
                    for k in df["NM_DOADOR"].to_list()
                ],
                "nome_doador_rf": [
                    unidecode(k.title()) if isinstance(k, str) else k
                    for k in df["NM_DOADOR_RFB"].to_list()
                ],
                "cargo_candidato_doador": df["DS_CARGO_CANDIDATO_DOADOR"].to_list(),
                "numero_partido_doador": df["NR_PARTIDO_DOADOR"].to_list(),
                "sigla_partido_doador": df["SG_PARTIDO_DOADOR"].to_list(),
                "nome_partido_doador": df["SG_PARTIDO_DOADOR"].to_list(),
                "esfera_partidaria_doador": df["DS_ESFERA_PARTIDARIA_DOADOR"].to_list(),
                "numero_candidato_doador": df["NR_CANDIDATO_DOADOR"].to_list(),
                "cnae_2_doador": df["CD_CNAE_DOADOR"].to_list(),
                "descricao_cnae_2_doador": df["DS_CNAE_DOADOR"].to_list(),
                "cpf_cnpj_doador_orig": n * [np.nan],
                "nome_doador_orig": n * [np.nan],
                "nome_doador_orig_rf": n * [np.nan],
                "tipo_doador_orig": n * [np.nan],
                "descricao_cnae_2_doador_orig": n * [np.nan],
                "nome_administrador": n * [np.nan],
                "cpf_administrador": n * [np.nan],
                "numero_recibo_eleitoral": n * [np.nan],
                "numero_documento": n * [np.nan],
                "numero_recibo_doacao": df["NR_RECIBO_DOACAO"].to_list(),
                "numero_documento_doacao": df["NR_DOCUMENTO_DOACAO"].to_list(),
                "tipo_prestacao_contas": [
                    unidecode(k.title()) if isinstance(k, str) else k
                    for k in df["TP_PRESTACAO_CONTAS"].to_list()
                ],
                "data_prestacao_contas": pd.to_datetime(
                    df["DT_PRESTACAO_CONTAS"], format="%d/%m/%Y"
                ).to_list(),
                "sequencial_prestador_contas": df["SQ_PRESTADOR_CONTAS"].to_list(),
                "cnpj_prestador_contas": df["NR_CNPJ_PRESTADOR_CONTA"].to_list(),
                "entrega_conjunto": n * [np.nan],
            }
        )

        table["tipo_eleicao"] = table["tipo_eleicao"].replace(
            {"ordinaria": "eleicao ordinaria"}
        )
        table.replace("#NULO#", np.nan, inplace=True)
        table.replace("#Nulo#", np.nan, inplace=True)
        table.replace("#nulo#", np.nan, inplace=True)
        table.replace(-1, np.nan, inplace=True)

        if table.shape[0] == 0:
            continue
        os.system(f"mkdir -p /tmp/data/output/ano=2022/sigla_uf={uf}/")
        table.drop_duplicates(inplace=True)
        table.drop("ano", axis=1, inplace=True)
        table.drop("sigla_uf", axis=1, inplace=True)
        table.to_csv(
            f"/tmp/data/output/ano=2022/sigla_uf={uf}/receitas_candidato.csv",
            index=False,
        )

    os.system("rm -rf /tmp/data/output/ano=2022/sigla_uf=BR*")
    return "/tmp/data/output/"
