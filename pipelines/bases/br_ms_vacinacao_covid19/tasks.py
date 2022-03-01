"""
Tasks for br_ibge_ipca
"""

from prefect import task
from bs4 import BeautifulSoup
import re
import pandas as pd
from traceback import print_exc
import time
import concurrent.futures
import datetime
import requests
import shutil
from itertools import chain
from tqdm import tqdm


@task
def download_ufs(ufs: list, method="multiprocess") -> None:
    """
    Uses _download_raw to download all partitions from a given list of ufs
    uf: acronym of a Brazilian state
    method: download method. Either multiprocess, multithreading or synchronous
    """
    ufs = [uf.upper() for uf in ufs]

    # nested dictionary where the upper level keys are the level of aggregation used in the available link. Links information inside the inner dictionary
    partition_info = {
        "partition": {
            "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/301983f2-aa50-4977-8fec-cfab0806cb0b",
            "pattern": r"Dados\sCompletos\s-\sParte\s\d+",
        },
        "AC-MT": {
            "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/5093679f-12c3-4d6b-b7bd-07694de54173?inner_span=True",
            "pattern": r"Dados\s[A-Z]+\s-\sParte\s\d+",
        },
        "PA-TO": {
            "url": "https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/10aed154-04c8-4cf4-b78a-8f0fa1bc5af4?inner_span=True",
            "pattern": r"Dados\s[A-Z]+\s-\sParte\s\d+",
        },
    }

    ac_mt = [
        list(d.keys())[0]
        for d in _url_scrapper(
            partition_info["AC-MT"]["url"], partition_info["AC-MT"]["pattern"]
        )
    ]

    pa_to = [
        list(d.keys())[0]
        for d in _url_scrapper(
            partition_info["PA-TO"]["url"], partition_info["PA-TO"]["pattern"]
        )
    ]

    siglas_pa_to = set([re.search(r"[A-Z]{2}", name).group() for name in pa_to])
    siglas_ac_mt = set([re.search(r"[A-Z]{2}", name).group() for name in ac_mt])

    partitions_1 = []
    partitions_2 = []
    for uf in ufs:
        if uf in siglas_ac_mt:
            partitions_1.append(list(filter(lambda x: x.__contains__(uf), ac_mt)))
        elif uf in siglas_pa_to:
            partitions_2.append(list(filter(lambda x: x.__contains__(uf), pa_to)))
        else:
            raise ValueError(
                f"""The uf must correspond to am acronym of a Brazilian federation. '{uf}' was found
            If you type correctly, please check the MS website to see if the selected uf has available data for download.
            """
            )

    partitions_1 = list(chain(*partitions_1))
    partitions_2 = list(chain(*partitions_2))

    if method == "multiprocess":
        if len(ufs) > 1:
            print(
                f"Started the download of {', '.join(ufs[:-1])} and {ufs[-1]} partitions"
            )
        else:
            print(f"Started the download of {ufs[0]}'s partitions")
        start = time.perf_counter()

        for group, partitions in zip(["AC-MT", "PA-TO"], [partitions_1, partitions_2]):
            _asyncronous_download(
                group=group,
                partitions=partitions,
                partition_info=partition_info,
                method="multiprocess",
            )

        end = time.perf_counter()
        total_time = end - start
        print("Total download time: " + str(datetime.timedelta(seconds=total_time)))
    elif method == "multithreading":
        if len(ufs) > 1:
            print(
                f"Started the download of {', '.join(ufs[:-1])} and {ufs[-1]} partitions"
            )
        else:
            print(f"Started the download of {ufs[0]}'s partitions")
        start = time.perf_counter()

        for group, partitions in zip(["AC-MT", "PA-TO"], [partitions_1, partitions_2]):
            _asyncronous_download(
                group=group,
                partitions=partitions,
                partition_info=partition_info,
                method="multiprocess",
            )

        end = time.perf_counter()
        total_time = end - start
        print("Total download time: " + str(datetime.timedelta(seconds=total_time)))
    elif method == "synchronous":
        for uf in ufs:
            print(f"Started the download of {uf}'s partitions")
            start = time.perf_counter()
            for partition in partitions:
                _download_raw(group, partition, partition_info)
            end = time.perf_counter()
        total_time = end - start
        print("Total download time: " + str(datetime.timedelta(seconds=total_time)))
    else:
        raise ValueError(
            "method option should be either multiprocess, multithreading or synchronous"
        )


@task
def build_data(ufs: list, municipio: pd.DataFrame, table: str) -> str:
    """
    Iterate over an array of uf's csv files to create the selected table. Returns a filepath
    """
    for uf in tqdm(ufs):
        filename = "/tmp/data/br_ms_vacinacao_covid19/input/" + uf + ".csv"
        print("Using raw files of {}.".format(uf))
        chunksize = 10 ** 4
        n_chunk = 1
        write_header = True
        for chunk in pd.read_csv(filename, sep=";", dtype=object, chunksize=chunksize):
            chunk.fillna("")
            if table == "microdados":
                chunk_microdados = _clean_microdados(uf, chunk, municipio, n_chunk)
                filepath = (
                    f"/tmp/data/br_ms_vacinacao_covid19/output/microdados/{uf}.csv"
                )
                chunk_microdados.to_csv(
                    filepath, mode="a", header=write_header, index=False
                )
            elif table == "vacinacao":
                chunk_vacinacao = _clean_vacinacao(uf, chunk, n_chunk)
                filepath = f"/tmp/data/br_ms_vacinacao_covid19/output/microdados_vacinacao/{uf}.csv"
                chunk_vacinacao.to_csv(
                    filepath, mode="a", header=write_header, index=False
                )
            elif table == "paciente":
                chunk_paciente = _clean_paciente(uf, chunk, municipio, n_chunk)
                filepath = f"/tmp/data/br_ms_vacinacao_covid19/output/microdados_paciente/{uf}.csv"
                chunk_paciente.to_csv(
                    filepath, mode="a", header=write_header, index=False
                )
            elif table == "estabelecimento":
                chunk_estabelecimento = _clean_estabelecimento(
                    uf, chunk, municipio, n_chunk
                )
                filepath = f"/tmp/data/br_ms_vacinacao_covid19/output/microdados_estabelecimento/{uf}.csv"
                chunk_estabelecimento.to_csv(
                    filepath, mode="a", header=write_header, index=False
                )
            else:
                raise ValueError(
                    "table must be either microdados, vacinacao, paciente or estabelecimento"
                )

            write_header = False
            n_chunk = n_chunk + 1


def _url_scrapper(url: str, pattern: str) -> list:
    """
    Get list of dictionaries where the values are the urls and the keys are the url's identifiers.
    url: source where the urls should be grabbed
    pattern: identifier's regex pattern
    """
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    all_links = soup.find_all("a")
    links = []
    for i in range(len(all_links)):
        text = all_links[i].text.strip()
        if bool(re.match(pattern, text)):
            link = all_links[i].attrs["href"]
            links.append({text: link})

    return links


def _asyncronous_download(
    group: str, partitions: str, partition_info: dict, method: str
) -> None:
    """
    Run _download_raw asyncronously using threadings or process
    """
    if method == "multiprocess":
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = [
                executor.submit(_download_raw, group, partition, partition_info)
                for partition in partitions
            ]
            for f in concurrent.futures.as_completed(results):
                print(f.result())
    elif method == "multithreading":
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [
                executor.submit(_download_raw, group, partition, partition_info)
                for partition in partitions
            ]
            for f in concurrent.futures.as_completed(results):
                print(f.result())
    else:
        raise ValueError(
            "method option should be either multiprocess or multithreading"
        )


def _download_raw(group: str, partition: str, partition_info: dict) -> str:
    """
    Download raw files from MS website.
    group: group of files as defined in MS website (e.g., PA-TO is the group of files containing data from the states of PA to TO, ordered alphabetically)
    partition: identification of a particular file within the group. Also, accordingly to MS website.
    partition_info: dictionary containing information of all partitions (group's names, urls, and regex pattern)
    """
    partitions = _url_scrapper(
        url=partition_info[group]["url"], pattern=partition_info[group]["pattern"]
    )

    try:
        url = {
            list(d.keys())[0]: list(d.values())[0]
            for d in partitions
            if list(d.keys())[0] == partition
        }[partition]
        filename = partition.replace(" ", "_").replace("_-_", "_").lower()
        path = "/tmp/data/br_ms_vacinacao_covid19/input/" + filename + ".csv"
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(path, "wb") as f:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, f)
        return f"The {partition} raw files was downloaded."
    except Exception as e:
        print("The following error occurred:\n")
        print(e.__class__.__name__)
        print_exc()


def _clean_microdados(uf, df, munic, n_chunk) -> pd.DataFrame:
    """
    Clean microdados by chunks
    """
    df = df[
        [
            "document_id",
            "paciente_id",
            "paciente_idade",
            "paciente_dataNascimento",
            "paciente_enumSexoBiologico",
            "paciente_racaCor_codigo",
            "paciente_endereco_coIbgeMunicipio",
            "paciente_endereco_coPais",
            "paciente_endereco_cep",
            "paciente_nacionalidade_enumNacionalidade",
            "estabelecimento_valor",
            "estabelecimento_razaoSocial",
            "estalecimento_noFantasia",
            "estabelecimento_municipio_codigo",
            "vacina_grupoAtendimento_codigo",
            "vacina_categoria_codigo",
            "vacina_lote",
            "vacina_fabricante_nome",
            "vacina_fabricante_referencia",
            "vacina_dataAplicacao",
            "vacina_descricao_dose",
            "vacina_codigo",
            "sistema_origem",
        ]
    ]

    df.columns = [
        "id_documento",
        "id_paciente",
        "idade_paciente",
        "data_nascimento_paciente",
        "sexo_paciente",
        "raca_cor_paciente",
        "id_municipio_6_endereco_paciente",
        "pais_endereco_paciente",
        "cep_endereco_paciente",
        "nacionalidade_paciente",
        "id_estabelecimento",
        "razao_social_estabelecimento",
        "nome_fantasia_estabelecimento",
        "id_municipio_6_estabelecimento",
        "grupo_atendimento_vacina",
        "categoria_vacina",
        "lote_vacina",
        "nome_fabricante_vacina",
        "referencia_fabricante_vacina",
        "data_aplicacao_vacina",
        "dose_vacina",
        "codigo_vacina",
        "sistema_origem",
    ]

    # -----------------#
    # paciente
    # -----------------#

    # print(df[df['raca_cor'].isnull()]) # https://stackoverflow.com/questions/47333227/pandas-valueerror-cannot-convert-float-nan-to-integer
    df = df.dropna(
        subset=["raca_cor_paciente"]
    )  # dropping the few observations with null information

    df["raca_cor_paciente"] = df["raca_cor_paciente"].astype("int64")
    df["raca_cor_paciente"] = df["raca_cor_paciente"].astype("string")

    df = df.merge(
        munic[["id_municipio", "id_municipio_6"]],
        left_on="id_municipio_6_endereco_paciente",
        right_on="id_municipio_6",
    )
    df = df.rename(columns={"id_municipio": "id_municipio_endereco_paciente"})

    # -----------------#
    # estabelecimento
    # -----------------#

    df = df.merge(
        munic[["id_municipio", "id_municipio_6"]],
        left_on="id_municipio_6_estabelecimento",
        right_on="id_municipio_6",
    )
    df = df.rename(columns={"id_municipio": "id_municipio_estabelecimento"})

    # -----------------#
    # vacinação
    # -----------------#

    df["grupo_atendimento_vacina"] = (
        pd.to_numeric(df["grupo_atendimento_vacina"], errors="coerce")
        .astype("string")
        .replace(["0"], "")
    )

    ######transforma caracter especial da variável
    df.loc[(df["dose_vacina"] == "1ª Dose Revacinação "), "dose_vacina"] = "1a Dose"
    df.loc[(df["dose_vacina"] == "1ª Dose"), "dose_vacina"] = "1a Dose"
    df.loc[(df["dose_vacina"] == "1Âª Dose"), "dose_vacina"] = "1a Dose"
    df.loc[(df["dose_vacina"] == "1º Reforço "), "dose_vacina"] = "1o Reforço"
    df.loc[(df["dose_vacina"] == "2ª Dose"), "dose_vacina"] = "2a Dose"
    df.loc[(df["dose_vacina"] == "2Âª Dose"), "dose_vacina"] = "2a Dose"
    df.loc[
        (df["dose_vacina"] == "2ª Dose Revacinação "), "dose_vacina"
    ] = "2a Dose Revacinação"
    df.loc[(df["dose_vacina"] == "Dose "), "dose_vacina"] = "Dose Única"
    df.loc[(df["dose_vacina"] == "Dose"), "dose_vacina"] = "Dose Única"
    df.loc[(df["dose_vacina"] == "3ª Dose"), "dose_vacina"] = "3a Dose"
    df.loc[(df["dose_vacina"] == "Única "), "dose_vacina"] = "Dose Única"
    df.loc[(df["dose_vacina"] == "Dose Adicional "), "dose_vacina"] = "Dose Adicional"
    df.loc[(df["dose_vacina"] == "Dose Inicial "), "dose_vacina"] = "Dose Inicial"
    df.loc[(df["dose_vacina"] == "ReforÃ§o"), "dose_vacina"] = "Dose Reforço"

    df["data_aplicacao_vacina"] = df["data_aplicacao_vacina"].str[:11]

    # df['horario_importacao_rnds'] = df['data_importacao_rnds'].str[11:19]
    # df['data_importacao_rnds']    = df['data_importacao_rnds'].str[:10]

    df = df[
        [
            "id_documento",
            "id_paciente",
            "idade_paciente",
            "data_nascimento_paciente",
            "sexo_paciente",
            "raca_cor_paciente",
            "id_municipio_endereco_paciente",
            "pais_endereco_paciente",
            "cep_endereco_paciente",
            "nacionalidade_paciente",
            "id_estabelecimento",
            "razao_social_estabelecimento",
            "nome_fantasia_estabelecimento",
            "id_municipio_estabelecimento",
            "grupo_atendimento_vacina",
            "categoria_vacina",
            "lote_vacina",
            "nome_fabricante_vacina",
            "referencia_fabricante_vacina",
            "data_aplicacao_vacina",
            "dose_vacina",
            "codigo_vacina",
            "sistema_origem",
        ]
    ]

    return df


def _clean_vacinacao(uf, df, n_chunk) -> pd.DataFrame:
    """
    Clean microdados by chunk and create vacinacao table
    """
    df = df[
        [
            "document_id",
            "paciente_id",
            "estabelecimento_valor",
            "vacina_grupoAtendimento_codigo",
            "vacina_categoria_codigo",
            "vacina_lote",
            "vacina_fabricante_nome",
            "vacina_fabricante_referencia",
            "vacina_dataAplicacao",
            "vacina_descricao_dose",
            "vacina_codigo",
            "sistema_origem",
        ]
    ]

    df.columns = [
        "id_documento",
        "id_paciente",
        "id_estabelecimento",
        "grupo_atendimento",
        "categoria",
        "lote",
        "nome_fabricante",
        "referencia_fabricante",
        "data_aplicacao",
        "dose",
        "vacina",
        "sistema_origem",
    ]

    df["grupo_atendimento"] = (
        pd.to_numeric(df["grupo_atendimento"], errors="coerce")
        .astype("string")
        .replace(["0"], "")
    )

    df["data_aplicacao"] = df["data_aplicacao"].str[:11]

    ######transforma caracter especial da variável
    df.loc[(df["dose"] == "1ª Dose Revacinação "), "dose"] = "1a Dose"
    df.loc[(df["dose"] == "1ª Dose"), "dose"] = "1a Dose"
    df.loc[(df["dose"] == "1Âª Dose"), "dose"] = "1a Dose"
    df.loc[(df["dose"] == "1º Reforço "), "dose"] = "1o Reforço"
    df.loc[(df["dose"] == "2ª Dose"), "dose"] = "2a Dose"
    df.loc[(df["dose"] == "2Âª Dose"), "dose"] = "2a Dose"
    df.loc[(df["dose"] == "2ª Dose Revacinação "), "dose"] = "2a Dose Revacinação"
    df.loc[(df["dose"] == "Dose "), "dose"] = "Dose Única"
    df.loc[(df["dose"] == "Dose"), "dose"] = "Dose Única"
    df.loc[(df["dose"] == "3ª Dose"), "dose"] = "3a Dose"
    df.loc[(df["dose"] == "Única "), "dose"] = "Dose Única"
    df.loc[(df["dose"] == "Dose Adicional "), "dose"] = "Dose Adicional"
    df.loc[(df["dose"] == "Dose Inicial "), "dose"] = "Dose Inicial"
    df.loc[(df["dose"] == "ReforÃ§o"), "dose"] = "Dose Reforço"

    df = df[
        [
            "id_documento",
            "id_paciente",
            "id_estabelecimento",
            "grupo_atendimento",
            "categoria",
            "lote",
            "nome_fabricante",
            "referencia_fabricante",
            "data_aplicacao",
            "dose",
            "vacina",
            "sistema_origem",
        ]
    ]

    return df


def _clean_paciente(uf, df, munic, n_chunk) -> pd.DataFrame:
    """
    Clean microdados by chunk and create paciente table
    """
    df = df[
        [
            "paciente_id",
            "paciente_idade",
            "paciente_dataNascimento",
            "paciente_enumSexoBiologico",
            "paciente_racaCor_codigo",
            "paciente_endereco_coIbgeMunicipio",
            "paciente_endereco_coPais",
            "paciente_endereco_cep",
            "paciente_nacionalidade_enumNacionalidade",
        ]
    ]

    df = df.drop_duplicates()

    df.columns = [
        "id_paciente",
        "idade",
        "data_nascimento",
        "sexo",
        "raca_cor",
        "id_municipio_6",
        "pais_endereco",
        "cep_endereco",
        "nacionalidade",
    ]

    # print(df[df['raca_cor'].isnull()]) # https://stackoverflow.com/questions/47333227/pandas-valueerror-cannot-convert-float-nan-to-integer
    df = df.dropna(
        subset=["raca_cor"]
    )  # dropping the few observations with null information

    df["raca_cor"] = (
        pd.to_numeric(df["raca_cor"], errors="coerce").astype("int64").astype("string")
    )

    df = df.merge(munic[["id_municipio", "id_municipio_6"]], on="id_municipio_6")
    df = df.rename(columns={"id_municipio": "id_municipio_endereco"})

    df = df[
        [
            "id_paciente",
            "idade",
            "data_nascimento",
            "sexo",
            "raca_cor",
            "id_municipio_endereco",
            "pais_endereco",
            "cep_endereco",
            "nacionalidade",
        ]
    ]

    return df


def _clean_estabelecimento(uf, df, munic, n_chunk) -> pd.DataFrame:
    """
    Clean microdados by chunk and aggregate microdados into an estabelecimento table
    """
    df = df[
        [
            "estabelecimento_valor",
            "estabelecimento_razaoSocial",
            "estalecimento_noFantasia",
            "estabelecimento_municipio_codigo",
        ]
    ]

    df = df.drop_duplicates()

    df.columns = [
        "id_estabelecimento",
        "razao_social",
        "nome_fantasia",
        "id_municipio_6",
    ]

    df = df.merge(munic[["id_municipio", "id_municipio_6"]], on="id_municipio_6")

    df = df[["id_municipio", "id_estabelecimento", "razao_social", "nome_fantasia"]]

    return df
