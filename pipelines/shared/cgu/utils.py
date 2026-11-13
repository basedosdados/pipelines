"""
General purpose functions for the br_cgu_cartao_pagamento project
"""

import datetime
import gc
import os
import shutil
import time
from functools import lru_cache

import basedosdados as bd
import pandas as pd
import requests
import unidecode
from dateutil.relativedelta import relativedelta
from rapidfuzz import process

from pipelines.shared.cgu.constants import (
    _TEMPORARY_STATUS_CODES,
    BENEFICIOS_CIDADAO_DATASET_ID,
    BROWSERS_USER_AGENT,
    LICITACAO_CONTRATO_DATASET_ID,
    MAX_MONTH_RANGE,
    SERVIDORES_DATASET_ID,
    TABELAS,
    SourceStatus,
)
from pipelines.utils.apply_architecture_to_dataframe.utils import (
    read_architecture_table,
    rename_columns,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url
from pipelines.utils.utils import log


def build_urls(
    dataset_id: str,
    url: str,
    year: int,
    month: int,
    table_id: str,
    # pyrefly: ignore [bad-return]
) -> str | list:
    """
    Constructs URLs based on the provided parameters.

    Args:
        modelo (str): The model type which determines the URL structure.
        url (str): The base URL to which the year, month, and table name will be appended.
        year (int): The year to be included in the URL.
        month (int): The month to be included in the URL.
        table_id (str): The table identifier used to fetch specific table names for constructing URLs.

    Returns:
        str: A single URL string if the model is 'TABELA'.
        list: A list of URL strings if the model is 'TABELA_SERVIDORES'.
    """

    if dataset_id in [
        "br_cgu_cartao_pagamento",
        LICITACAO_CONTRATO_DATASET_ID,
        BENEFICIOS_CIDADAO_DATASET_ID,
    ]:
        log(f"URL -> {url}{year}{str(month).zfill(2)}/")

        return f"{url}{year}{str(month).zfill(2)}/"

    elif dataset_id == SERVIDORES_DATASET_ID:
        list_url = []
        for table_name in TABELAS[dataset_id][table_id]["READ"]:
            url_completa = f"{url}{year}{str(month).zfill(2)}_{table_name}/"
            log(f"URL -> {url_completa}")
            list_url.append(url_completa)
        return list_url


def build_input(table_id):
    """
    Builds a list of input directories based on the given table ID.

    This function retrieves the input keys from the TABELAS['br_cgu_servidores_executivo_federal']
    dictionary for the specified table_id. It then checks if each input directory
    exists, creates it if it does not, and appends the directory path to a list.
    Finally, it logs the list of input directories and returns it.

    Args:
        table_id (str): The ID of the table for which to build the input directories.

    Returns:
        list: A list of input directory paths.

    Raises:
        KeyError: If the table_id is not found.
    """
    list_input = []
    for input in TABELAS[SERVIDORES_DATASET_ID][table_id]["READ"]:
        value_input = f"{input}"
        if not os.path.exists(value_input):
            os.makedirs(value_input)
            log(value_input)
        list_input.append(value_input)
    return list_input


# Função para carregar o dataframe
@lru_cache(maxsize=1)  # Cache para evitar recarregar a tabela
def load_municipio() -> None:
    municipio: pd.DataFrame = bd.read_table(
        "br_bd_diretorios_brasil",
        "municipio",
        billing_project_id="basedosdados",
        from_file=True,
    )
    municipio["cidade_uf"] = (
        municipio["nome"].apply(lambda x: x.upper())
        + "-"
        + municipio["sigla_uf"]
    )
    # pyrefly: ignore [bad-return]
    return municipio


def get_similar_cities_process(city):
    municipio = load_municipio()
    # pyrefly: ignore [unsupported-operation]
    results = process.extractOne(city, municipio["cidade_uf"], score_cutoff=70)
    return results[0] if results else None


def read_csv(
    dataset_id: str,
    table_id: str,
    column_replace: list[str] | None,
    # pyrefly: ignore [bad-return]
) -> pd.DataFrame:
    """
    Reads a CSV file from a specified path and processes its columns.

    Args:
        table_id (str): The identifier for the table to be read.
        url (str): The URL from which the CSV file is to be read.
        column_replace (List, optional): A list of column names whose values need to be replaced. Default is ['VALOR_TRANSACAO'].

    Returns:
        pd.DataFrame: A DataFrame containing the processed data from the CSV file.

    Notes:
        - The function reads the CSV file from a directory specified in a constants file.
        - It assumes the CSV file is encoded in 'latin1' and uses ';' as the separator.
        - Column names are converted to uppercase, spaces are replaced with underscores, and accents are removed.
        - For columns specified in `column_replace`, commas in their values are replaced  with dots and the values are converted to float.
    """

    column_replace = (
        ["VALOR_TRANSACAO"] if column_replace is None else column_replace
    )

    value_constants = TABELAS[dataset_id][table_id]
    # pyrefly: ignore [no-matching-overload]
    log(os.listdir(value_constants["INPUT"]))
    if dataset_id == "br_cgu_cartao_pagamento":
        csv_file = next(
            f
            # pyrefly: ignore [no-matching-overload]
            for f in os.listdir(value_constants["INPUT"])
            if f.endswith(".csv")
        )
        log(f"CSV files: {csv_file}")

        df = pd.read_csv(
            f"{value_constants['INPUT']}/{csv_file}",
            sep=";",
            encoding="latin1",
        )
        df.columns = [
            unidecode.unidecode(x).upper().replace(" ", "_")
            for x in df.columns
        ]

        for list_column_replace in column_replace:
            df[list_column_replace] = (
                df[list_column_replace].str.replace(",", ".").astype(float)
            )

        return df

    if dataset_id == LICITACAO_CONTRATO_DATASET_ID:
        csv_file = [  # noqa: RUF015
            f
            # pyrefly: ignore [no-matching-overload]
            for f in os.listdir(value_constants["INPUT"])
            if f.endswith(value_constants["READ"])
        ][0]
        log(f"CSV files: {csv_file}")
        log(f"{value_constants['INPUT']}/{csv_file}")
        df = pd.read_csv(
            f"{value_constants['INPUT']}/{csv_file}",
            sep=";",
            encoding="latin1",
        )
        df["ano"] = csv_file[:4]
        df["mes"] = csv_file[4:6]

        df.columns = [unidecode.unidecode(col) for col in df.columns]
        df.columns = [col.replace(" ", "_").lower() for col in df.columns]

        if table_id == "licitacao":
            df["cidade_uf"] = df["municipio"] + "-" + df["uf"]

            df["cidade_uf_dir"] = df["cidade_uf"].apply(
                lambda x: get_similar_cities_process(x)
            )
            df = df.drop(["cidade_uf", "municipio"], axis=1)

            df = df.rename(columns={"cidade_uf_dir": "municipio"})

            df["municipio"] = df["municipio"].apply(
                lambda x: x if x is None else x.split("-")[0]
            )

        return df


def last_date_in_metadata(dataset_id: str, table_id: str) -> datetime.date:
    """
    Retorna a data mais recente de cobertura da tabela no backend de produção.

    Args:
        dataset_id (str): The ID of the dataset to query.
        table_id (str): The ID of the table within the dataset to query.

    Returns:
        last_date_in_api (datetime.date): The most recent date found in the API.
    """

    backend = bd.Backend(graphql_url=get_url("prod"))
    last_date_in_api = get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format="%Y-%m",
        backend=backend,
    )
    return last_date_in_api


def most_recent_available_date(
    dataset_id: str,
    table_id: str,
    relative_month: int,
    last_date_in_api: datetime.date,
) -> datetime.date:
    """Encontra o mês mais recente já publicado na fonte.

    Parte de `last_date_in_api + relative_month` e avança de `relative_month`
    em `relative_month` enquanto o mês estiver disponível (todas as URLs
    em `SourceStatus.AVAILABLE`), até `MAX_MONTH_RANGE` passos ou a data de
    hoje. O que encerra a busca depende do status do primeiro mês que não
    está disponível:

    - `NOT_PUBLISHED`: o mês não existe na fonte. Para, e devolve o último mês
      disponível (ou `last_date_in_api`, se nenhum).
    - `TEMPORARY` (405, 202 esgotado, 5xx, erro de rede): a fonte pode ter o
      mês, só não respondeu. Para sem avançar e devolve esse mês, para o
      `extract_and_load` tentar baixá-lo e falhar com o erro real, em vez de
      pular para um mês que não existe.

    A disponibilidade é "tudo ou nada": basta uma URL do mês não estar
    disponível para o mês não valer.

    Args:
        dataset_id: ID do dataset.
        table_id: ID da tabela.
        relative_month: Passo, em meses, entre períodos consecutivos.
        last_date_in_api: Última data já presente no backend.

    Returns:
        O mês a carregar: o mais recente disponível, o mês com falha
        temporária, ou `last_date_in_api` se nenhum mês novo estiver
        publicado.
    """

    next_date_in_api = last_date_in_api + relativedelta(months=relative_month)
    latest = last_date_in_api
    value_constants = TABELAS[dataset_id][table_id]
    _step = 0
    while (
        next_date_in_api <= datetime.datetime.now().date()
        and _step < MAX_MONTH_RANGE
    ):
        urls = build_urls(
            # pyrefly: ignore [bad-argument-type]
            url=value_constants["URL"],
            year=next_date_in_api.year,
            month=next_date_in_api.month,
            table_id=table_id,
            dataset_id=dataset_id,
        )
        log(f"Last date in API: {last_date_in_api}")
        log(f"Checking date: {next_date_in_api}")
        status = _month_status(urls)
        log(f"Status: {status}")
        if status is SourceStatus.AVAILABLE:
            latest = next_date_in_api
            next_date_in_api = next_date_in_api + relativedelta(
                months=relative_month
            )
            _step += 1
        elif status is SourceStatus.TEMPORARY:
            return next_date_in_api
        else:
            break
    return latest


def _month_status(urls: str | list[str]) -> SourceStatus:
    """Status de um mês inteiro: pior status entre as URLs dele.

    `NOT_PUBLISHED` vence `TEMPORARY`, que vence `AVAILABLE`. Para na primeira
    URL `NOT_PUBLISHED`, sem consultar as demais.
    """
    statuses: list[SourceStatus] = []
    for url in [urls] if isinstance(urls, str) else urls:
        status = source_url_is_available(url)
        if status is SourceStatus.NOT_PUBLISHED:
            return status
        statuses.append(status)
    if SourceStatus.TEMPORARY in statuses:
        return SourceStatus.TEMPORARY
    return SourceStatus.AVAILABLE


def create_column_ano(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """
    Adds a new column 'ano' to the DataFrame based on the first four characters of the csv_path.

    Parameters:
    df (pd.DataFrame): The input DataFrame to which the new column will be added.
    csv_path (str): The file path string from which the year will be extracted.

    Returns:
    pd.DataFrame: The DataFrame with the added 'ano' column.
    """
    df["ano"] = int(csv_path[:4])
    return df


def create_column_month(df: pd.DataFrame, csv_path: str) -> str:
    """
    Adds a 'mes' column to the DataFrame based on the month extracted from the csv_path.

    Args:
        df (pd.DataFrame): The input DataFrame to which the 'mes' column will be added.
        csv_path (str): The file path string from which the month will be extracted.
                        It is assumed that the month is represented by characters at positions 4 and 5.

    Returns:
        pd.DataFrame: The DataFrame with the added 'mes' column.
    """
    df["mes"] = int(csv_path[4:6])

    # pyrefly: ignore [bad-return]
    return df


def exclude_used_tables(path: str) -> None:
    """
    Deletes the directory at the specified path if it exists and performs garbage collection.

    Args:
        path (str): The path to the directory to be deleted.

    Returns:
        None
    """
    if os.path.exists(path):
        shutil.rmtree(path)
    gc.collect()

    return None


def read_and_clean_csv(table_id: str) -> pd.DataFrame:
    """
    Reads and cleans CSV files for a given table ID.

    This function performs the following steps:
    1. Builds the input path for the given table ID.
    2. Iterates through the CSV files in the input path.
    3. Reads each CSV file into a DataFrame.
    4. Renames columns based on a predefined architecture.
    5. Adds 'ano' and 'month' columns based on the CSV file name.
    6. Adds an 'origem' column if it exists in the architecture.
    7. Appends the cleaned DataFrame to a list.
    8. Concatenates all DataFrames in the list into a single DataFrame.
    9. Returns the concatenated DataFrame.

    Args:
        table_id (str): The ID of the table to process.

    Returns:
        pd.DataFrame: The concatenated and cleaned DataFrame.
    """
    append_dataframe = []
    constants_cgu_servidores = TABELAS[SERVIDORES_DATASET_ID][table_id]
    for csv_path in build_input(table_id):
        path = f"{constants_cgu_servidores['INPUT']}/{csv_path}"
        for get_csv in os.listdir(path):
            if get_csv.endswith(f"{constants_cgu_servidores['NAME_TABLE']}"):
                df = pd.read_csv(
                    os.path.join(path, get_csv),
                    sep=";",
                    encoding="latin-1",
                ).rename(columns=lambda col: col.replace("\x96 ", ""))
                url_architecture = constants_cgu_servidores["ARCHITECTURE"]
                df_architecture = read_architecture_table(url_architecture)
                df = rename_columns(df, df_architecture)

                create_column_ano(df, get_csv)
                create_column_month(df, get_csv)
                if "origem" in df_architecture["name"].to_list():
                    df["origem"] = get_source(table_id, csv_path)

                    append_dataframe.append(df)

                exclude_used_tables(path)

    if len(append_dataframe) > 1:
        df = pd.concat(append_dataframe)

    # pyrefly: ignore [unbound-name]
    return df


def get_source(table_name: str, source: str) -> str:
    origins = {
        "cadastro_aposentados": {
            "Aposentados_BACEN": "BACEN",
            "Aposentados_SIAPE": "SIAPE",
        },
        "cadastro_pensionistas": {
            "Pensionistas_SIAPE": "SIAPE",
            "Pensionistas_DEFESA": "Defesa",
            "Pensionistas_BACEN": "BACEN",
        },
        "cadastro_servidores": {
            "Servidores_BACEN": "BACEN",
            "Servidores_SIAPE": "SIAPE",
            "Militares": "Militares",
        },
        "cadastro_reserva_reforma_militares": {
            "Reserva_Reforma_Militares": "Reserva Reforma Militares"
        },
        "remuneracao": {
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
        "afastamentos": {
            "Servidores_BACEN": "BACEN",
            "Servidores_SIAPE": "SIAPE",
        },
        "observacoes": {
            "Aposentados_BACEN": "Aposentados BACEN",
            "Aposentados_SIAPE": "Aposentados SIAPE",
            "Militares": "Militares",
            "Pensionistas_BACEN": "Pensionistas BACEN",
            "Pensionistas_DEFESA": "Pensionistas DEFESA",
            "Pensionistas_SIAPE": "Pensionistas SIAPE",
            "Reserva_Reforma_Militares": "Reserva Reforma Militares",
            "Servidores_BACEN": "Servidores BACEN",
            "Servidores_SIAPE": "Servidores SIAPE",
        },
    }

    return origins[table_name][source]


def partition_data_beneficios_cidadao(
    table_id: str,
    df,
    coluna1: str,
    coluna2: str,
    counter,
    # pyrefly: ignore [bad-return]
) -> str:
    value_constants = TABELAS[BENEFICIOS_CIDADAO_DATASET_ID][table_id]
    if table_id == "novo_bolsa_familia":
        unique_anos = df[coluna1].unique().tolist()
        unique_meses = df[coluna2].unique().tolist()

        for ano_completencia in unique_anos:
            for mes_completencia in unique_meses:
                path_partition = f"{value_constants['OUTPUT']}/{coluna1}={ano_completencia}/{coluna2}={mes_completencia}"

                if not os.path.exists(path_partition):
                    os.makedirs(path_partition)

                # Mudando o filter do Polars para o loc do Pandas
                df_partition = df.loc[
                    (df[coluna1] == ano_completencia)
                    & (df[coluna2] == mes_completencia)
                ]
                # Usando drop com axis=1 para remover colunas no Pandas
                df_partition = df_partition.drop([coluna1, coluna2], axis=1)

                # Usando to_parquet do Pandas ao invés do write_parquet do Polars
                df_partition.to_parquet(
                    f"{path_partition}/data_{counter}.parquet"
                )

    else:
        unique_meses = df[coluna1].unique().tolist()

        for mes in unique_meses:
            path_partition = f"{value_constants['OUTPUT']}/{coluna1}={mes}/"

            if not os.path.exists(path_partition):
                os.makedirs(path_partition)

            # Mudando o filter do Polars para o loc do Pandas
            df_partition = df.loc[df[coluna1] == mes]
            # Usando drop com axis=1 para remover colunas no Pandas
            df_partition = df_partition.drop([coluna1], axis=1)

            # Usando to_parquet do Pandas ao invés do write_parquet do Polars
            df_partition.to_parquet(f"{path_partition}/data_{counter}.parquet")


def source_url_is_available(
    url: str,
    max_retries: int = 5,
    wait_seconds: int = 30,
) -> SourceStatus:
    """Consulta se o ZIP da fonte (portal CGU) está pronto para download.

    O portal prepara o arquivo de forma assíncrona: responde 202 enquanto gera
    o ZIP e 200 (via redirect) quando pronto. Requisições sem User-Agent de
    navegador são bloqueadas com 405.

    - 200: `AVAILABLE`.
    - 202, 405, 5xx ou erro de rede: repete até `max_retries` vezes; esgotadas,
      `TEMPORARY`. Não diz que o arquivo não existe.
    - Qualquer outro código (ex.: 404): `NOT_PUBLISHED`, sem repetir.

    Args:
        url (str): URL de download do mês no portal da transparência.
        max_retries (int): Tentativas enquanto a resposta for temporária.
        wait_seconds (int): Segundos de espera entre tentativas quando 202.

    Returns:
        SourceStatus: `AVAILABLE`, `NOT_PUBLISHED` ou `TEMPORARY`.
    """
    headers = {"User-Agent": BROWSERS_USER_AGENT["chrome"]}

    for t in range(1, max_retries + 1):
        try:
            with requests.get(
                url=url, headers=headers, stream=True, timeout=30
            ) as response:
                code = response.status_code
        except requests.RequestException as e:
            log(f"Erro de rede em {url}: {e}. Retry {t}", level="warning")
            time.sleep(15)
            continue

        if code == 200:
            time.sleep(5)
            return SourceStatus.AVAILABLE
        if code == 202:
            log(f"preparando ZIP, tentativa {t}")
            time.sleep(wait_seconds)
        elif code in _TEMPORARY_STATUS_CODES or code >= 500:
            log(f"Requisição com status code: {code}. Retry {t}")
            time.sleep(15)
        else:
            log(f"URL não publicada, status code: {code}")
            return SourceStatus.NOT_PUBLISHED

    log(f"Fonte sem resposta definitiva após {max_retries} tentativas: {url}")
    return SourceStatus.TEMPORARY
