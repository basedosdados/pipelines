"""
General purpose functions for the br_rf_cnpj project
"""

import datetime
import os
import zipfile
from asyncio import Semaphore, gather, sleep
from pathlib import Path

import basedosdados as bd
import pandas as pd
import requests
from bs4 import BeautifulSoup
from httpx import AsyncClient, HTTPError
from tqdm import tqdm

from pipelines.crawler.rf_cnpj.constants import constants as constants_cnpj
from pipelines.utils.utils import log

ufs = constants_cnpj.UFS.value
timeout = constants_cnpj.TIMEOUT.value


def data_url(url: str) -> datetime.date:
    """
    Fetches data from a URL, parses the HTML to find the latest folder date, and compares it to today's date.

    Args:
        url (str): The URL to fetch the data from.
        headers (dict): Headers to include in the request.

    Returns:

        tuple[datetime, datetime]: The maximum date found in the folders (max_folder_date) and max last modified date (max_last_modified_date).
    """

    link_data = requests.request(
        method="PROPFIND",
        url=url,
        headers=constants_cnpj.HEADERS.value,
        data=constants_cnpj.XML_BODY.value,
        timeout=30,
    )
    link_data.raise_for_status()

    soup = BeautifulSoup(link_data.text, "html.parser")

    max_last_modified_date = max(
        datetime.datetime.strptime(
            p.find("d:getlastmodified").text, "%a, %d %b %Y %H:%M:%S GMT"
        )
        for p in soup.find_all("d:prop")
        if p.find("d:getlastmodified")
    ).date()

    value = []

    for x in soup.find_all("d:href"):
        urls = x.get_text(strip=True).split("/")[-2]
        if len(urls) == 7:
            value.append(urls)
    max_folder_date = max(value)

    log(
        f"A data máxima extraida da API da Receita Federal que será utilizada para comparar com os metadados da BD: {max_folder_date}"
    )

    log(
        f"A data máxima extraida da API da Receita Federal que será utilizada para gerar partições no Storage: {max_last_modified_date}"
    )

    return max_folder_date, max_last_modified_date


# ! Cria o caminho do output
def build_paths(
    table_id: str, build_input: bool = True, build_output: bool = True
) -> tuple[Path, Path]:
    """
    Constructs the output directory path based on the suffix and collection date.

    Args:
        table_id (str): Table ID or name (lower case).

    Returns:
        tuple[Path,Path]: The constructed paths.
    """

    tmp_dir = Path("tmp")
    input_dir = tmp_dir / "br_rf_cnpj" / "input"
    output_dir = tmp_dir / "br_rf_cnpj" / "output"

    tmp_dir.mkdir(exist_ok=True, parents=True)
    input_dir.mkdir(exist_ok=True, parents=True)
    output_dir.mkdir(exist_ok=True, parents=True)
    input_path = None
    output_path = None
    if build_input:
        input_path = input_dir / table_id
        input_path.mkdir(exist_ok=True, parents=True)
        log("Pasta input construido")
    if build_output:
        output_path = output_dir / table_id
        output_path.mkdir(exist_ok=True, parents=True)
        log("Pasta destino output construido")
    return input_path, output_path


# ! Adiciona zero a esquerda nas colunas
def fill_left_zeros(
    df: datetime.datetime, column, num_digits: int
) -> pd.DataFrame:
    """
    Adds left zeros to the specified column of a DataFrame to meet the required digit count.

    Args:
        df (pd.DataFrame): DataFrame with the target column.
        column (str): Column name to fill with zeros.
        num_digits (int): Total number of digits for the column.

    Returns:
        pd.DataFrame: Updated DataFrame with filled zeros.
    """
    df[column] = df[column].astype(str).str.zfill(num_digits)
    return df


# ! Download assincrono e em chunck`s do zip
def chunk_range(content_length: int, chunk_size: int) -> list[tuple[int, int]]:
    """
    Splits the content length into a list of chunk ranges for downloading. It Calculates
    each chunk range value in bytes.

    Args:
        content_length (int): The total content length.
        chunk_size (int): Size of each chunk.

    Returns:
        List[Tuple[int, int]]: List of start and end byte ranges for each chunk to be used as a header within download_chunk function
    """
    return [
        (i, min(i + chunk_size - 1, content_length - 1))
        for i in range(0, content_length, chunk_size)
    ]


# from https://stackoverflow.com/a/64283770
async def download(
    url,
    chunk_size=15 * 1024 * 1024,
    max_retries=5,
    max_parallel=12,
    timeout=5 * 60,
):
    """
    Downloads a file from a URL asynchronously, splitting it into chunks for parallel downloading.

    Args:
        url (str): The URL of the file to download.
        chunk_size (int): The size of each chunk in bytes (default: 15 MB).
        max_retries (int): Maximum number of retries allowed for each chunk (default: 5).
        max_parallel (int): Maximum number of parallel downloads (default: 5).
        timeout (int): Timeout for each HTTP request, in seconds (default: 5 minutes).

    Returns:
        bytes: The content of the fully downloaded file.

    Raises:
        HTTPError: If the server responds with an error or the download fails.
    """
    async with AsyncClient() as client:
        try:
            request_head = await client.head(url, timeout=timeout)
            request_head.raise_for_status()
            log(request_head.headers["content-length"])
            content_length = int(request_head.headers["content-length"])
            chunk_ranges = chunk_range(content_length, chunk_size)
            total_chunks = len(chunk_ranges)

            log(
                f"Baixando {url} com {content_length} bytes ({content_length / 1e6:.2f} MB). "
                f"Cada chunk terá tamanho de {chunk_size} bytes ({chunk_size / 1e6:.2f} MB). "
                f"Serão feitos {max_parallel} downloads paralelos por vez, com um total de {total_chunks} chunks."
            )

            semaphore = Semaphore(max_parallel)
            progress = {"completed": 0}
            last_logged_progress = {
                "percentage": 0
            }  # Tracks download progress

            tasks = [
                download_chunk(
                    client,
                    url,
                    chunk,
                    max_retries,
                    timeout,
                    semaphore,
                    progress,
                    total_chunks,
                    last_logged_progress,
                )
                for chunk in chunk_ranges
            ]
            return b"".join(await gather(*tasks))

        except HTTPError as e:
            log(f"Requisição mal sucedida: {e}")
            raise e


def print_progress(
    completed: int, total: int, last_logged_progress: dict
) -> None:
    """
    Logs the download progress only when a significant change occurs.

    Args:
        completed (int): Number of chunks completed.
        total (int): Total number of chunks.
        last_logged_progress (dict): A dictionary to store the last logged progress.
    """
    progress_percentage = (completed / total) * 100
    if (
        progress_percentage - last_logged_progress.get("percentage", 0) >= 10
    ):  # Log a progress update every 10%
        last_logged_progress["percentage"] = progress_percentage
        log(
            f"Progresso no download: {completed}/{total} chunks baixados ({progress_percentage:.1f}%)"
        )


async def download_chunk(
    client: AsyncClient,
    url: str,
    chunk_range: tuple[int, int],
    max_retries: int,
    timeout: int,
    semaphore: Semaphore,
    progress: dict,
    total_chunks: int,
    last_logged_progress: dict,
) -> bytes:
    """
    Downloads a specific chunk of a file asynchronously, with retry logic and progress tracking.

    Args:
        client (AsyncClient): HTTP client instance for making requests.
        url (str): The URL of the file to download.
        chunk_range (tuple[int, int]): The byte range (start, end) for the chunk being downloaded.
        max_retries (int): Maximum number of retries allowed for downloading this chunk.
        timeout (int): Timeout for each HTTP request, in seconds.
        semaphore (Semaphore): Semaphore to limit the number of parallel downloads.
        progress (dict): Dictionary to track the number of completed chunks.
        total_chunks (int): Total number of chunks to be downloaded.
        last_logged_progress (dict): Dictionary to track the last logged progress percentage.

    Returns:
        bytes: The content of the downloaded chunk.

    Raises:
        HTTPError: If the download fails after all retry attempts.
    """
    async with semaphore:
        for attempt in range(max_retries):
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
                    "Sec-GPC": "1",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1",
                    "Priority": "u=0, i",
                    "Range": f"bytes={chunk_range[0]}-{chunk_range[1]}",
                }

                response = await client.get(
                    url, headers=headers, timeout=timeout
                )
                response.raise_for_status()

                progress["completed"] += 1
                print_progress(
                    progress["completed"], total_chunks, last_logged_progress
                )

                return response.content
            except HTTPError:
                delay = 2**attempt
                log(
                    f"Falha no download do chunk {chunk_range[0]}-{chunk_range[1]} "
                    f"na tentativa {attempt + 1}. Retentando em {delay} segundos..."
                )
                await sleep(delay)

        raise HTTPError(
            f"Download do chunk {chunk_range[0]}-{chunk_range[1]} falhou após {max_retries} tentativas"
        )


# ! Executa o download do zip file
async def download_unzip_csv(url: str, path: Path | str) -> None:
    """
    Downloads a ZIP file from a URL and extracts its content.

    Args:
        url (str): The URL of the ZIP file.
        path (str): The directory to save and extract the ZIP file.
    """
    log(f"Baixando o arquivo {url}")
    save_path = path / f"{Path(url).stem}.zip"
    content = await download(url)
    with open(save_path, "wb") as fd:
        fd.write(content)

    try:
        with zipfile.ZipFile(save_path) as z:
            z.extractall(path)
        log("Dados extraídos com sucesso!")
    except zipfile.BadZipFile:
        log(f"O arquivo {Path(url).stem} não é um arquivo ZIP válido.")
        raise

    os.remove(save_path)


# ! Salva os dados CSV Estabelecimentos
def process_csv_estabelecimentos(
    input_path: Path | str,
    output_path: Path | str,
    data_coleta: str,
    i: int,
    chunk_size: int = 100000,
) -> None:
    """
    Processes and saves CSV data for establishments, organizing data into partitions by state.

    Args:
        input_path (Path|str): Path to the input data.
        output_path (Path|str): Directory to save processed data.
        data_coleta (str): Data collection date as string.
        i (int): File number or batch index.
        chunk_size (int): Number of rows to process per chunk.
    """
    ordem = constants_cnpj.COLUNAS_ESTABELECIMENTO_ORDEM.value
    colunas = constants_cnpj.COLUNAS_ESTABELECIMENTO.value
    save_folder = Path(output_path) / f"data={data_coleta}"
    save_folder.mkdir(exist_ok=True, parents=True)

    for filepath in Path(input_path).iterdir():
        if "estabele" in filepath.as_posix().lower():
            log(f"Carregando o arquivo: {filepath}")
            for chunk in tqdm(
                pd.read_csv(
                    filepath,
                    encoding="latin1",
                    sep=";",
                    header=None,
                    names=colunas,
                    dtype=str,
                    chunksize=chunk_size,
                ),
                desc="Lendo o arquivo CSV",
            ):
                # Arrumando as colunas datas
                date_cols = [
                    "data_situacao_cadastral",
                    "data_inicio_atividade",
                    "data_situacao_especial",
                ]
                chunk[date_cols] = chunk[date_cols].apply(
                    pd.to_datetime, format="%Y%m%d", errors="coerce"
                )
                chunk[date_cols] = chunk[date_cols].apply(
                    lambda x: x.dt.strftime("%Y-%m-%d")
                )

                # Fills 'cnpj_basico', 'cnpj_ordem' and  'cnpj_dv' with left zeros
                for col, zeros in dict(
                    zip(
                        ["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
                        [8, 4, 2],
                        strict=True,
                    )
                ).items():
                    chunk = fill_left_zeros(chunk, col, zeros)

                # Generating 'cnpj' column
                chunk["cnpj"] = (
                    chunk["cnpj_basico"]
                    + chunk["cnpj_ordem"]
                    + chunk["cnpj_dv"]
                )
                chunk["id_municipio"] = ""
                chunk = chunk.loc[:, ordem]
                for uf in constants_cnpj.UFS.value:
                    df_particao = chunk[chunk["sigla_uf"] == uf].copy()
                    df_particao = df_particao.drop(["sigla_uf"], axis=1)
                    particao_path = save_folder / f"sigla_uf={uf}"
                    particao_path.mkdir(exist_ok=True, parents=True)
                    particao_filename = f"estabelecimentos_{i}.csv"
                    particao_file_path = particao_path / particao_filename

                    mode = "a" if particao_file_path.exists() else "w"
                    df_particao.to_csv(
                        particao_file_path,
                        index=False,
                        encoding="utf-8",
                        mode=mode,
                        header=mode == "w",
                    )
            log(f"Arquivo estabelecimento_{i} salvo")
            os.remove(filepath)


# ! Salva os dados CSV Empresas
def process_csv_empresas(
    input_path: Path | str,
    output_path: Path | str,
    data_coleta: str,
    i: int,
    chunk_size: int = 100000,
) -> None:
    """
    Processes and saves CSV data for companies.

    Args:
        input_path (Path|str): Path to the input data.
        output_path (Path|str): Directory to save processed data.
        data_coleta (str): Data collection date as string.
        i (int): File number or batch index.
        chunk_size (int): Number of rows to process per chunk.
    """
    colunas = constants_cnpj.COLUNAS_EMPRESAS.value
    save_folder = Path(output_path) / f"data={data_coleta}"
    save_folder.mkdir(exist_ok=True, parents=True)
    save_path = save_folder / f"empresas_{i}.csv"
    for filepath in Path(input_path).iterdir():
        if filepath.as_posix().lower().endswith("csv"):
            log(f"Carregando o arquivo: {filepath}")
            with open(save_path, "wb") as fd:
                for chunk in tqdm(
                    pd.read_csv(
                        filepath,
                        encoding="latin1",
                        sep=";",
                        header=None,
                        names=colunas,
                        dtype=str,
                        chunksize=chunk_size,
                    ),
                    desc="Lendo o arquivo CSV",
                ):
                    # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
                    chunk = fill_left_zeros(chunk, "cnpj_basico", 8)
                    # Preenchimento de zeros à esquerda no campo 'natureza_juridica'
                    chunk = fill_left_zeros(chunk, "natureza_juridica", 4)
                    # Convertendo a coluna 'capital_social' para float e mudando o separator

                    chunk["capital_social"] = (
                        chunk["capital_social"]
                        .str.replace(",", ".")
                        .astype(float)
                    )

                    chunk.to_csv(fd, index=False, encoding="utf-8")

            log(f"Arquivo empresas_{i} salvo")
            os.remove(filepath)


# ! Salva os dados CSV Socios
def process_csv_socios(
    input_path: Path | str,
    output_path: Path | str,
    data_coleta: str,
    i: int,
    chunk_size: int = 1000,
) -> None:
    """
    Processes and saves CSV data for socios (partners).

    Args:
        input_path (Path|str): Path to the input data.
        output_path (Path|str): Directory to save processed data.
        data_coleta (str): Data collection date as string.
        i (int): File number or batch index.
        chunk_size (int): Number of rows to process per chunk.

    Returns:
        None
    """
    colunas = constants_cnpj.COLUNAS_SOCIOS.value
    save_folder = Path(output_path) / f"data={data_coleta}"
    save_folder.mkdir(exist_ok=True, parents=True)
    save_path = save_folder / f"socios_{i}.csv"
    for filepath in Path(input_path).iterdir():
        if filepath.as_posix().lower().endswith("csv"):
            log(f"Carregando o arquivo: {filepath}")
            with open(save_path, "wb") as fd:
                for chunk in tqdm(
                    pd.read_csv(
                        filepath,
                        encoding="latin1",
                        sep=";",
                        header=None,
                        names=colunas,
                        dtype=str,
                        chunksize=chunk_size,
                    ),
                    desc="Lendo o arquivo CSV",
                ):
                    # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
                    chunk = fill_left_zeros(chunk, "cnpj_basico", 8)
                    # Retirando valores de cpf's nulos
                    chunk["cpf_representante_legal"] = chunk[
                        "cpf_representante_legal"
                    ].replace("***000000**", "")
                    # Ajustando a coluna data
                    for col in chunk.columns:
                        if col.startswith("data_"):
                            chunk[col] = chunk[col].replace("0", "")
                            chunk[col] = pd.to_datetime(
                                chunk[col], format="%Y%m%d", errors="coerce"
                            )

                    chunk.to_csv(fd, index=False, encoding="utf-8")

            log(f"Arquivo socios_{i} salvo")
            os.remove(filepath)


# ! Salva os dados CSV Simples
def process_csv_simples(
    input_path: Path | str,
    output_path: Path | str,
    data_coleta: str,
    sufixo: str,
    chunk_size: int = 1000,
) -> None:
    """
    Processes and saves CSV data for simples.

    Args:
        input_path (Path|str): Path to the input data.
        output_path (Path|str): Directory to save processed data.
        data_coleta (str): Data collection date as string.
        sufixo (str): Suffix used to construct the output filename.
        chunk_size (int): Number of rows to process per chunk.

    Returns:
        None
    """
    colunas = constants_cnpj.COLUNAS_SIMPLES.value
    save_path = Path(output_path) / f"{sufixo}.csv"
    for filepath in Path(input_path).iterdir():
        if "simples.csv" in filepath.as_posix().lower():
            log(f"Carregando o arquivo: {filepath}")
            with open(save_path, "wb") as fd:
                for chunk in tqdm(
                    pd.read_csv(
                        filepath,
                        encoding="latin1",
                        sep=";",
                        header=None,
                        names=colunas,
                        dtype=str,
                        chunksize=chunk_size,
                    ),
                    desc="Lendo o arquivo CSV",
                ):
                    for col in chunk.columns:
                        if col.startswith("data_"):
                            chunk[col] = chunk[col].replace(
                                {"0": "", "00000000": ""}
                            )
                            chunk[col] = pd.to_datetime(
                                chunk[col], format="%Y%m%d", errors="coerce"
                            )
                    # Preenchimento de zeros à esquerda no campo 'cnpj_basico'
                    chunk = fill_left_zeros(chunk, "cnpj_basico", 8)
                    # Transformando colunas em dummy
                    chunk["opcao_simples"] = chunk["opcao_simples"].replace(
                        {"N": "0", "S": "1"}
                    )
                    chunk["opcao_mei"] = chunk["opcao_mei"].replace(
                        {"N": "0", "S": "1"}
                    )
                    chunk.to_csv(fd, index=False, encoding="utf-8")

            log(f"Arquivo {sufixo} salvo")
            os.remove(filepath)


def get_table_unique_keys(table_id: str, column: str):
    """
    Retrieves unique keys from a specified table and column in the basedosdados.br_rf_cnpj.dicionario.
    Args:
        table_id (str): The ID of the table to query for unique keys.
        column (str): The name of the column to retrieve unique keys from.
    Returns:
        pd.DataFrame: A DataFrame containing the unique keys from the specified table and column.
    """
    query = f"""WITH tmp_split AS(
    SELECT
        split({column},",") AS chave 
    FROM `basedosdados.br_rf_cnpj.{table_id}`
    )
    SELECT DISTINCT chave
    FROM tmp_split,
    UNNEST(chave) AS chave"""
    uniques = bd.read_sql(query=query, from_file=True)["chave"].unique()
    df_uniques = pd.DataFrame(uniques, columns=["chave"])
    df_uniques.loc[df_uniques["chave"] == "", "chave"] = None

    return df_uniques


def format_country_name(dataframe: pd.DataFrame) -> None:
    """
    Formats the 'nome_pais' column of the DataFrame, applying various transformations to standardize country names.
    The transformations include:
    - Convert names to title case (first letter uppercase) with prepositions and conjunctions lowercase.
    - Add a space after abbreviation periods.
    - Ensure parentheses are balanced, adding a closing parenthesis if there is an opening one without a corresponding closing one.
    - Extract content within parentheses to a new "parenteses" column to append at the end of the string.
    - Split the country name into two parts, "nome_pais_1" and "nome_pais_2", based on commas or parentheses, and reorder the names.
    Example: 'WALLIS E FUTURNA, ILHAS' becomes 'Ilhas Wallis e Futurna'
             'Pacífico, Ilhas do (Administ.dos Eua)' becomes 'Ilhas do Pacífico (Administ. dos Eua)'
    Args:
        dataframe (pd.DataFrame): The DataFrame containing the 'nome_pais' column to be formatted. The formatting is done in-place, modifying the provided DataFrame directly.
    """
    df_pais = dataframe.copy()
    df_pais = df_pais.rename(columns={"valor": "nome_pais"})
    df_pais["nome_pais"] = df_pais["nome_pais"].str.title()
    df_pais["nome_pais"] = df_pais["nome_pais"].str.replace(
        r"(D)(?=[aeo]{1}s?)", "d", regex=True
    )
    df_pais["nome_pais"] = df_pais["nome_pais"].str.replace(
        r"(?<=\s)(E)(?=\s)", "e", regex=True
    )
    df_pais["nome_pais"] = df_pais["nome_pais"].str.replace(
        r"(?<=\w)(\.)(?=\w)", ". ", regex=True
    )
    df_pais.loc[
        df_pais["nome_pais"].str.contains("(", regex=False, na=False)
        & (~df_pais["nome_pais"].str.contains(")", regex=False, na=False)),
        "nome_pais",
    ] = df_pais["nome_pais"] + ")"
    df_pais["parenteses"] = df_pais["nome_pais"].str.extract(r"(\(.*\))")
    df_pais["nome_pais"] = df_pais["nome_pais"].str.replace(
        r"(\(.*\))$", "", regex=True
    )

    df_pais["parenteses"] = df_pais["parenteses"].str.replace(
        r"\(", "", regex=True
    )
    df_pais["parenteses"] = df_pais["parenteses"].str.replace(
        r"\)", "", regex=True
    )
    df_pais.loc[
        (df_pais["parenteses"].str.contains(","))
        & (df_pais["parenteses"].notna()),
        "parenteses",
    ] = (
        df_pais["parenteses"].str.split(",").str[1].str.strip()
        + " "
        + df_pais["parenteses"].str.split(",").str[0].str.strip().fillna("")
    )
    df_pais.loc[df_pais["parenteses"].notna(), "parenteses"] = (
        "(" + df_pais["parenteses"].str.strip() + ")"
    )

    df_pais["nome_pais"] = df_pais["nome_pais"].str.strip()
    df_pais["nome_pais_2"] = df_pais["nome_pais"].str.extract(
        r"([\w.\s]+)(?=\s*,|\(\s*)"
    )
    df_pais["nome_pais_1"] = df_pais["nome_pais"].str.extract(
        r"(?:\s*,|\)\s*)([\w.\s]+)"
    )
    df_pais["nome_pais_1"] = df_pais["nome_pais_1"].fillna(
        df_pais["nome_pais"]
    )
    df_pais["nome_pais_2"] = df_pais["nome_pais_2"].fillna("")
    df_pais["parenteses"] = df_pais["parenteses"].fillna("")

    df_pais["novo_nome_pais"] = (
        df_pais["nome_pais_1"]
        + " "
        + df_pais["nome_pais_2"]
        + " "
        + df_pais["parenteses"]
    )
    df_pais["novo_nome_pais"] = df_pais["novo_nome_pais"].str.strip()

    df_pais = df_pais.drop(
        columns=["nome_pais", "nome_pais_1", "nome_pais_2", "parenteses"],
    )
    df_pais = df_pais.rename(columns={"novo_nome_pais": "valor"})
    df_pais.loc[df_pais["valor"].isin(["Nan", "NaN", "None", ""]), "valor"] = (
        None
    )
    return df_pais


def find_missing_countries(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Finds and fills missing country names by querying the world directory and matching with import/export data.

    Retrieves distinct country IDs and names from the comex tables (importação and exportação),
    joins them with the world directory, and fills missing country names in the input DataFrame
    with data from the directory.

    Args:
        dataframe (pd.DataFrame): DataFrame containing 'chave' (id_pais) and 'valor' (country name) columns.

    Returns:
        pd.DataFrame: DataFrame with filled country names, deduplicated by id_pais and value.
    """
    query = """SELECT DISTINCT a.id_pais, nome_pt
        FROM(
        SELECT DISTINCT id_pais, sigla_pais_iso3
        FROM `basedosdados.br_me_comex_stat.ncm_importacao`
        UNION ALL
        SELECT DISTINCT id_pais, sigla_pais_iso3
        FROM `basedosdados.br_me_comex_stat.ncm_exportacao`
        ) a
        INNER JOIN 
        `basedosdados.br_bd_diretorios_mundo.pais` b
        ON a.sigla_pais_iso3 = b.sigla_iso3"""

    df_pais = dataframe.copy()

    df_pais_dir = bd.read_sql(query=query, from_file=True)
    df_pais_dir = df_pais_dir.astype(
        {col: "str" for col in df_pais_dir.columns}
    )

    df_pais = df_pais.rename(
        columns={"valor": "nome_pais", "chave": "id_pais"}
    )
    df_merge = df_pais.merge(
        df_pais_dir, how="left", left_on="id_pais", right_on="id_pais"
    )
    df_merge["nome_pais"] = df_merge["nome_pais"].fillna(df_merge["nome_pt"])
    df_merge = df_merge.drop(columns=["nome_pt"])
    df_merge = df_merge.rename(
        columns={"id_pais": "chave", "nome_pais": "valor"}
    )
    df_merge = df_merge.drop_duplicates(subset=df_merge.columns.to_list())

    return df_merge


def verify_duplicates(dataframe: pd.DataFrame, columns: list[str]):
    duplicated = dataframe.duplicated(subset=columns).sum()
    log(f"Duplicated values:\n{duplicated}")


def process_csv_dicionario(
    input_path: Path | str,
    output_path: Path | str,
    table_name: str,
) -> None:
    """
    Processes CSV dictionary files and transforms them into a standardized format.

    For each CSV file in the input path, reads the data in chunks, adds metadata columns
    (id_tabela, nome_coluna, cobertura_temporal) based on the table configuration,
    and appends the transformed data to a single output CSV file.

    Args:
        input_path (Path|str): Directory containing the input CSV files.
        output_path (Path|str): Base directory for output.
        table_name (str): Name of the table to look up configuration in TABLE_CONFIGS.
    """
    save_path = output_path
    save_path.mkdir(exist_ok=True, parents=True)
    save_path = save_path / "data.csv"

    log(f"Save path: {save_path}")
    table_configs = constants_cnpj.TABLE_CONFIGS.value[table_name]
    files = [
        fp
        for fp in Path(input_path).iterdir()
        if fp.is_file() and "csv" in fp.suffix.lower()
    ]
    for filepath in files:
        if table_name in filepath.as_posix().lower():
            filename = filepath.name
            log(f"Carregando o arquivo: {filename}")

            chunk = pd.read_csv(
                filepath,
                encoding="latin1",
                sep=";",
                header=None,
                names=["chave", "valor"],
                dtype=str,
            )

            chunk["chave"] = chunk["chave"].str.replace(".0", "", regex=False)
            chunk["chave"] = chunk["chave"].str.replace(
                r"(^0+)(?=[^0]+|0{1})", "", regex=True
            )
            chunk.loc[chunk["valor"] == "", "valor"] = None
            chunk.loc[chunk["chave"] == "", "chave"] = None

            for relationship in table_configs["relationships"]:
                log(
                    f"Processando relacionamento: table_id={relationship['id_tabela']}, column={relationship['nome_coluna']}"
                )
                id_tabela = relationship["id_tabela"]
                nome_coluna = relationship["nome_coluna"]

                df_unique_keys = get_table_unique_keys(
                    table_id=id_tabela, column=nome_coluna
                )

                df_unique_keys["id_tabela"] = id_tabela
                df_unique_keys["nome_coluna"] = nome_coluna
                df_unique_keys["chave"] = df_unique_keys["chave"].str.replace(
                    r"(^0+)(?=[^0]+|0{1})", "", regex=True
                )
                chunk_save = df_unique_keys.merge(
                    chunk, how="left", on="chave"
                )
                chunk_save["cobertura_temporal"] = "(1)"

                if "pais" in table_name.lower():
                    chunk_save = find_missing_countries(chunk_save)
                    chunk_save = format_country_name(chunk_save)

                chunk_save = chunk_save.dropna(
                    subset=["chave", "valor"], how="all"
                )
                chunk_save = chunk_save.drop_duplicates(
                    subset=["id_tabela", "nome_coluna", "chave", "valor"]
                )

                chunk_save[
                    [
                        "id_tabela",
                        "nome_coluna",
                        "chave",
                        "cobertura_temporal",
                        "valor",
                    ]
                ].to_csv(
                    save_path,
                    mode="a" if save_path.exists() else "w",
                    index=False,
                    encoding="utf-8",
                    header=not save_path.exists(),  # Write header only if file doesn't exist
                )

    log(f"Arquivo {table_name} salvo")
    os.remove(filepath)
    return save_path


def process_manual_dictionaries(output_path: Path | str, table_name: str):
    """
    Processes dictionary keys and values, manually defined.

    Args:
        output_path (Path|str): Base directory for output.
        table_name (str): Name of the table to look up configuration in TABLE_CONFIGS.
    """
    save_path = output_path
    save_path.mkdir(exist_ok=True, parents=True)
    save_path = save_path / "data.csv"
    table_configs = constants_cnpj.TABLE_CONFIGS.value[table_name]

    chunk = pd.DataFrame(table_configs["chaves_valores"])
    for relationship in table_configs["relationships"]:
        log(
            f"Processando relacionamento: table_id={relationship['id_tabela']}, column={relationship['nome_coluna']}"
        )
        id_tabela = relationship["id_tabela"]
        nome_coluna = relationship["nome_coluna"]

        df_unique_keys = get_table_unique_keys(
            table_id=id_tabela, column=nome_coluna
        )

        df_unique_keys["id_tabela"] = id_tabela
        df_unique_keys["nome_coluna"] = nome_coluna
        df_unique_keys["chave"] = df_unique_keys["chave"].str.replace(
            r"(^0+)(?=[^0]+|0{1})", "", regex=True
        )
        chunk_save = df_unique_keys.merge(chunk, how="left", on="chave")
        chunk_save["cobertura_temporal"] = "(1)"

        chunk_save = chunk_save.dropna(subset=["chave", "valor"], how="all")
        chunk_save = chunk_save.drop_duplicates(
            subset=["id_tabela", "nome_coluna", "chave", "valor"]
        )

        chunk_save[
            [
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ]
        ].to_csv(
            save_path,
            mode="a" if save_path.exists() else "w",
            index=False,
            encoding="utf-8",
            header=not save_path.exists(),  # Write header only if file doesn't exist
        )
