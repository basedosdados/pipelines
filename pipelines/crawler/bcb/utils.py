import re
import time
from io import StringIO
from pathlib import Path
from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from pipelines.crawler.bcb.constants import Constants
from pipelines.utils.schema_validator import validate_schema
from pipelines.utils.utils import log

STORAGE_OPTIONS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def get_sicor_download_links():
    """
    Scrapes the BCB website to retrieve SICOR download links.

    Returns:
        list: A list of URLs for the download files.
    """

    url = Constants.URL.value

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Adding a standard user-agent helps bypass basic bot checks
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    log("Setting up ChromeDriver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        log(f"Navigating to {url}...")
        driver.get(url)

        log("Waiting for JS to render the content...")
        WebDriverWait(driver, 8).until(
            ec.presence_of_element_located((By.TAG_NAME, "a"))
        )

        time.sleep(2)

        html_content = driver.page_source

        soup = BeautifulSoup(html_content, "html.parser")

        # recor is the previous system to sicor, so it is ignored. The pipeline extracts only sicor data;
        sicor_links = [
            a["href"]
            for a in soup.find_all("a", href=True)
            if (".gz" in a["href"] or ".csv" in a["href"])
            and "recor" not in a["href"]
            and "DadosBrutos" in a["href"]
        ]
        log(f"Found {len(sicor_links)} .gz links on the page.")

        return sicor_links

    except Exception as e:
        log(f"An error occurred: {e}")
        raise

    finally:
        driver.quit()


def build_sicor_download_df(links: list) -> pd.DataFrame:
    """
    Creates a DataFrame with download link information, mapping raw names to standardized table IDs.
    Also extracts the Content-Length of each file.

    Args:
        links (list): List of download links.

    Returns:
        pd.DataFrame: DataFrame with columns [id_tabela, link, tipo_liberacao_arquivo, ano, mes, content_length]
    """
    data = []
    mapping = Constants.sicor_to_bd_table_names.value

    storage_options = STORAGE_OPTIONS

    for link in links:
        filename = link.split("/")[-1]
        name_no_ext = re.sub(r"\.(gz|csv)$", "", filename, flags=re.IGNORECASE)
        clean_name = re.sub(r"\d+", "", name_no_ext).strip("_")

        id_tabela = None
        for table_id, info in mapping.items():
            raw_name = info["table_raw_name"]
            if clean_name.lower().endswith(raw_name.lower()):
                id_tabela = table_id
                break

        if id_tabela:
            year_match = re.search(r"_(\d{4})", link)
            ano = year_match.group(1) if year_match else None

            tipo_liberacao_arquivo = "yearly" if ano else "unique"

            indice_arquivo = None
            if ano:
                period_match = re.search(rf"_{ano}_(\d{{2}})", link)
                if period_match:
                    indice_arquivo = period_match.group(1)

            response = requests.head(link, headers=storage_options, timeout=10)
            response.raise_for_status()
            content_length = int(response.headers["Content-Length"])

            data.append(
                {
                    "id_tabela": id_tabela,
                    "link": link,
                    "tipo_liberacao_arquivo": tipo_liberacao_arquivo,
                    "ano": ano,
                    "mindice_arquivos": indice_arquivo,
                    "content_length": content_length,
                }
            )

    return pd.DataFrame(data)


def filter_sicor_links(
    links_df: pd.DataFrame, table_id: str, download_all_files: bool = False
) -> pd.DataFrame:
    """
    Filters the links DataFrame for a specific table, keeping only the most recent year by default.

    Args:
        links_df (pd.DataFrame): The full links DataFrame.
        table_id (str): The ID of the table.
        download_all_files (bool): If True, keeps all years. Default is False.

    Returns:
        pd.DataFrame: Filtered DataFrame for the specific table.
    """
    table_df = links_df[links_df["id_tabela"] == table_id].copy()

    # Tabelas de domínio publicadas fora de /DadosBrutos/ não aparecem no
    # scraping, então sua linha é sintetizada aqui a partir das URLs fixas em
    # `Constants.tabelas_dominio_urls`. Uma tabela pode ter mais de uma URL
    # (ver `fonte_recurso`); `get_sicor_table_size` soma os `content_length`.
    dominio_urls = Constants.tabelas_dominio_urls.value.get(table_id)
    if dominio_urls:
        return pd.DataFrame(
            [
                {
                    "id_tabela": table_id,
                    "link": link,
                    "content_length": get_content_length(link),
                }
                for link in dominio_urls
            ]
        )

    # Those tables are realeased by the origal source in early files;
    yearly_tables = [
        "operacao",
        "saldo",
        "recurso_publico_gleba",
    ]

    if not download_all_files and table_id in yearly_tables:
        max_year = table_df["ano"].astype(float).max()
        table_df = table_df[table_df["ano"].astype(float) == max_year]

    return table_df


def get_content_length(link: str) -> int:
    """Devolve o `Content-Length` de `link`, usado pelo guard de atualização.

    Args:
        link (str): URL do arquivo.

    Returns:
        int: tamanho do arquivo em bytes.
    """
    response = requests.head(link, headers=STORAGE_OPTIONS, timeout=10)
    response.raise_for_status()
    return int(response.headers["Content-Length"])


def create_folder_structure(id_tabela: str) -> Path:
    """
    Creates the folder structure for the specified table.

    Args:
        id_tabela (str): The ID of the table.

    Returns:
        Path: The created directory path.
    """
    path = Path().cwd()
    output_path = path / Constants.OUTPUT_FOLDER.value / id_tabela

    output_path.mkdir(parents=True, exist_ok=True)

    return output_path


def create_tables(
    data: pd.DataFrame,
    id_tabela: str,
    download_dir: Path,
    # pyrefly: ignore [bad-return]
) -> str:
    """
    Downloads, transforms, and saves tables in Parquet format.
    Expects data to be already filtered for the specific table.

    Args:
        data (pd.DataFrame): DataFrame containing link information for the table.
        id_tabela (str): The ID of the table to process.
        download_dir (Path): The directory where files will be saved.

    Returns:
        str: The download directory path.
    """
    config = Constants.sicor_to_bd_table_names.value.get(id_tabela)

    # pyrefly: ignore [unsupported-operation]
    renames = config["table_schema"]

    colunas_originais = list(renames.keys())

    pyarrow_fields = []

    for _col_original, col_final in renames.items():
        tipo_pa = pa.string()
        pyarrow_fields.append(pa.field(str(col_final), tipo_pa))

    explicit_schema = pa.schema(pyarrow_fields)

    storage_options = STORAGE_OPTIONS

    partitioned_tables = [
        "operacao",
        "saldo",
        "recurso_publico_gleba",
    ]

    for _, row in data.iterrows():
        link = row["link"]
        ano = row.get("ano")
        # A fonte publica tanto `.gz` (microdados) quanto `.csv` puro
        # (`SICOR_LISTA_IFS.csv`), e ambos viram parquet.
        filename = re.sub(
            r"\.(csv\.gz|gz|csv)$",
            ".parquet",
            link.split("/")[-1],
            flags=re.IGNORECASE,
        )

        if id_tabela in partitioned_tables and ano:
            # Create the ano={ano} directory
            partition_dir = download_dir / f"ano={ano}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            filepath = partition_dir / filename
        else:
            filepath = Path(download_dir) / filename

        log(f"Downloading, transforming and saving {link} to {filepath}...")

        writer = None

        chunk_iterator = pd.read_csv(
            link,
            storage_options=storage_options,
            # `infer` cobre `.gz` e `.csv` puro; para os `.gz` o efeito é
            # idêntico ao `compression="gzip"` anterior.
            compression="infer",
            encoding="latin-1",
            sep=";",
            chunksize=100000,
            dtype=str,
        )

        for chunk in chunk_iterator:
            # Validate that the source columns match the expected schema in constants
            validate_schema(chunk.columns.tolist(), colunas_originais)

            chunk = chunk.rename(columns=renames)

            table = pa.Table.from_pandas(chunk, schema=explicit_schema)

            if writer is None:
                writer = pq.ParquetWriter(filepath, explicit_schema)

            writer.write_table(table)

        if writer:
            writer.close()

        log(f"Parquet saved successfully to: {filename}")


# pyrefly: ignore [bad-return]
def create_empreendimento(id_tabela: str, download_dir: Path) -> str:
    """
    Downloads, transforms, and saves the empreendimento table in CSV format.

    Args:
        id_tabela (str): The ID of the table.
        download_dir (Path): The directory where the file will be saved.

    Returns:
        str: The download directory path.
    """
    link = Constants.tabelas_dominio_urls.value["empreendimento"][0]
    config = Constants.sicor_to_bd_table_names.value.get(id_tabela)

    # pyrefly: ignore [unsupported-operation]
    renames = config["table_schema"]

    colunas_originais = list(renames.keys())

    storage_options = STORAGE_OPTIONS

    filename = link.split("/")[-1]

    filepath = Path(download_dir) / filename

    log(f"Downloading, transforming and saving {link} to {filepath}...")

    df = pd.read_csv(
        link,
        storage_options=storage_options,
        encoding="latin-1",
        sep=";",
        dtype=str,
    )

    validate_schema(df.columns.tolist(), colunas_originais)

    df = df.rename(columns=renames)

    df.to_csv(filepath, index=False, encoding="utf-8", sep=",")

    log(f"CSV saved successfully to: {filename}")


def create_fonte_recurso(id_tabela: str, download_dir: Path) -> None:
    """Monta a tabela de fontes de recurso a partir de duas tabelas de domínio.

    `FonteRecursos.csv` traz os 37 códigos com descrição e vigência.
    `FonteRecursosPublicos.csv` traz um subconjunto estrito de 16 desses
    códigos, com descrições idênticas — seu único conteúdo novo é a própria
    participação na lista, que define quais fontes são públicas/controladas e,
    por consequência, quais operações aparecem nas tabelas `recurso_publico_*`.
    Por isso o segundo arquivo entra como o indicador
    `indicador_recurso_publico` em vez de virar linhas do dicionário, onde
    colidiria com as chaves de `id_fonte_recurso` já vindas do primeiro.

    Os dois arquivos divergem em separador e codificação: o de todas as fontes
    é latin-1 com `;`, o de fontes públicas é UTF-8 com `,`.

    Args:
        id_tabela (str): ID da tabela (`fonte_recurso`).
        download_dir (Path): diretório onde o CSV será salvo.
    """
    # O valor do Enum não é tipado, então o cast é o que informa ao verificador
    # de tipos o que `sicor_to_bd_table_names` realmente guarda.
    config = cast(
        dict[str, dict[str, str]],
        Constants.sicor_to_bd_table_names.value[id_tabela],
    )
    renames = config["table_schema"]
    renames_publicos = config["table_schema_recurso_publico"]

    url_todas, url_publicas = Constants.tabelas_dominio_urls.value[id_tabela]

    log(f"Downloading and merging {url_todas} and {url_publicas}...")

    todas = pd.read_csv(
        url_todas,
        storage_options=STORAGE_OPTIONS,
        encoding="latin-1",
        sep=";",
        dtype=str,
    )
    validate_schema(todas.columns.tolist(), list(renames.keys()))
    todas = todas.rename(columns=renames)

    publicas = pd.read_csv(
        url_publicas,
        storage_options=STORAGE_OPTIONS,
        encoding="utf-8",
        sep=",",
        dtype=str,
    )
    validate_schema(publicas.columns.tolist(), list(renames_publicos.keys()))
    publicas = publicas.rename(columns=renames_publicos)

    codigos_publicos = set(publicas["id_fonte_recurso"].str.strip())
    faltantes = codigos_publicos - set(todas["id_fonte_recurso"].str.strip())
    if faltantes:
        raise ValueError(
            "Códigos de FonteRecursosPublicos.csv ausentes de "
            f"FonteRecursos.csv: {sorted(faltantes)}. O arquivo de fontes "
            "públicas deveria ser um subconjunto do de todas as fontes."
        )

    for coluna in renames.values():
        todas[coluna] = todas[coluna].str.strip()

    todas["indicador_recurso_publico"] = (
        todas["id_fonte_recurso"]
        .isin(codigos_publicos)
        .map({True: "1", False: "0"})
    )

    filepath = Path(download_dir) / "fonte_recurso.csv"
    todas.to_csv(filepath, index=False, encoding="utf-8", sep=",")

    log(
        f"CSV saved successfully to: {filepath.name} "
        f"({len(todas)} fontes, {len(codigos_publicos)} públicas)"
    )


def parse_cobertura(row):
    """
    Parses the temporal coverage from a dictionary row. Sicor tables have several deprecated dictionavary values.
    This function parser sicor deprecated keys pattern to basedosdados pattern.

    Args:
        row (pd.Series): A row from the dictionary DataFrame.

    Returns:
        str: The formatted temporal coverage string.
    """
    try:
        # Date format in tables is dd/mm/yyyy
        start_year = (
            str(row["DATA_INICIO"]).split("/")[-1]
            if pd.notna(row["DATA_INICIO"])
            else ""
        )
        end_year = (
            str(row["DATA_FIM"]).split("/")[-1]
            if pd.notna(row["DATA_FIM"])
            else ""
        )

        if start_year and end_year:
            return f"{start_year}(1){end_year}"
        elif start_year:
            return f"{start_year}(1)"
        elif end_year:
            return f"(1){end_year}"
        else:
            return "(1)"
    except Exception as e:
        log(f"Error parsing date in parse_cobertura: {e}")
        raise ValueError(e) from e


def create_dictionary() -> str:
    """
    Creates the dictionary table using the metadata defined in Constants.dicionario.

    Returns:
        str: The generated dictionary directory path.
    """
    all_data = []
    dicionario_config = Constants.dicionario.value

    headers = STORAGE_OPTIONS

    for entry in dicionario_config:
        id_tabela = entry["id_tabela"]
        nome_coluna = entry["nome_coluna"]
        url = entry["url"]
        colunas_map = entry["colunas"]
        sep = entry.get("sep")

        log(f"Processing dictionary for {id_tabela}.{nome_coluna} from {url}")

        try:
            # SICOR CSVs use latin-1 encoding and ; or , as sep
            # Some CSVs with sep="," have the entire line in quotes, which breaks pandas parsing.
            # The solution found was to download with requests and fix the problematic lines before passing to pandas. More verbose but it works :)
            # pyrefly: ignore [bad-argument-type]
            response = requests.get(url, headers=headers)
            content = response.content.decode("latin-1")

            lines = content.splitlines()
            fixed_lines = []
            for line in lines:
                # Marker for broken BCB lines: "1,""TR"""
                if (
                    line.startswith('"')
                    and line.endswith('"')
                    and (sep + '""') in line  # pyrefly: ignore [unsupported-operation]
                ):
                    line = line[1:-1].replace('""', '"')
                fixed_lines.append(line)

            fixed_content = "\n".join(fixed_lines)

            # pyrefly: ignore [no-matching-overload]
            df = pd.read_csv(
                StringIO(fixed_content),
                sep=sep,
                dtype=str,
            )
            chave_src_col = next(
                k
                # pyrefly: ignore [missing-attribute]
                for k, v in colunas_map.items()
                if v == "chave"
            )
            valor_src_col = next(
                k
                # pyrefly: ignore [missing-attribute]
                for k, v in colunas_map.items()
                if v == "valor"
            )

            temp_df = pd.DataFrame()
            temp_df["chave"] = df[chave_src_col].str.strip()
            temp_df["valor"] = df[valor_src_col].str.strip()
            temp_df["id_tabela"] = id_tabela
            temp_df["nome_coluna"] = nome_coluna

            # These dictionaries have columns for the start and end of code validity;
            # it implies that some cols have values for cobertural_temporal different from (1)
            # A Program, like PRONAF, has a start date and a possible end date.
            if nome_coluna in [
                "id_fonte_recurso",
                "id_categoria_emitente",
                "id_programa",
            ]:
                temp_df["cobertura_temporal"] = df.apply(
                    parse_cobertura, axis=1
                )
            else:
                temp_df["cobertura_temporal"] = "(1)"

            all_data.append(temp_df)

        except Exception as e:
            log(f"Error processing dictionary entry for {nome_coluna}: {e}")
            raise

    final_df = pd.concat(all_data, ignore_index=True)

    output_dir = create_folder_structure("dicionario")
    output_path = output_dir / "dicionario.csv"
    final_df.to_csv(output_path, index=False, encoding="utf-8", sep=",")

    log(f"Dictionary CSV saved to {output_path}")

    return str(output_dir.absolute())
